#include <Backups/BackupIO_Default.h>

#include <Disks/IDisk.h>
#include <IO/copyData.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>


namespace DB
{

BackupReaderDefault::BackupReaderDefault(Poco::Logger * log_, const ContextPtr & context_)
    : log(log_)
    , read_settings(context_->getBackupReadSettings())
    , write_settings(context_->getWriteSettings())
    , write_buffer_size(DBMS_DEFAULT_BUFFER_SIZE)
{
}

void BackupReaderDefault::copyFileToDisk(const String & path_in_backup, size_t file_size, bool encrypted_in_backup,
                                         DiskPtr destination_disk, const String & destination_path, WriteMode write_mode)
{
    LOG_TRACE(log, "Copying file {} to disk {} through buffers", path_in_backup, destination_disk->getName());
    auto read_buffer = readFile(path_in_backup);

    std::unique_ptr<WriteBuffer> write_buffer;
    auto buf_size = std::min(file_size, write_buffer_size);
    if (encrypted_in_backup)
        write_buffer = destination_disk->writeEncryptedFile(destination_path, buf_size, write_mode, write_settings);
    else
        write_buffer = destination_disk->writeFile(destination_path, buf_size, write_mode, write_settings);

    copyData(*read_buffer, *write_buffer, file_size);
    write_buffer->finalize();
}

BackupWriterDefault::BackupWriterDefault(Poco::Logger * log_, const ContextPtr & context_)
    : log(log_)
    , read_settings(context_->getBackupReadSettings())
    , write_settings(context_->getWriteSettings())
    , write_buffer_size(DBMS_DEFAULT_BUFFER_SIZE)
{
}

bool BackupWriterDefault::fileContentsEqual(const String & file_name, const String & expected_file_contents)
{
    if (!fileExists(file_name))
        return false;

    try
    {
        auto in = readFile(file_name, expected_file_contents.size());
        String actual_file_contents(expected_file_contents.size(), ' ');
        return (in->read(actual_file_contents.data(), actual_file_contents.size()) == actual_file_contents.size())
            && (actual_file_contents == expected_file_contents) && in->eof();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
}

/**

BackupImpl::writeFile
->
BackupWriterS3::copyFileFromDisk | BackupWriterDisk::copyFileFromDisk
->
// BackupWriterS3 和 BackupWriterDisk 都重写了copyFileFromDisk
void BackupWriterS3::copyFileFromDisk || void BackupWriterDisk::copyFileFromDisk
->
BackupWriterS3::copyDataToFile(path_in_backup, create_read_buffer, start_pos, length); (只有BackupWriterS3重写了该方法，而BackupWriterDisk没有重写该方法)
或者
BackupWriterDefault::copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
->
BackupWriterDisk::readFile | BackupWriterS3::readFile
->
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase
这个是从磁盘备份到磁盘的调用逻辑
调用者是 void BackupWriterDisk::copyFileFromDisk

对应 BackupWriterS3::copyDataToFile的调用者是void BackupWriterDisk::copyFileFromDisk
*/
void BackupWriterDefault::copyDataToFile(const String & path_in_backup, const CreateReadBufferFunction & create_read_buffer, UInt64 start_pos, UInt64 length)
{
    // 这个回调定义在 void BackupWriterDefault::copyFileFromDisk， 从local disk进行throttle也是设置在这里
    auto read_buffer = create_read_buffer();

    if (start_pos)
        read_buffer->seek(start_pos, SEEK_SET);

    auto write_buffer = writeFile(path_in_backup);
    // void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes)
    copyData(*read_buffer, *write_buffer, length);
    write_buffer->finalize();
}

/**

BackupImpl::writeFile
->
BackupWriterS3::copyFileFromDisk | BackupWriterDisk::copyFileFromDisk
->
// BackupWriterS3 和 BackupWriterDisk 都重写了copyFileFromDisk
void BackupWriterS3::copyFileFromDisk || void BackupWriterDisk::copyFileFromDisk
->
BackupWriterS3::copyDataToFile(path_in_backup, create_read_buffer, start_pos, length); (只有BackupWriterS3重写了该方法，而BackupWriterDisk没有重写该方法)
或者
BackupWriterDefault::copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
->
BackupWriterDisk::readFile | BackupWriterS3::readFile
->
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase


*/
void BackupWriterDefault::copyFileFromDisk(const String & path_in_backup, DiskPtr src_disk, const String & src_path,
                                           bool copy_encrypted, UInt64 start_pos, UInt64 length)
{
    // 在这个位置，备份到s3和备份到disk到书来给你是一样的，为什么readFile中LocalReadThrottlerBytes会翻倍呢？
    LOG_TRACE(log, "Copying file {} from disk {} through buffers", src_path, src_disk->getName());
    auto create_read_buffer = [src_disk, src_path, copy_encrypted, settings = read_settings.adjustBufferSize(start_pos + length)]
    {
        if (copy_encrypted) {
            LOG_INFO(log, "mydebug this is an ecrypted file");
            return src_disk->readEncryptedFile(src_path, settings);
        }

        else
            // 这里只是定义了一个callback
            {
            LOG_INFO(log, "mydebug this is not an ecrypted file");
            return src_disk->readFile(src_path, settings);
            }
    };

    // void BackupWriterS3::copyDataToFile 或 void BackupWriterDefault::copyDataToFile
    copyDataToFile(path_in_backup, create_read_buffer, start_pos, length);
}
}
