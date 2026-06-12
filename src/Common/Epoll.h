#pragma once
#if defined(OS_LINUX)

#include <sys/epoll.h>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>

namespace DB
{

/** Linux epoll 的薄封装：epoll_create → epoll_fd，epoll_ctl(ADD/DEL) → add/remove，epoll_wait → getManyReady。
  * HedgedConnections / HedgedConnectionsFactory 等用同一套接口同时监视 socket、timerfd、内层 epoll 控制 fd。
  */
class Epoll
{
public:
    Epoll();

    Epoll(const Epoll &) = delete;
    Epoll & operator=(const Epoll &) = delete;

    Epoll & operator=(Epoll && other) noexcept;
    Epoll(Epoll && other) noexcept;

    /// 把 fd 登记到本 epoll 实例。ptr 为 nullptr 时 epoll_event.data.fd = fd，否则 data.ptr = ptr。
    /// 默认 events = EPOLLIN | EPOLLERR：关心「可读」与错误（timerfd 到期、socket 有数据都属于可读）。
    void add(int fd, void * ptr = nullptr, uint32_t events = EPOLLIN | EPOLLERR);
    void add(int fd, uint32_t events) { add(fd, nullptr, events); }

    /// 从 epoll 摘掉 fd（对应 epoll_ctl EPOLL_CTL_DEL）。
    void remove(int fd);

    /// 等待就绪事件，写入 events_out，返回就绪个数。
    /// timeout 单位毫秒：-1 一直阻塞到有事发生；0 立即返回（无事件则返回 0）；>0 最多等这么多毫秒。
    size_t getManyReady(int max_events, epoll_event * events_out, int timeout) const;

    /// 本 epoll 实例自己的 fd（epoll_create 的返回值）；外层有时会对它 epoll_wait / async_callback。
    int getFileDescriptor() const { return epoll_fd; }

    /// 当前登记的 fd 数量（add 递增，remove 递减）。
    int size() const { return events_count; }

    bool empty() const { return events_count == 0; }

    /// 供异步调度/logging 用的描述字符串。
    const std::string & getDescription() const { return fd_description; }

    ~Epoll();

private:
    int epoll_fd; ///< epoll 实例 fd
    std::atomic<int> events_count;
    const std::string fd_description = "epoll";
};

}
#endif
