#include <IO/WriteHelpers.h>
#include <Processors/Port.h>
#include <QueryPipeline/Chain.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void checkSingleInput(const IProcessor & transform)
{
    if (transform.getInputs().size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Transform for chain should have single input, but {} has {} inputs",
            transform.getName(),
            transform.getInputs().size());

    if (transform.getInputs().front().isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transform for chain has connected input");
}

static void checkSingleOutput(const IProcessor & transform)
{
    if (transform.getOutputs().size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Transform for chain should have single output, but {} has {} outputs",
            transform.getName(),
            transform.getOutputs().size());

    if (transform.getOutputs().front().isConnected())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Transform for chain has connected output");
}

static void checkTransform(const IProcessor & transform)
{
    checkSingleInput(transform);
    checkSingleOutput(transform);
}

static void checkInitialized(const std::list<ProcessorPtr> & processors)
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chain is not initialized");
}

Chain::Chain(ProcessorPtr processor)
{
    checkTransform(*processor);
    processors.emplace_back(std::move(processor));
}

Chain::Chain(std::list<ProcessorPtr> processors_) : processors(std::move(processors_))
{
    if (processors.empty())
        return;

    checkSingleInput(*processors.front());
    checkSingleOutput(*processors.back());

    for (const auto & processor : processors)
    {
        for (const auto & input : processor->getInputs())
            if (&input != &getInputPort() && !input.isConnected())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot initialize chain because there is a disconnected input for {}",
                    processor->getName());

        for (const auto & output : processor->getOutputs())
            if (&output != &getOutputPort() && !output.isConnected())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot initialize chain because there is a disconnected output for {}",
                    processor->getName());
    }
}

/**
 * 将这个Processor添加到这个Chain的头部，作为这个Chain的第一个Processor
 * @param processor
 */
void Chain::addSource(ProcessorPtr processor)
{
    checkTransform(*processor);

    if (!processors.empty())
        connect(processor->getOutputs().front(), getInputPort());

    processors.emplace_front(std::move(processor));
}

/**
 * 将这个Processor添加到这个Chain的尾部，作为这个Chain的最后一个Processor
 * @param processor
 */
void Chain::addSink(ProcessorPtr processor)
{
    checkTransform(*processor);

    if (!processors.empty()) // 将当前chain的output和新的processor的input连接起来
        connect(getOutputPort(), processor->getInputs().front());

    processors.emplace_back(std::move(processor));
}

/**
 *  Chain 类的一个成员函数，用于将一个链（Chain）追加到当前链的尾部，形成一个更长的处理链。
 *  这个在构建物化视图处理流程或 insert 查询处理时经常用到。
 * @param chain
 */
void Chain::appendChain(Chain chain)
{
    // 将当前chain的OutputPort连接到新的chain的InputPort
    connect(getOutputPort(), chain.getInputPort());
    // 把 chain 中的所有处理器 processors 移动到当前链的 processors 列表尾部。
    // std::move 表示把资源“拿走”，chain 自己的 processors 就被清空了。
    processors.splice(processors.end(), std::move(chain.processors));
    // 从 chain 中分离出它的资源管理器（比如表的引用、context 持有等），并添加到当前链中。
    attachResources(chain.detachResources());
    // 把 chain 的线程数加进当前链的线程数。每个 Chain 可以使用一定数量的线程来执行任务；拼接后线程数是总和。
    num_threads += chain.num_threads;
}

IProcessor & Chain::getSource()
{
    checkInitialized(processors);
    return *processors.front();
}

IProcessor & Chain::getSink()
{
    checkInitialized(processors);
    return *processors.back();
}

InputPort & Chain::getInputPort() const
{
    checkInitialized(processors);
    return processors.front()->getInputs().front();
}

OutputPort & Chain::getOutputPort() const
{
    checkInitialized(processors);
    return processors.back()->getOutputs().front();
}

const Block & Chain::getInputHeader() const
{
    return getInputPort().getHeader();
}

const Block & Chain::getOutputHeader() const
{
    return getOutputPort().getHeader();
}

void Chain::reset()
{
    Chain to_remove = std::move(*this);
    *this = Chain();
}

}
