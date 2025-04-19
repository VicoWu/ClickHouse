#include <Processors/Executors/ExecutingGraph.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>

#include <shared_mutex>
#include <stack>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ExecutingGraph::ExecutingGraph(std::shared_ptr<Processors> processors_, bool profile_processors_)
    : processors(std::move(processors_))
    , profile_processors(profile_processors_)
{
    uint64_t num_processors = processors->size();
    nodes.reserve(num_processors);
    source_processors.reserve(num_processors);

    /// Create nodes.
    for (uint64_t node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors->at(node).get();
        processors_map[proc] = node;
        nodes.emplace_back(std::make_unique<Node>(proc, node));

        bool is_source = proc->getInputs().empty();
        source_processors.emplace_back(is_source);
    }

    /// Create edges.
    for (uint64_t node = 0; node < num_processors; ++node)
        addEdges(node);
}

ExecutingGraph::Edge & ExecutingGraph::addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to)
{
    auto it = processors_map.find(to);
    if (it == processors_map.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Processor {} was found as {} for processor {}, but not found in list of processors",
            to->getName(),
            edge.backward ? "input" : "output",
            from->getName());

    edge.to = it->second;
    auto & added_edge = edges.emplace_back(std::move(edge));
    added_edge.update_info.id = &added_edge;
    return added_edge;
}

bool ExecutingGraph::addEdges(uint64_t node)
{
    IProcessor * from = nodes[node]->processor;

    bool was_edge_added = false;

    /// Add backward edges from input ports.
    auto & inputs = from->getInputs();
    auto from_input = nodes[node]->back_edges.size();

    if (from_input < inputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it, ++from_input)
        {
            const IProcessor * to = &it->getOutputPort().getProcessor();
            auto output_port_number = to->getOutputPortNumber(&it->getOutputPort());
            Edge edge(0, true, from_input, output_port_number, &nodes[node]->post_updated_input_ports);
            auto & added_edge = addEdge(nodes[node]->back_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    /// Add direct edges from output ports.
    auto & outputs = from->getOutputs();
    auto from_output = nodes[node]->direct_edges.size();

    if (from_output < outputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it, ++from_output)
        {
            const IProcessor * to = &it->getInputPort().getProcessor();
            auto input_port_number = to->getInputPortNumber(&it->getInputPort());
            Edge edge(0, false, input_port_number, from_output, &nodes[node]->post_updated_output_ports);
            auto & added_edge = addEdge(nodes[node]->direct_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    return was_edge_added;
}

ExecutingGraph::UpdateNodeStatus ExecutingGraph::expandPipeline(std::stack<uint64_t> & stack, uint64_t pid)
{
    auto & cur_node = *nodes[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return UpdateNodeStatus::Exception;
    }

    {
        std::lock_guard guard(processors_mutex);
        /// Do not add new processors to existing list, since the query was already cancelled.
        if (cancelled)
        {
            for (auto & processor : new_processors)
                processor->cancel();
            return UpdateNodeStatus::Cancelled;
        }
        processors->insert(processors->end(), new_processors.begin(), new_processors.end());

        // Do not consider sources added during pipeline expansion as cancelable to avoid tricky corner cases (e.g. ConvertingAggregatedToChunksWithMergingSource cancellation)
        source_processors.resize(source_processors.size() + new_processors.size(), false);
    }

    uint64_t num_processors = processors->size();
    std::vector<uint64_t> back_edges_sizes(num_processors, 0);
    std::vector<uint64_t> direct_edge_sizes(num_processors, 0);

    for (uint64_t node = 0; node < nodes.size(); ++node)
    {
        direct_edge_sizes[node] = nodes[node]->direct_edges.size();
        back_edges_sizes[node] = nodes[node]->back_edges.size();
    }

    nodes.reserve(num_processors);

    while (nodes.size() < num_processors)
    {
        auto * processor = processors->at(nodes.size()).get();
        if (processors_map.contains(processor))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} was already added to pipeline", processor->getName());

        processors_map[processor] = nodes.size();
        nodes.emplace_back(std::make_unique<Node>(processor, nodes.size()));
    }

    std::vector<uint64_t> updated_nodes;

    for (uint64_t node = 0; node < num_processors; ++node)
    {
        if (addEdges(node))
            updated_nodes.push_back(node);
    }

    for (auto updated_node : updated_nodes)
    {
        auto & node = *nodes[updated_node];

        size_t num_direct_edges = node.direct_edges.size();
        size_t num_back_edges = node.back_edges.size();

        std::lock_guard guard(node.status_mutex);

        for (uint64_t edge = back_edges_sizes[updated_node]; edge < num_back_edges; ++edge)
            node.updated_input_ports.emplace_back(edge);

        for (uint64_t edge = direct_edge_sizes[updated_node]; edge < num_direct_edges; ++edge)
            node.updated_output_ports.emplace_back(edge);

        if (node.status == ExecutingGraph::ExecStatus::Idle)
        {
            node.status = ExecutingGraph::ExecStatus::Preparing;
            stack.push(updated_node);
        }
    }

    return UpdateNodeStatus::Done;
}

void ExecutingGraph::initializeExecution(Queue & queue, Queue & async_queue)
{
    std::stack<uint64_t> stack;

    /// Add childless processors to stack.
    uint64_t num_processors = nodes.size();
    for (uint64_t proc = 0; proc < num_processors; ++proc)
    {
        if (nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }

    while (!stack.empty())
    {
        uint64_t proc = stack.top();
        stack.pop();

        updateNode(proc, queue, async_queue);
    }
}

/**
 * 调用者是 PipelineExecutor::executeStepImpl
 * 据某个 Processor 的执行结果更新执行图状态，并递推推进下游 Processor 状态
* @param pid
 * @param queue
 * @param async_queue
 * @return
 */
ExecutingGraph::UpdateNodeStatus ExecutingGraph::updateNode(uint64_t pid, Queue & queue, Queue & async_queue)
{
    std::stack<Edge *> updated_edges;
    std::stack<uint64_t> updated_processors;
    updated_processors.push(pid); // 向updated_processors中添加第一个元素

    std::shared_lock read_lock(nodes_mutex);

    while (!updated_processors.empty() || !updated_edges.empty())
    {
        std::optional<std::unique_lock<std::mutex>> stack_top_lock;

        if (updated_processors.empty())
        {
            auto * edge = updated_edges.top(); // 从栈中取出栈顶元素
            updated_edges.pop();

            /// Here we have ownership on edge, but node can be concurrently accessed.

            auto & node = *nodes[edge->to];

            std::unique_lock lock(node.status_mutex);
            // 获取当前的执行状态。注意是执行状态ExecStatus，而不是IProcessor::Status
            ExecutingGraph::ExecStatus status = node.status;

            if (status != ExecutingGraph::ExecStatus::Finished)
            {
                if (edge->backward)
                    node.updated_output_ports.push_back(edge->output_port_number);
                else
                    node.updated_input_ports.push_back(edge->input_port_number);

                if (status == ExecutingGraph::ExecStatus::Idle)
                {
                    node.status = ExecutingGraph::ExecStatus::Preparing;
                    updated_processors.push(edge->to);
                    stack_top_lock = std::move(lock);
                }
                else
                    nodes[edge->to]->processor->onUpdatePorts();
            }
        }

        if (!updated_processors.empty())
        {
            pid = updated_processors.top();
            updated_processors.pop();

            /// In this method we have ownership on node.
            auto & node = *nodes[pid]; // 取出对应的ExecutingGraph::Node节点

            bool need_expand_pipeline = false;

            if (!stack_top_lock)
                stack_top_lock.emplace(node.status_mutex);

            {
#ifndef NDEBUG
                Stopwatch watch;
#endif

                std::unique_lock<std::mutex> lock(std::move(*stack_top_lock));

                try
                {
                    auto & processor = *node.processor;
                    // 暂存一下执行prepare以前的状态
                    const auto last_status = node.last_processor_status;
                    // 调用当前processor.prepare方法，收集返回状态
                    /**
                     * 执行图 ExecutingGraph 中每个 node 是一个 processor 节点，processor 节点之间的连接（也就是端口的连接）是通过 Edge 表示的。
                     * 当一个 processor 的 output port 写入了数据，会导致相连的 input port 状态变化（例如 hasData() = true）,
                     * 被连接的 processor 需要重新进入调度逻辑，进行 prepare() , 于是，ClickHouse 用 Edge 来追踪这种数据流动引起的“更新”。
                     * 对于KafkaSource，这里调用的是 ISource::prepare()。搜索 virtual Status prepare();查看该方法的功能
                     *
                     */
                    IProcessor::Status status = processor.prepare(node.updated_input_ports, node.updated_output_ports);
                    node.last_processor_status = status;
                    if (status == IProcessor::Status::Finished && CurrentThread::getGroup())
                        CurrentThread::getGroup()->memory_spill_scheduler.remove(&processor);

                    if (profile_processors) // profile_processors是构造ExecutingGraph的时候构造的，记录每个 Processor 的执行性能数据
                    {
                        /// NeedData 如果执行prepare()以前不是NeedData，现在的状态是NeedData
                        if (last_status != IProcessor::Status::NeedData && status == IProcessor::Status::NeedData)
                        {
                            // 重置一下等待Input的时间计数器
                            processor.input_wait_watch.restart();
                        }
                        // 如果执行prepare()以前是NeedData，现在的状态不是NeedData
                        else if (last_status == IProcessor::Status::NeedData && status != IProcessor::Status::NeedData)
                        {
                            // 时间统计，记录一下等待input的时间
                            processor.input_wait_elapsed_ns += processor.input_wait_watch.elapsedNanoseconds();
                        }

                        /// PortFull 如果执行prepare()以前不是PortFull，现在是PortFull
                        if (last_status != IProcessor::Status::PortFull && status == IProcessor::Status::PortFull)
                        {
                            // 重新计数一下 等待output_wait的时间计数器
                            processor.output_wait_watch.restart();
                        }
                        // 如果执行prepare()以前不是PortFull，现在的状态是PortFull
                        else if (last_status == IProcessor::Status::PortFull && status != IProcessor::Status::PortFull)
                        {
                            processor.output_wait_elapsed_ns += processor.output_wait_watch.elapsedNanoseconds();
                        }
                    }
                }
                catch (...)
                {
                    node.exception = std::current_exception();
                    return UpdateNodeStatus::Exception;
                }

#ifndef NDEBUG
                node.preparation_time_ns += watch.elapsed();
#endif

                node.updated_input_ports.clear();
                node.updated_output_ports.clear();

                switch (*node.last_processor_status)
                {
                    case IProcessor::Status::NeedData:
                    case IProcessor::Status::PortFull:
                    {
                        // prepare以后的状态是NeedData或者PortFull，那么执行状态更新为Idle
                        node.status = ExecutingGraph::ExecStatus::Idle;
                        break;
                    }
                    case IProcessor::Status::Finished:
                    {
                        // prepare以后的状态是Finished，那么执行状态更新为Finished
                        node.status = ExecutingGraph::ExecStatus::Finished;
                        break;
                    }
                    case IProcessor::Status::Ready:
                    {
                        // 执行状态更新为执行，并且把这个ExecutingGraph::Node添加到同步队列中
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::Async:
                    {
                        // 异步状态，添加到async_queue中去，更新当前的执行状态为Executing
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        async_queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::ExpandPipeline:
                    {
                        // 需要扩展Pipeline
                        need_expand_pipeline = true;
                        break;
                    }
                }

                if (!need_expand_pipeline)
                {  // 不需要扩展Pipeline
                    /// If you wonder why edges are pushed in reverse order,
                    /// it is because updated_edges is a stack, and we prefer to get from stack
                    /// input ports firstly, and then outputs, both in-order.
                    ///
                    /// Actually, there should be no difference in which order we process edges.
                    /// However, some tests are sensitive to it (e.g. something like SELECT 1 UNION ALL 2).
                    /// Let's not break this behaviour so far.
                    /**
                     * post_updated_output_ports 是当前 processor 在 prepare() 后发现有变化的 output port 对应的边（通常是下游 processor）
                     * post_updated_input_ports 是有变化的 input port 对应的边（通常是上游 processor）
                     * 这两组边被 push 到 updated_edges 栈中，下一轮 updateNode() 会处理它们指向的 processor。
                     *
                     * 因为我们希望 先处理 Input port 所连接的 Processor（也就是当前 processor 的上游），再处理下游。
                     *
                     * - 先去处理“谁给我提供数据了”（= 上游）
                     * - 然后才处理“我可以把数据往哪儿送”（= 下游）
                     */
                    for (auto it = node.post_updated_output_ports.rbegin(); it != node.post_updated_output_ports.rend(); ++it)
                    {
                        // 把 Output 边逆序 push 进去
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge); // 先把Output边入栈，因此在pop的时候会后处理
                        edge->update_info.trigger();
                    }

                    for (auto it = node.post_updated_input_ports.rbegin(); it != node.post_updated_input_ports.rend(); ++it)
                    {
                        // 把 Input 边逆序 push 进去
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge); // 然后再把input边入栈，因此在pop的时候会先处理
                        edge->update_info.trigger();
                    }
                    // 已经添加到updated_edges中，下一轮updateNode()就会处理了，
                    // 因此可以把当前node的post_updated_input_ports和post_updated_output_ports清空了
                    node.post_updated_input_ports.clear();
                    node.post_updated_output_ports.clear();
                }
            }

            if (need_expand_pipeline)
            {  // 需要扩展pipeline
                // We do not need to upgrade lock atomically, so we can safely release shared_lock and acquire unique_lock
                read_lock.unlock();
                {
                    std::unique_lock lock(nodes_mutex); // ExecutingGraph上全局锁，因为此时要更新整个Graph
                    auto status = expandPipeline(updated_processors, pid); // 扩展
                    if (status != UpdateNodeStatus::Done)
                        return status;
                }
                read_lock.lock();

                /// Add itself back to be prepared again.
                updated_processors.push(pid); // 如果需要扩展pipeline，那么就往updated_processors添加节点
            }
        }
    }

    return UpdateNodeStatus::Done;
}

void ExecutingGraph::cancel(bool cancel_all_processors)
{
    std::exception_ptr exception_ptr;

    {
        std::lock_guard guard(processors_mutex);
        uint64_t num_processors = processors->size();
        for (uint64_t proc = 0; proc < num_processors; ++proc)
        {
            try
            {
                /// Stop all processors in the general case, but in a specific case
                /// where the pipeline needs to return a result on a partially read table,
                /// stop only the processors that read from the source
                if (cancel_all_processors || source_processors.at(proc))
                {
                    IProcessor * processor = processors->at(proc).get();
                    processor->cancel();
                }
            }
            catch (...)
            {
                if (!exception_ptr)
                    exception_ptr = std::current_exception();

                /// Log any exception since:
                /// a) they are pretty rare (the only that I know is from
                ///    RemoteQueryExecutor)
                /// b) there can be exception during query execution, and in this
                ///    case, this exception can be ignored (not showed to the user).
                tryLogCurrentException("ExecutingGraph");
            }
        }
        if (cancel_all_processors)
            cancelled = true;
    }

    if (exception_ptr)
        std::rethrow_exception(exception_ptr);
}

}
