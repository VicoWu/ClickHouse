#include <Processors/Port.h>
#include <Processors/IProcessor.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
/**
 * 连接两个port
 * @param output
 * @param input
 * @param reconnect
 */
void connect(OutputPort & output, InputPort & input, bool reconnect)
{
    if (!reconnect && input.state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is already connected, (header: [{}])", input.header.dumpStructure());

    if (!reconnect && output.state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Port is already connected, (header: [{}])", output.header.dumpStructure());
    auto out_name = output.processor ? output.getProcessor().getName() : "null";
    auto in_name = input.processor ? input.getProcessor().getName() : "null";

    assertCompatibleHeader(output.getHeader(), input.getHeader(), fmt::format("function connect between {} and {}", out_name, in_name));

    input.output_port = &output;// 将这个input的output_port指向output
    output.input_port = &input; // 将这个output的input_port指向input
    input.state = std::make_shared<Port::State>(); // 为两个Port之间的连接构建共享信息
    output.state = input.state;
}

}
