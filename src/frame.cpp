#include <sessiongw/frame.hpp>

#include <sessiongw/errors.hpp>

#include <algorithm>
#include <limits>

namespace sessiongw
{
namespace
{

void writeU16(std::vector<std::uint8_t>& out, const std::size_t offset, const std::uint16_t value)
{
    out[offset] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    out[offset + 1] = static_cast<std::uint8_t>(value & 0xffU);
}

void writeU32(std::vector<std::uint8_t>& out, const std::size_t offset, const std::uint32_t value)
{
    out[offset] = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
    out[offset + 1] = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
    out[offset + 2] = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    out[offset + 3] = static_cast<std::uint8_t>(value & 0xffU);
}

void writeU64(std::vector<std::uint8_t>& out, const std::size_t offset, const std::uint64_t value)
{
    for (std::size_t index = 0; index < 8; ++index)
    {
        const unsigned shift = static_cast<unsigned>(56U - (index * 8U));
        out[offset + index] = static_cast<std::uint8_t>((value >> shift) & 0xffU);
    }
}

std::uint16_t readU16(const std::span<const std::uint8_t> bytes, const std::size_t offset)
{
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8U) |
                                      static_cast<std::uint16_t>(bytes[offset + 1]));
}

std::uint32_t readU32(const std::span<const std::uint8_t> bytes, const std::size_t offset)
{
    return (static_cast<std::uint32_t>(bytes[offset]) << 24U) |
           (static_cast<std::uint32_t>(bytes[offset + 1]) << 16U) |
           (static_cast<std::uint32_t>(bytes[offset + 2]) << 8U) |
           static_cast<std::uint32_t>(bytes[offset + 3]);
}

std::uint64_t readU64(const std::span<const std::uint8_t> bytes, const std::size_t offset)
{
    std::uint64_t result = 0;
    for (std::size_t index = 0; index < 8; ++index)
    {
        result = (result << 8U) | static_cast<std::uint64_t>(bytes[offset + index]);
    }
    return result;
}

} // namespace

void encodeFrameInto(const Frame& frame, std::vector<std::uint8_t>& out)
{
    if (frame.payload.size() > std::numeric_limits<std::uint32_t>::max())
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW frame payload is too large");
    }

    FrameHeader header = frame.header;
    header.magic = frame_magic;
    header.version = protocol_version_v1;
    header.payload_length = static_cast<std::uint32_t>(frame.payload.size());

    out.resize(frame_header_size + frame.payload.size());
    writeU32(out, 0, header.magic);
    writeU16(out, 4, header.version);
    writeU16(out, 6, static_cast<std::uint16_t>(header.message_type));
    writeU16(out, 8, header.flags);
    writeU16(out, 10, 0); // reserved
    writeU64(out, 12, header.request_id);
    writeU32(out, 20, header.payload_length);
    std::copy(frame.payload.begin(), frame.payload.end(), out.begin() + static_cast<std::ptrdiff_t>(frame_header_size));
}

std::vector<std::uint8_t> encodeFrame(const Frame& frame)
{
    std::vector<std::uint8_t> result;
    encodeFrameInto(frame, result);
    return result;
}

Frame decodeFrame(const std::span<const std::uint8_t> bytes)
{
    if (bytes.size() < frame_header_size)
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW frame is shorter than the header");
    }

    Frame frame;
    frame.header.magic = readU32(bytes, 0);
    if (frame.header.magic != frame_magic)
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW frame has invalid magic");
    }

    frame.header.version = readU16(bytes, 4);
    if (frame.header.version != protocol_version_v1)
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW frame has unsupported version");
    }

    frame.header.message_type = static_cast<MessageType>(readU16(bytes, 6));
    frame.header.flags = readU16(bytes, 8);
    frame.header.request_id = readU64(bytes, 12);
    frame.header.payload_length = readU32(bytes, 20);

    if (bytes.size() != frame_header_size + frame.header.payload_length)
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW frame payload length does not match buffer size");
    }

    frame.payload.assign(bytes.begin() + static_cast<std::ptrdiff_t>(frame_header_size), bytes.end());
    return frame;
}

} // namespace sessiongw
