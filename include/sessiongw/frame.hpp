#ifndef SESSIONGW_FRAME_HPP
#define SESSIONGW_FRAME_HPP

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace sessiongw
{

inline constexpr std::uint32_t frame_magic = 0x53475731U; // "SGW1"
inline constexpr std::uint16_t protocol_version_v1 = 1;
inline constexpr std::size_t frame_header_size = 24;

enum class MessageType : std::uint16_t
{
    // Session/control.
    hello = 1,
    hello_ok = 2,
    ping = 3,
    pong = 4,
    close = 5,
    ok = 6,
    error = 7,

    // Metadata SDK operations: describeTable(), getTableVersion().
    describe_table = 8,
    describe_table_result = 9,
    get_table_version = 10,
    get_table_version_result = 11,

    // Read SDK operations:
    // - pushed query: openPushedQuery() -> fetch()* -> closeCursor()
    // - table scan: openTableScan() -> fetch()* / fetch_positioned_rows()* -> closeCursor()
    open_pushed_query = 12,
    open_cursor_result = 13,
    fetch = 14,
    fetch_result = 15,
    close_cursor = 16,
    open_table_scan = 17,

    // Insert SDK operation: openTableInsert() -> insertRows()* -> closeOperation().
    open_table_insert = 18,
    open_table_operation_result = 19,
    insert_rows = 20,
    affected_rows_result = 21,
    close_operation = 22,

    // Transaction SDK operations.
    set_autocommit = 23,
    commit = 24,
    rollback = 25,

    // Reserved update/delete SDK operations. Server returns unsupported until
    // reviewed row-handle semantics and real providers exist.
    open_table_update = 26,
    update_rows = 27,
    open_table_delete = 28,
    delete_rows = 29,

    // Dedicated vector positioned read from an existing table-scan cursor.
    fetch_positioned_rows = 30,
    fetch_native = 31,
    fetch_native_result = 32,
    fetch_positioned_rows_native = 33
};

struct FrameHeader
{
    std::uint32_t magic = frame_magic;
    std::uint16_t version = protocol_version_v1;
    MessageType message_type = MessageType::hello;
    std::uint16_t flags = 0;
    std::uint64_t request_id = 0;
    std::uint32_t payload_length = 0;
};

struct Frame
{
    FrameHeader header;
    std::vector<std::uint8_t> payload;
};

void encodeFrameInto(const Frame& frame, std::vector<std::uint8_t>& out);
std::vector<std::uint8_t> encodeFrame(const Frame& frame);
Frame decodeFrame(std::span<const std::uint8_t> bytes);

} // namespace sessiongw

#endif // SESSIONGW_FRAME_HPP
