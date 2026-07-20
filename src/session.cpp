#include <sessiongw/session.hpp>

#include <sessiongw/errors.hpp>
#include <sessiongw/frame.hpp>
#include <sessiongw/native_read_batch.hpp>

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/record_batch.h>

#include <charconv>
#include <cstring>
#include <limits>
#include <sstream>
#include <utility>

namespace sessiongw
{
namespace
{

void require(bool condition, const char* message)
{
    if (!condition)
    {
        throw Error(ErrorCategory::protocol_error, message);
    }
}

void appendU8(std::vector<std::uint8_t>& out, std::uint8_t value) { out.push_back(value); }

std::optional<std::uint64_t> tableRowCount(const std::shared_ptr<arrow::Schema>& schema)
{
    if (schema == nullptr || schema->metadata() == nullptr)
        return std::nullopt;
    const auto encoded = schema->metadata()->Get("sessiongw.table.row_count");
    if (!encoded.ok())
        return std::nullopt;
    std::uint64_t rowCount = 0;
    const char* const begin = encoded->data();
    const char* const end = begin + encoded->size();
    const auto parsed = std::from_chars(begin, end, rowCount);
    require(parsed.ec == std::errc{} && parsed.ptr == end,
            "malformed SessionGW table row-count metadata");
    return rowCount;
}
void appendU16(std::vector<std::uint8_t>& out, std::uint16_t value)
{
    out.push_back(static_cast<std::uint8_t>(value >> 8U));
    out.push_back(static_cast<std::uint8_t>(value));
}
void appendU32(std::vector<std::uint8_t>& out, std::uint32_t value)
{
    for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<std::uint8_t>(value >> static_cast<unsigned>(shift)));
}
void appendU64(std::vector<std::uint8_t>& out, std::uint64_t value)
{
    for (int shift = 56; shift >= 0; shift -= 8)
        out.push_back(static_cast<std::uint8_t>(value >> static_cast<unsigned>(shift)));
}

std::uint8_t readU8(std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    require(offset < bytes.size(), "truncated SessionGW u8");
    return bytes[offset++];
}
std::uint16_t readU16(std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    require(offset <= bytes.size() && bytes.size() - offset >= 2U, "truncated SessionGW u16");
    const std::uint16_t value = static_cast<std::uint16_t>(bytes[offset] << 8U) | bytes[offset + 1U];
    offset += 2U;
    return value;
}
std::uint32_t readU32(std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    require(offset <= bytes.size() && bytes.size() - offset >= 4U, "truncated SessionGW u32");
    std::uint32_t value = 0;
    for (std::size_t index = 0; index < 4U; ++index) value = (value << 8U) | bytes[offset + index];
    offset += 4U;
    return value;
}
std::uint64_t readU64(std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    require(offset <= bytes.size() && bytes.size() - offset >= 8U, "truncated SessionGW u64");
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < 8U; ++index) value = (value << 8U) | bytes[offset + index];
    offset += 8U;
    return value;
}

void appendString16(std::vector<std::uint8_t>& out, const std::string& value)
{
    require(!value.empty() && value.size() <= std::numeric_limits<std::uint16_t>::max(),
            "invalid SessionGW short string");
    appendU16(out, static_cast<std::uint16_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}
void appendString32(std::vector<std::uint8_t>& out, const std::string& value)
{
    require(!value.empty() && value.size() <= std::numeric_limits<std::uint32_t>::max(),
            "invalid SessionGW string");
    appendU32(out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}
std::string readString16(std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    const std::size_t size = readU16(bytes, offset);
    require(offset <= bytes.size() && size <= bytes.size() - offset, "truncated SessionGW string");
    std::string result(reinterpret_cast<const char*>(bytes.data() + offset), size);
    offset += size;
    return result;
}
std::span<const std::uint8_t> readBytes32(std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    const std::size_t size = readU32(bytes, offset);
    require(offset <= bytes.size() && size <= bytes.size() - offset, "truncated SessionGW byte field");
    const auto result = bytes.subspan(offset, size);
    offset += size;
    return result;
}
void appendBytes32(std::vector<std::uint8_t>& out, std::span<const std::uint8_t> bytes)
{
    require(bytes.size() <= std::numeric_limits<std::uint32_t>::max(), "SessionGW byte field is too large");
    appendU32(out, static_cast<std::uint32_t>(bytes.size()));
    out.insert(out.end(), bytes.begin(), bytes.end());
}
void ensureConsumed(std::span<const std::uint8_t> bytes, std::size_t offset)
{
    require(offset == bytes.size(), "SessionGW payload has trailing bytes");
}

std::uint8_t expectedNativeKind(const std::shared_ptr<arrow::DataType>& type)
{
    switch (type->id())
    {
        case arrow::Type::BOOL: return 1U;
        case arrow::Type::INT64: return 2U;
        case arrow::Type::DOUBLE: return 3U;
        case arrow::Type::DECIMAL128: return 4U;
        case arrow::Type::DATE32: return 5U;
        case arrow::Type::TIMESTAMP: return 6U;
        case arrow::Type::STRING: return 7U;
        default: throw Error(ErrorCategory::unsupported_type, "unsupported native read column type");
    }
}

void validateNativeBatch(std::span<const std::uint8_t> bytes,
                         const std::shared_ptr<arrow::Schema>& schema)
{
    std::size_t offset = 0;
    require(readU32(bytes, offset) == 0x53475242U, "invalid native read batch magic");
    require(readU32(bytes, offset) == 1U, "unsupported native read batch version");
    const std::uint32_t rows = readU32(bytes, offset);
    const std::uint32_t columns = readU32(bytes, offset);
    require(columns == static_cast<std::uint32_t>(schema->num_fields()),
            "native read batch column count does not match cursor schema");
    require(readU32(bytes, offset) == 1U, "native read batch scalar encoding is not little-endian");
    require(readU32(bytes, offset) == 1U, "unsupported native read scalar ABI");
    for (std::uint32_t column = 0; column < columns; ++column)
    {
        const auto& type = schema->field(static_cast<int>(column))->type();
        const std::uint8_t kind = readU8(bytes, offset);
        const bool decimalKind = kind == 4U || kind == 8U || kind == 9U;
        require(kind == expectedNativeKind(type) ||
                    (type->id() == arrow::Type::DECIMAL128 && decimalKind),
                "native read column kind does not match cursor schema");
        require(readU8(bytes, offset) == 0U && readU8(bytes, offset) == 0U && readU8(bytes, offset) == 0U,
                "native read column reserved bits are nonzero");
        const std::int32_t scale = static_cast<std::int32_t>(readU32(bytes, offset));
        const std::uint32_t bufferCount = readU32(bytes, offset);
        const std::uint32_t expectedBuffers = type->id() == arrow::Type::STRING ? 3U : 2U;
        require(bufferCount == expectedBuffers, "native read column has an invalid buffer count");
        if (type->id() == arrow::Type::DECIMAL128)
            require(scale == std::static_pointer_cast<arrow::Decimal128Type>(type)->scale(),
                    "native read decimal scale does not match cursor schema");
        else
            require(scale == 0, "native read non-decimal column has a scale");

        const std::uint32_t nullSize = readU32(bytes, offset);
        require(nullSize == rows && offset <= bytes.size() && nullSize <= bytes.size() - offset,
                "native read NULL vector has an invalid extent");
        const std::size_t nullOffset = offset;
        for (std::uint32_t row = 0; row < rows; ++row)
            require(bytes[offset + row] == 0x00U || bytes[offset + row] == 0xffU,
                    "native read NULL vector contains an invalid value");
        offset += nullSize;

        if (type->id() == arrow::Type::STRING)
        {
            const std::uint32_t sizesSize = readU32(bytes, offset);
            require(sizesSize == static_cast<std::uint64_t>(rows) * 8U &&
                        offset <= bytes.size() && sizesSize <= bytes.size() - offset,
                    "native read string sizes have an invalid extent");
            std::uint64_t total = 0;
            for (std::uint32_t row = 0; row < rows; ++row)
            {
                const std::uint64_t size = readU64(bytes, offset);
                require(bytes[nullOffset + row] != 0xffU || size == 0U,
                        "native read NULL string has a nonzero size");
                require(size <= std::numeric_limits<std::uint64_t>::max() - total,
                        "native read string size sum overflows");
                total += size;
            }
            const std::uint32_t dataSize = readU32(bytes, offset);
            require(total == dataSize && offset <= bytes.size() && dataSize <= bytes.size() - offset,
                    "native read string data has an invalid extent");
            offset += dataSize;
        }
        else
        {
            std::uint32_t width = 8U;
            if (type->id() == arrow::Type::BOOL) width = 1U;
            else if (type->id() == arrow::Type::DATE32) width = 4U;
            else if (type->id() == arrow::Type::DECIMAL128)
                width = kind == 8U ? 4U : (kind == 9U ? 8U : 16U);
            const std::uint32_t dataSize = readU32(bytes, offset);
            require(dataSize == static_cast<std::uint64_t>(rows) * width &&
                        offset <= bytes.size() && dataSize <= bytes.size() - offset,
                    "native read fixed-width values have an invalid extent");
            if (type->id() == arrow::Type::BOOL)
                for (std::uint32_t row = 0; row < rows; ++row)
                    require(bytes[offset + row] <= 1U, "native read Boolean contains an invalid value");
            offset += dataSize;
        }
    }
    ensureConsumed(bytes, offset);
}

NativeFetchBatch decodeNativeFetchResult(const Frame& frame, const Cursor& cursor)
{
    std::size_t offset = 0;
    NativeFetchBatch result;
    result.cursor_id = readU64(frame.payload, offset);
    result.end_of_cursor = readU8(frame.payload, offset) != 0U;
    const auto rows = readBytes32(frame.payload, offset);
    if (!rows.empty()) validateNativeReadBatch(rows, cursor.schema);
    result.native_rows.assign(rows.begin(), rows.end());
    const std::uint32_t count = readU32(frame.payload, offset);
    result.row_locations.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index)
        result.row_locations.push_back({readU64(frame.payload, offset)});
    ensureConsumed(frame.payload, offset);
    require(result.cursor_id == cursor.id, "SessionGW native fetch cursor mismatch");
    return result;
}

ErrorCategory decodeErrorCategory(std::uint16_t value)
{
    if (value >= static_cast<std::uint16_t>(ErrorCategory::protocol_error) &&
        value <= static_cast<std::uint16_t>(ErrorCategory::transport_error))
        return static_cast<ErrorCategory>(value);
    return ErrorCategory::protocol_error;
}

std::shared_ptr<arrow::Buffer> ownedBuffer(std::span<const std::uint8_t> bytes)
{
    require(bytes.size() <= static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()),
            "Arrow IPC field is too large");
    auto allocated = arrow::AllocateBuffer(static_cast<std::int64_t>(bytes.size()));
    if (!allocated.ok()) throw Error(ErrorCategory::resource_limit, allocated.status().ToString());
    auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocated).ValueOrDie());
    if (!bytes.empty()) std::memcpy(buffer->mutable_data(), bytes.data(), bytes.size());
    return buffer;
}

std::shared_ptr<arrow::Schema> decodeSchema(std::span<const std::uint8_t> bytes)
{
    arrow::io::BufferReader reader(ownedBuffer(bytes));
    auto result = arrow::ipc::ReadSchema(&reader, nullptr);
    if (!result.ok()) throw Error(ErrorCategory::protocol_error, "invalid Arrow schema: " + result.status().ToString());
    return std::move(result).ValueOrDie();
}

std::vector<std::uint8_t> encodeSchema(const std::shared_ptr<arrow::Schema>& schema)
{
    require(schema != nullptr, "SessionGW table operation requires an Arrow schema");
    auto result = arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool());
    if (!result.ok()) throw Error(ErrorCategory::protocol_error, "cannot encode Arrow schema: " + result.status().ToString());
    const auto buffer = std::move(result).ValueOrDie();
    return {buffer->data(), buffer->data() + buffer->size()};
}

std::shared_ptr<arrow::RecordBatch> decodeBatch(const std::shared_ptr<arrow::Schema>& schema,
                                                std::span<const std::uint8_t> bytes)
{
    if (bytes.empty()) return {};
    require(schema != nullptr, "SessionGW cursor has no Arrow schema");
    arrow::io::BufferReader reader(ownedBuffer(bytes));
    arrow::ipc::DictionaryMemo memo;
    auto result = arrow::ipc::ReadRecordBatch(schema, &memo, arrow::ipc::IpcReadOptions::Defaults(), &reader);
    if (!result.ok()) throw Error(ErrorCategory::protocol_error, "invalid Arrow record batch: " + result.status().ToString());
    return std::move(result).ValueOrDie();
}

std::vector<std::uint8_t> tablePayload(const TableName& table)
{
    std::vector<std::uint8_t> payload;
    appendString16(payload, table.schema);
    appendString16(payload, table.table);
    return payload;
}

void appendLocations(std::vector<std::uint8_t>& payload, std::span<const RowLocation> locations)
{
    require(locations.size() <= std::numeric_limits<std::uint32_t>::max(), "too many SessionGW row locations");
    appendU32(payload, static_cast<std::uint32_t>(locations.size()));
    for (const RowLocation location : locations) appendU64(payload, location.row_number);
}

} // namespace

void validateNativeReadBatch(const std::span<const std::uint8_t> bytes,
                             const std::shared_ptr<arrow::Schema>& schema)
{
    require(schema != nullptr, "native read validation requires a cursor schema");
    validateNativeBatch(bytes, schema);
}

class Session::Impl
{
public:
    explicit Impl(WebSocketConnection connection) : connection_(std::move(connection)) {}

    Frame request(MessageType type,
                  std::vector<std::uint8_t> payload,
                  MessageType expected,
                  bool uncertain_on_transport = false)
    {
        require(!closed_, "SessionGW session is closed");
        const std::uint64_t requestId = nextRequestId_++;
        try
        {
            connection_.sendFrame(type, requestId, payload);
            Frame response = connection_.receiveFrame(requestId);
            if (response.header.message_type == MessageType::error)
            {
                std::size_t offset = 0;
                const ErrorCategory category = decodeErrorCategory(readU16(response.payload, offset));
                const std::string message = readString16(response.payload, offset);
                ensureConsumed(response.payload, offset);
                throw Error(category, message);
            }
            require(response.header.message_type == expected, "unexpected SessionGW response type");
            return response;
        }
        catch (const Error& error)
        {
            if (uncertain_on_transport && error.category() == ErrorCategory::transport_error)
            {
                std::ostringstream message;
                message << "SessionGW outcome unknown after completion request " << requestId
                        << "; the operation must not be replayed";
                throw Error(ErrorCategory::outcome_unknown, message.str());
            }
            throw;
        }
    }

    TableOperation openColumnOperation(MessageType type,
                                       const TableName& table,
                                       const std::vector<std::string>& columns,
                                       std::uint32_t max_rows_per_batch,
                                       const std::shared_ptr<arrow::Schema>& schema)
    {
        require(columns.size() <= std::numeric_limits<std::uint32_t>::max(),
                "too many table-operation columns");
        std::vector<std::uint8_t> payload;
        appendString32(payload, table.schema);
        appendString32(payload, table.table);
        appendU32(payload, static_cast<std::uint32_t>(columns.size()));
        for (const auto& column : columns) appendString32(payload, column);
        appendU32(payload, max_rows_per_batch);
        const auto schemaBytes = encodeSchema(schema);
        appendBytes32(payload, schemaBytes);
        const Frame frame = request(type, std::move(payload), MessageType::open_table_operation_result);
        std::size_t offset = 0;
        TableOperation operation;
        operation.id = readU64(frame.payload, offset);
        const auto acceptedSchema = readBytes32(frame.payload, offset);
        operation.accepted_schema_ipc.assign(acceptedSchema.begin(), acceptedSchema.end());
        operation.accepted_schema = decodeSchema(acceptedSchema);
        ensureConsumed(frame.payload, offset);
        return operation;
    }

    WebSocketConnection connection_;
    Capabilities capabilities_;
    std::uint64_t nextRequestId_ = 1;
    bool closed_ = false;
};

Session::Session(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Session::~Session()
{
    if (impl_ != nullptr && !impl_->closed_)
    {
        try { close(); } catch (...) { try { impl_->connection_.close(); } catch (...) {} }
    }
}
Session::Session(Session&&) noexcept = default;
Session& Session::operator=(Session&&) noexcept = default;

Session Session::connect(const WebSocketOptions& options)
{
    auto impl = std::make_unique<Impl>(WebSocketConnection::connectAndLogin(options));
    impl->connection_.enterSessionGateway();
    const Frame hello = impl->request(MessageType::hello, {}, MessageType::hello_ok);
    std::size_t offset = 0;
    require(readU16(hello.payload, offset) == protocol_version_v1, "unsupported SessionGW protocol version");
    const std::uint16_t count = readU16(hello.payload, offset);
    for (std::uint16_t index = 0; index < count; ++index)
        impl->capabilities_.add(static_cast<Capability>(readU16(hello.payload, offset)));
    ensureConsumed(hello.payload, offset);
    return Session(std::move(impl));
}

const Capabilities& Session::capabilities() const
{
    require(impl_ != nullptr, "SessionGW session is not connected");
    return impl_->capabilities_;
}
const WebSocketStatistics& Session::statistics() const
{
    require(impl_ != nullptr, "SessionGW session is not connected");
    return impl_->connection_.statistics();
}

TableMetadata Session::describeTable(const TableName& table)
{
    const Frame frame = impl_->request(MessageType::describe_table, tablePayload(table), MessageType::describe_table_result);
    std::size_t offset = 0;
    TableMetadata result;
    result.table.schema = readString16(frame.payload, offset);
    result.table.table = readString16(frame.payload, offset);
    result.version = readString16(frame.payload, offset);
    const auto schema = readBytes32(frame.payload, offset);
    result.schema_ipc.assign(schema.begin(), schema.end());
    result.schema = decodeSchema(schema);
    result.row_count = tableRowCount(result.schema);
    ensureConsumed(frame.payload, offset);
    return result;
}

std::string Session::getTableVersion(const TableName& table)
{
    const Frame frame = impl_->request(MessageType::get_table_version, tablePayload(table), MessageType::get_table_version_result);
    std::size_t offset = 0;
    (void)readString16(frame.payload, offset);
    (void)readString16(frame.payload, offset);
    std::string version = readString16(frame.payload, offset);
    ensureConsumed(frame.payload, offset);
    return version;
}

Cursor Session::openPushedQuery(const std::string& sql)
{
    std::vector<std::uint8_t> payload;
    appendString32(payload, sql);
    const Frame frame = impl_->request(MessageType::open_pushed_query, std::move(payload), MessageType::open_cursor_result);
    std::size_t offset = 0;
    Cursor cursor;
    cursor.id = readU64(frame.payload, offset);
    const auto schema = readBytes32(frame.payload, offset);
    cursor.schema_ipc.assign(schema.begin(), schema.end());
    cursor.schema = decodeSchema(schema);
    ensureConsumed(frame.payload, offset);
    return cursor;
}

Cursor Session::openTableScan(const TableName& table,
                              const std::vector<std::string>& columns,
                              bool include_row_locations)
{
    require(columns.size() <= std::numeric_limits<std::uint32_t>::max(), "too many scan columns");
    std::vector<std::uint8_t> payload;
    appendString32(payload, table.schema);
    appendString32(payload, table.table);
    appendU32(payload, static_cast<std::uint32_t>(columns.size()));
    for (const auto& column : columns) appendString32(payload, column);
    appendU8(payload, include_row_locations ? 1U : 0U);
    const Frame frame = impl_->request(MessageType::open_table_scan, std::move(payload), MessageType::open_cursor_result);
    std::size_t offset = 0;
    Cursor cursor;
    cursor.id = readU64(frame.payload, offset);
    const auto schema = readBytes32(frame.payload, offset);
    cursor.schema_ipc.assign(schema.begin(), schema.end());
    cursor.schema = decodeSchema(schema);
    ensureConsumed(frame.payload, offset);
    return cursor;
}

FetchBatch Session::fetch(const Cursor& cursor, std::uint32_t max_rows, std::uint32_t max_bytes)
{
    FetchBatch result = fetchIpc(cursor, max_rows, max_bytes);
    result.rows = decodeBatch(cursor.schema, result.rows_ipc);
    return result;
}

FetchBatch Session::fetchIpc(const Cursor& cursor, std::uint32_t max_rows, std::uint32_t max_bytes)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursor.id);
    appendU32(payload, max_rows);
    appendU32(payload, max_bytes);
    const Frame frame = impl_->request(MessageType::fetch, std::move(payload), MessageType::fetch_result);
    std::size_t offset = 0;
    FetchBatch result;
    result.cursor_id = readU64(frame.payload, offset);
    result.end_of_cursor = readU8(frame.payload, offset) != 0U;
    const auto rows = readBytes32(frame.payload, offset);
    result.rows_ipc.assign(rows.begin(), rows.end());
    const std::uint32_t count = readU32(frame.payload, offset);
    result.row_locations.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) result.row_locations.push_back({readU64(frame.payload, offset)});
    ensureConsumed(frame.payload, offset);
    require(result.cursor_id == cursor.id, "SessionGW fetch cursor mismatch");
    return result;
}

FetchBatch Session::fetchPositioned(const Cursor& cursor,
                                    std::span<const RowLocation> locations,
                                    std::uint32_t max_bytes)
{
    FetchBatch result = fetchPositionedIpc(cursor, locations, max_bytes);
    result.rows = decodeBatch(cursor.schema, result.rows_ipc);
    return result;
}

FetchBatch Session::fetchPositionedIpc(const Cursor& cursor,
                                       std::span<const RowLocation> locations,
                                       std::uint32_t max_bytes)
{
    require(!locations.empty(), "positioned fetch requires row locations");
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursor.id);
    appendU32(payload, max_bytes);
    appendLocations(payload, locations);
    const Frame frame = impl_->request(MessageType::fetch_positioned_rows, std::move(payload), MessageType::fetch_result);
    std::size_t offset = 0;
    FetchBatch result;
    result.cursor_id = readU64(frame.payload, offset);
    result.end_of_cursor = readU8(frame.payload, offset) != 0U;
    const auto rows = readBytes32(frame.payload, offset);
    result.rows_ipc.assign(rows.begin(), rows.end());
    const std::uint32_t count = readU32(frame.payload, offset);
    result.row_locations.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) result.row_locations.push_back({readU64(frame.payload, offset)});
    ensureConsumed(frame.payload, offset);
    require(result.cursor_id == cursor.id, "SessionGW positioned fetch cursor mismatch");
    return result;
}

NativeFetchBatch Session::fetchNative(const Cursor& cursor,
                                      std::uint32_t max_rows,
                                      std::uint32_t max_bytes)
{
    require(capabilities().supports(Capability::native_table_read_batch_v1),
            "server does not support native table read batches");
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursor.id);
    appendU32(payload, max_rows);
    appendU32(payload, max_bytes);
    return decodeNativeFetchResult(
        impl_->request(MessageType::fetch_native, std::move(payload), MessageType::fetch_native_result), cursor);
}

NativeFetchBatch Session::fetchPositionedNative(const Cursor& cursor,
                                                std::span<const RowLocation> locations,
                                                std::uint32_t max_bytes)
{
    require(capabilities().supports(Capability::native_table_read_batch_v1),
            "server does not support native table read batches");
    require(!locations.empty(), "positioned native fetch requires row locations");
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursor.id);
    appendU32(payload, max_bytes);
    appendLocations(payload, locations);
    return decodeNativeFetchResult(
        impl_->request(MessageType::fetch_positioned_rows_native,
                       std::move(payload),
                       MessageType::fetch_native_result),
        cursor);
}

void Session::closeCursor(Cursor& cursor)
{
    require(cursor.id != 0U, "SessionGW cursor is not open");
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursor.id);
    (void)impl_->request(MessageType::close_cursor, std::move(payload), MessageType::ok);
    cursor.id = 0;
    cursor.schema.reset();
}

TableOperation Session::openTableInsert(const TableName& table,
                                        const std::vector<std::string>& columns,
                                        std::uint32_t max_rows_per_batch,
                                        const std::shared_ptr<arrow::Schema>& schema)
{
    return impl_->openColumnOperation(MessageType::open_table_insert, table, columns, max_rows_per_batch, schema);
}
TableOperation Session::openTableUpdate(const TableName& table,
                                        const std::vector<std::string>& columns,
                                        std::uint32_t max_rows_per_batch,
                                        const std::shared_ptr<arrow::Schema>& schema)
{
    return impl_->openColumnOperation(MessageType::open_table_update, table, columns, max_rows_per_batch, schema);
}
TableOperation Session::openTableDelete(const TableName& table, std::uint32_t max_rows_per_batch)
{
    std::vector<std::uint8_t> payload;
    appendString32(payload, table.schema);
    appendString32(payload, table.table);
    appendU32(payload, max_rows_per_batch);
    const Frame frame = impl_->request(MessageType::open_table_delete, std::move(payload), MessageType::open_table_operation_result);
    std::size_t offset = 0;
    TableOperation operation;
    operation.id = readU64(frame.payload, offset);
    const auto acceptedSchema = readBytes32(frame.payload, offset);
    operation.accepted_schema_ipc.assign(acceptedSchema.begin(), acceptedSchema.end());
    operation.accepted_schema = decodeSchema(acceptedSchema);
    ensureConsumed(frame.payload, offset);
    return operation;
}

static std::vector<std::uint8_t> nativePayload(std::uint64_t operation_id,
                                               std::span<const std::uint8_t> batch)
{
    require(!batch.empty() && batch.size() <= std::numeric_limits<std::uint32_t>::max(), "invalid native write batch");
    std::vector<std::uint8_t> payload;
    appendU64(payload, operation_id);
    appendU32(payload, static_cast<std::uint32_t>(batch.size()));
    alignNativeWriteBatch(payload);
    payload.insert(payload.end(), batch.begin(), batch.end());
    return payload;
}
static std::uint64_t affectedRows(const Frame& frame)
{
    std::size_t offset = 0;
    const std::uint64_t rows = readU64(frame.payload, offset);
    ensureConsumed(frame.payload, offset);
    return rows;
}

std::uint64_t Session::insertRows(const TableOperation& operation, std::span<const std::uint8_t> native_batch)
{
    return affectedRows(impl_->request(MessageType::insert_rows,
                                       nativePayload(operation.id, native_batch),
                                       MessageType::affected_rows_result));
}
std::uint64_t Session::updateRows(const TableOperation& operation,
                                  std::span<const RowLocation> locations,
                                  std::span<const std::uint8_t> native_batch)
{
    require(!locations.empty(), "SessionGW update requires row locations");
    require(!native_batch.empty() && native_batch.size() <= std::numeric_limits<std::uint32_t>::max(),
            "invalid native update batch");
    std::vector<std::uint8_t> payload;
    appendU64(payload, operation.id);
    appendLocations(payload, locations);
    appendU32(payload, static_cast<std::uint32_t>(native_batch.size()));
    alignNativeWriteBatch(payload);
    payload.insert(payload.end(), native_batch.begin(), native_batch.end());
    return affectedRows(impl_->request(MessageType::update_rows, std::move(payload), MessageType::affected_rows_result));
}
std::uint64_t Session::deleteRows(const TableOperation& operation, std::span<const RowLocation> locations)
{
    require(!locations.empty(), "SessionGW delete requires row locations");
    std::vector<std::uint8_t> payload;
    appendU64(payload, operation.id);
    appendLocations(payload, locations);
    return affectedRows(impl_->request(MessageType::delete_rows, std::move(payload), MessageType::affected_rows_result));
}
void Session::closeOperation(TableOperation& operation)
{
    require(operation.id != 0U, "SessionGW operation is not open");
    std::vector<std::uint8_t> payload;
    appendU64(payload, operation.id);
    (void)impl_->request(MessageType::close_operation, std::move(payload), MessageType::ok, true);
    operation.id = 0;
    operation.accepted_schema.reset();
}

void Session::setAutocommit(bool enabled)
{
    (void)impl_->request(MessageType::set_autocommit, {static_cast<std::uint8_t>(enabled ? 1U : 0U)}, MessageType::ok);
}
void Session::commit() { (void)impl_->request(MessageType::commit, {}, MessageType::ok, true); }
void Session::rollback() { (void)impl_->request(MessageType::rollback, {}, MessageType::ok); }
void Session::close()
{
    if (impl_ == nullptr || impl_->closed_) return;
    (void)impl_->request(MessageType::close, {}, MessageType::ok);
    impl_->closed_ = true;
    impl_->connection_.close();
}

} // namespace sessiongw
