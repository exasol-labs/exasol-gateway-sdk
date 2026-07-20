#include <sessiongw/c_api.h>

#include <sessiongw/errors.hpp>
#include <sessiongw/session.hpp>

#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

struct sessiongw_c_session { sessiongw::Session value; };
struct sessiongw_c_metadata { sessiongw::TableMetadata value; };
struct sessiongw_c_cursor { sessiongw::Cursor value; };
struct sessiongw_c_fetch {
    sessiongw::FetchBatch value;
    std::vector<std::uint64_t> locations;
};
struct native_column_view {
    std::uint8_t kind = 0;
    std::int32_t scale = 0;
    std::size_t null_offset = 0;
    std::size_t null_size = 0;
    std::size_t sizes_offset = 0;
    std::size_t sizes_size = 0;
    std::size_t data_offset = 0;
    std::size_t data_size = 0;
};
struct sessiongw_c_native_fetch {
    sessiongw::NativeFetchBatch value;
    std::uint32_t row_count = 0;
    std::vector<native_column_view> columns;
    std::vector<std::uint64_t> locations;
};
struct sessiongw_c_operation { sessiongw::TableOperation value; };

namespace
{
thread_local std::uint16_t lastCategory = static_cast<std::uint16_t>(sessiongw::ErrorCategory::transport_error);
thread_local std::string lastMessage;

void require(const bool condition, const char* message)
{
    if (!condition) throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error, message);
}

template <typename Function>
int protect(Function&& function) noexcept
{
    try
    {
        lastMessage.clear();
        function();
        return 0;
    }
    catch (const sessiongw::Error& error)
    {
        lastCategory = static_cast<std::uint16_t>(error.category());
        lastMessage = error.what();
    }
    catch (const std::exception& error)
    {
        lastCategory = static_cast<std::uint16_t>(sessiongw::ErrorCategory::internal_error);
        lastMessage = error.what();
    }
    catch (...)
    {
        lastCategory = static_cast<std::uint16_t>(sessiongw::ErrorCategory::internal_error);
        lastMessage = "unknown SessionGW SDK error";
    }
    return -1;
}

std::string text(const char* value, const char* message)
{
    require(value != nullptr && *value != '\0', message);
    return value;
}

std::string jsonEscape(const std::string& value)
{
    std::string result;
    result.reserve(value.size());
    for (const unsigned char byte : value)
    {
        switch (byte)
        {
            case '\\': result += "\\\\"; break;
            case '"': result += "\\\""; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            default:
                require(byte >= 0x20U, "SessionGW SQL contains an invalid control byte");
                result.push_back(static_cast<char>(byte));
        }
    }
    return result;
}

sessiongw::WebSocketOptions options(const sessiongw_c_options* input)
{
    require(input != nullptr, "SessionGW options are required");
    sessiongw::WebSocketOptions result;
    result.host = text(input->host, "SessionGW host is required");
    result.port = input->port;
    result.user = text(input->user, "SessionGW user is required");
    result.password = input->password == nullptr ? std::string{} : input->password;
    const std::string mode = input->tls_mode == nullptr ? "verify" : input->tls_mode;
    if (mode == "verify") result.tls_mode = sessiongw::WebSocketTlsMode::tls_verify;
    else if (mode == "skip_verify") result.tls_mode = sessiongw::WebSocketTlsMode::tls_skip_verify_for_test_only;
    else if (mode == "plain") result.tls_mode = sessiongw::WebSocketTlsMode::plain_for_test_only;
    else throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error, "invalid SessionGW TLS mode");
    if (input->ca_file != nullptr) result.ca_file = input->ca_file;
    result.instrumentation_enabled = input->instrumentation_enabled != 0;
    return result;
}

sessiongw::TableName tableName(const char* schema, const char* table)
{
    return {text(schema, "SessionGW schema is required"), text(table, "SessionGW table is required")};
}

std::vector<std::string> columnNames(const char* const* columns, const std::size_t count)
{
    require(count <= std::numeric_limits<std::uint32_t>::max(), "too many SessionGW columns");
    require(count == 0U || columns != nullptr, "SessionGW columns are required");
    std::vector<std::string> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index)
        result.push_back(text(columns[index], "SessionGW column name is required"));
    return result;
}

std::uint32_t nativeU32(const std::vector<std::uint8_t>& bytes, std::size_t& offset)
{
    require(offset <= bytes.size() && bytes.size() - offset >= 4U, "truncated native read batch");
    std::uint32_t value = 0;
    for (unsigned index = 0; index < 4U; ++index) value = (value << 8U) | bytes[offset + index];
    offset += 4U;
    return value;
}

void nativeSkip(const std::vector<std::uint8_t>& bytes, std::size_t& offset, const std::size_t size)
{
    require(offset <= bytes.size() && size <= bytes.size() - offset, "truncated native read buffer");
    offset += size;
}

void indexNativeBatch(sessiongw_c_native_fetch& fetch)
{
    const auto& bytes = fetch.value.native_rows;
    if (bytes.empty()) return;
    std::size_t offset = 0;
    require(nativeU32(bytes, offset) == 0x53475242U && nativeU32(bytes, offset) == 1U,
            "invalid native read batch");
    fetch.row_count = nativeU32(bytes, offset);
    const std::uint32_t columnCount = nativeU32(bytes, offset);
    require(nativeU32(bytes, offset) == 1U && nativeU32(bytes, offset) == 1U,
            "unsupported native read scalar ABI");
    fetch.columns.reserve(columnCount);
    for (std::uint32_t column = 0; column < columnCount; ++column)
    {
        require(offset <= bytes.size() && bytes.size() - offset >= 4U, "truncated native read column");
        native_column_view view;
        view.kind = bytes[offset];
        offset += 4U;
        view.scale = static_cast<std::int32_t>(nativeU32(bytes, offset));
        const std::uint32_t buffers = nativeU32(bytes, offset);
        require(buffers == (view.kind == 7U ? 3U : 2U), "invalid native read buffer count");
        view.null_size = nativeU32(bytes, offset);
        view.null_offset = offset;
        nativeSkip(bytes, offset, view.null_size);
        if (view.kind == 7U)
        {
            view.sizes_size = nativeU32(bytes, offset);
            view.sizes_offset = offset;
            nativeSkip(bytes, offset, view.sizes_size);
        }
        view.data_size = nativeU32(bytes, offset);
        view.data_offset = offset;
        nativeSkip(bytes, offset, view.data_size);
        fetch.columns.push_back(view);
    }
    require(offset == bytes.size(), "native read batch has trailing bytes");
}

std::vector<sessiongw::RowLocation> locations(const std::uint64_t* rows, const std::size_t count)
{
    require(count <= std::numeric_limits<std::uint32_t>::max(), "too many SessionGW row locations");
    require(count == 0U || rows != nullptr, "SessionGW row locations are required");
    std::vector<sessiongw::RowLocation> result;
    result.reserve(count);
    for (std::size_t index = 0; index < count; ++index) result.push_back({rows[index]});
    return result;
}

std::shared_ptr<arrow::Schema> decodeSchema(const std::uint8_t* data, const std::size_t size)
{
    require(data != nullptr && size != 0U, "SessionGW Arrow schema is required");
    require(size <= static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()),
            "SessionGW Arrow schema is too large");
    auto allocated = arrow::AllocateBuffer(static_cast<std::int64_t>(size));
    if (!allocated.ok()) throw sessiongw::Error(sessiongw::ErrorCategory::resource_limit, allocated.status().ToString());
    auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocated).ValueOrDie());
    std::memcpy(buffer->mutable_data(), data, size);
    arrow::io::BufferReader reader(buffer);
    auto schema = arrow::ipc::ReadSchema(&reader, nullptr);
    if (!schema.ok()) throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                             "invalid Arrow schema: " + schema.status().ToString());
    return std::move(schema).ValueOrDie();
}

std::shared_ptr<arrow::Schema> projectSchema(const std::shared_ptr<arrow::Schema>& schema,
                                             const std::vector<std::string>& columns)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columns.size());
    for (const std::string& column : columns)
    {
        const int index = schema->GetFieldIndex(column);
        require(index >= 0, "SessionGW operation column is absent from the Arrow schema");
        fields.push_back(schema->field(index));
    }
    return arrow::schema(std::move(fields), schema->metadata());
}

template <typename T>
const std::uint8_t* bytes(const std::vector<T>& value, std::size_t* size)
{
    if (size != nullptr) *size = value.size() * sizeof(T);
    return value.empty() ? nullptr : reinterpret_cast<const std::uint8_t*>(value.data());
}
} // namespace

extern "C" {

uint16_t sessiongw_c_last_error_category(void) { return lastCategory; }
const char* sessiongw_c_last_error_message(void) { return lastMessage.c_str(); }

int sessiongw_c_connect(const sessiongw_c_options* input, sessiongw_c_session** out)
{
    return protect([&] { require(out != nullptr, "SessionGW output session is required"); *out = new sessiongw_c_session{sessiongw::Session::connect(options(input))}; });
}
int sessiongw_c_connect_with_client_name(const sessiongw_c_options* input,
                                         const char* client_name,
                                         sessiongw_c_session** out)
{
    return protect([&] {
        require(out != nullptr, "SessionGW output session is required");
        sessiongw::WebSocketOptions converted = options(input);
        converted.client_name = text(client_name, "SessionGW client name is required");
        require(!converted.client_name.empty(), "SessionGW client name must not be empty");
        *out = new sessiongw_c_session{sessiongw::Session::connect(converted)};
    });
}
int sessiongw_c_execute_sql(const sessiongw_c_options* input, const char* sql)
{
    return protect([&] {
        auto connection = sessiongw::WebSocketConnection::connectAndLogin(options(input));
        const std::string request = "{\"command\":\"execute\",\"sqlText\":\"" +
                                    jsonEscape(text(sql, "SessionGW SQL is required")) + "\"}";
        const std::string response = connection.sendJsonCommand(request);
        connection.close();
        if (response.find("\"status\":\"ok\"") == std::string::npos &&
            response.find("\"status\": \"ok\"") == std::string::npos)
            throw sessiongw::Error(sessiongw::ErrorCategory::internal_error,
                                   "SessionGW SQL command failed: " + response);
    });
}
void sessiongw_c_session_destroy(sessiongw_c_session* session) { delete session; }
int sessiongw_c_close(sessiongw_c_session* session)
{ return protect([&] { require(session != nullptr, "SessionGW session is required"); session->value.close(); }); }
int sessiongw_c_statistics_get(const sessiongw_c_session* session, sessiongw_c_statistics* out)
{
    return protect([&] {
        require(session != nullptr && out != nullptr, "SessionGW statistics arguments are required");
        const auto& stats = session->value.statistics();
        *out = {stats.sessiongw_frames_sent, stats.sessiongw_payload_bytes_sent,
                stats.sessiongw_payload_bytes_received,
                stats.transport_write_nanoseconds + stats.transport_read_nanoseconds};
    });
}

int sessiongw_c_transport_profile_get(const sessiongw_c_session* session,
                                      sessiongw_c_transport_profile* out)
{
    return protect([&] {
        require(session != nullptr && out != nullptr, "SessionGW transport profile arguments are required");
        const auto& stats = session->value.statistics();
        *out = {stats.transport_write_calls,
                stats.transport_read_calls,
                stats.transport_read_iterations,
                stats.transport_bytes_written,
                stats.transport_bytes_read,
                stats.transport_write_nanoseconds,
                stats.transport_read_nanoseconds,
                stats.websocket_header_read_calls,
                stats.websocket_header_read_bytes,
                stats.websocket_header_read_nanoseconds,
                stats.websocket_payload_read_calls,
                stats.websocket_payload_read_bytes,
                stats.websocket_payload_read_nanoseconds,
                stats.sessiongw_frame_decode_nanoseconds};
    });
}

int sessiongw_c_describe_table(sessiongw_c_session* session, const char* schema, const char* table,
                               sessiongw_c_metadata** out)
{
    return protect([&] { require(session != nullptr && out != nullptr, "SessionGW metadata arguments are required"); *out = new sessiongw_c_metadata{session->value.describeTable(tableName(schema, table))}; });
}
void sessiongw_c_metadata_destroy(sessiongw_c_metadata* metadata) { delete metadata; }
const char* sessiongw_c_metadata_schema_name(const sessiongw_c_metadata* value) { return value == nullptr ? nullptr : value->value.table.schema.c_str(); }
const char* sessiongw_c_metadata_table_name(const sessiongw_c_metadata* value) { return value == nullptr ? nullptr : value->value.table.table.c_str(); }
const char* sessiongw_c_metadata_version(const sessiongw_c_metadata* value) { return value == nullptr ? nullptr : value->value.version.c_str(); }
int sessiongw_c_metadata_has_row_count(const sessiongw_c_metadata* value)
{ return value != nullptr && value->value.row_count.has_value() ? 1 : 0; }
uint64_t sessiongw_c_metadata_row_count(const sessiongw_c_metadata* value)
{ return value != nullptr && value->value.row_count.has_value() ? *value->value.row_count : 0U; }
const uint8_t* sessiongw_c_metadata_schema_ipc(const sessiongw_c_metadata* value, size_t* size)
{ if (value == nullptr) { if (size != nullptr) *size = 0; return nullptr; } return bytes(value->value.schema_ipc, size); }
int sessiongw_c_get_table_version(sessiongw_c_session* session, const char* schema, const char* table,
                                  sessiongw_c_metadata** out)
{
    return protect([&] {
        require(session != nullptr && out != nullptr, "SessionGW version arguments are required");
        sessiongw::TableMetadata result;
        result.table = tableName(schema, table);
        result.version = session->value.getTableVersion(result.table);
        *out = new sessiongw_c_metadata{std::move(result)};
    });
}

int sessiongw_c_open_pushed_query(sessiongw_c_session* session, const char* sql, sessiongw_c_cursor** out)
{ return protect([&] { require(session != nullptr && out != nullptr, "SessionGW cursor arguments are required"); *out = new sessiongw_c_cursor{session->value.openPushedQuery(text(sql, "SessionGW SQL is required"))}; }); }
int sessiongw_c_open_table_scan(sessiongw_c_session* session, const char* schema, const char* table,
                                const char* const* columns, size_t count, int include, sessiongw_c_cursor** out)
{ return protect([&] { require(session != nullptr && out != nullptr, "SessionGW cursor arguments are required"); *out = new sessiongw_c_cursor{session->value.openTableScan(tableName(schema, table), columnNames(columns, count), include != 0)}; }); }
void sessiongw_c_cursor_destroy(sessiongw_c_cursor* cursor) { delete cursor; }
uint64_t sessiongw_c_cursor_id(const sessiongw_c_cursor* cursor) { return cursor == nullptr ? 0U : cursor->value.id; }
const uint8_t* sessiongw_c_cursor_schema_ipc(const sessiongw_c_cursor* cursor, size_t* size)
{ if (cursor == nullptr) { if (size != nullptr) *size = 0; return nullptr; } return bytes(cursor->value.schema_ipc, size); }

int sessiongw_c_fetch_rows(sessiongw_c_session* session, const sessiongw_c_cursor* cursor,
                           uint32_t max_rows, uint32_t max_bytes, sessiongw_c_fetch** out)
{
    return protect([&] {
        require(session != nullptr && cursor != nullptr && out != nullptr, "SessionGW fetch arguments are required");
        auto result = std::make_unique<sessiongw_c_fetch>();
        result->value = session->value.fetchIpc(cursor->value, max_rows, max_bytes);
        for (const auto row : result->value.row_locations) result->locations.push_back(row.row_number);
        *out = result.release();
    });
}
int sessiongw_c_fetch_positioned(sessiongw_c_session* session, const sessiongw_c_cursor* cursor,
                                 const uint64_t* rows, size_t count, uint32_t max_bytes,
                                 sessiongw_c_fetch** out)
{
    return protect([&] {
        require(session != nullptr && cursor != nullptr && out != nullptr, "SessionGW positioned fetch arguments are required");
        const auto requested = locations(rows, count);
        auto result = std::make_unique<sessiongw_c_fetch>();
        result->value = session->value.fetchPositionedIpc(cursor->value, requested, max_bytes);
        for (const auto row : result->value.row_locations) result->locations.push_back(row.row_number);
        *out = result.release();
    });
}
void sessiongw_c_fetch_destroy(sessiongw_c_fetch* fetch) { delete fetch; }
uint64_t sessiongw_c_fetch_cursor_id(const sessiongw_c_fetch* fetch) { return fetch == nullptr ? 0U : fetch->value.cursor_id; }
int sessiongw_c_fetch_end(const sessiongw_c_fetch* fetch) { return fetch != nullptr && fetch->value.end_of_cursor ? 1 : 0; }
const uint8_t* sessiongw_c_fetch_rows_ipc(const sessiongw_c_fetch* fetch, size_t* size)
{ if (fetch == nullptr) { if (size != nullptr) *size = 0; return nullptr; } return bytes(fetch->value.rows_ipc, size); }
const uint64_t* sessiongw_c_fetch_row_locations(const sessiongw_c_fetch* fetch, size_t* count)
{ if (count != nullptr) *count = fetch == nullptr ? 0U : fetch->locations.size(); return fetch == nullptr || fetch->locations.empty() ? nullptr : fetch->locations.data(); }
int sessiongw_c_fetch_native(sessiongw_c_session* session, const sessiongw_c_cursor* cursor,
                             uint32_t max_rows, uint32_t max_bytes,
                             sessiongw_c_native_fetch** out)
{
    return protect([&] {
        require(session != nullptr && cursor != nullptr && out != nullptr,
                "SessionGW native fetch arguments are required");
        auto result = std::make_unique<sessiongw_c_native_fetch>();
        result->value = session->value.fetchNative(cursor->value, max_rows, max_bytes);
        for (const auto row : result->value.row_locations) result->locations.push_back(row.row_number);
        indexNativeBatch(*result);
        *out = result.release();
    });
}
int sessiongw_c_fetch_positioned_native(sessiongw_c_session* session,
                                        const sessiongw_c_cursor* cursor,
                                        const uint64_t* rows, size_t count,
                                        uint32_t max_bytes, sessiongw_c_native_fetch** out)
{
    return protect([&] {
        require(session != nullptr && cursor != nullptr && out != nullptr,
                "SessionGW positioned native fetch arguments are required");
        const auto requested = locations(rows, count);
        auto result = std::make_unique<sessiongw_c_native_fetch>();
        result->value = session->value.fetchPositionedNative(cursor->value, requested, max_bytes);
        for (const auto row : result->value.row_locations) result->locations.push_back(row.row_number);
        indexNativeBatch(*result);
        *out = result.release();
    });
}
void sessiongw_c_native_fetch_destroy(sessiongw_c_native_fetch* fetch) { delete fetch; }
uint64_t sessiongw_c_native_fetch_cursor_id(const sessiongw_c_native_fetch* fetch)
{ return fetch == nullptr ? 0U : fetch->value.cursor_id; }
int sessiongw_c_native_fetch_end(const sessiongw_c_native_fetch* fetch)
{ return fetch != nullptr && fetch->value.end_of_cursor ? 1 : 0; }
uint32_t sessiongw_c_native_fetch_row_count(const sessiongw_c_native_fetch* fetch)
{ return fetch == nullptr ? 0U : fetch->row_count; }
size_t sessiongw_c_native_fetch_column_count(const sessiongw_c_native_fetch* fetch)
{ return fetch == nullptr ? 0U : fetch->columns.size(); }
uint8_t sessiongw_c_native_fetch_column_kind(const sessiongw_c_native_fetch* fetch, size_t column)
{ return fetch == nullptr || column >= fetch->columns.size() ? 0U : fetch->columns[column].kind; }
int32_t sessiongw_c_native_fetch_column_scale(const sessiongw_c_native_fetch* fetch, size_t column)
{ return fetch == nullptr || column >= fetch->columns.size() ? 0 : fetch->columns[column].scale; }
const uint8_t* sessiongw_c_native_fetch_column_nulls(const sessiongw_c_native_fetch* fetch,
                                                      size_t column, size_t* size)
{
    if (fetch == nullptr || column >= fetch->columns.size()) { if (size != nullptr) *size = 0; return nullptr; }
    const auto& view = fetch->columns[column];
    if (size != nullptr) *size = view.null_size;
    return view.null_size == 0U ? nullptr : fetch->value.native_rows.data() + view.null_offset;
}
const uint8_t* sessiongw_c_native_fetch_column_fixed_data(const sessiongw_c_native_fetch* fetch,
                                                          size_t column, size_t* size)
{
    if (fetch == nullptr || column >= fetch->columns.size() || fetch->columns[column].kind == 7U)
    { if (size != nullptr) *size = 0; return nullptr; }
    const auto& view = fetch->columns[column];
    if (size != nullptr) *size = view.data_size;
    return view.data_size == 0U ? nullptr : fetch->value.native_rows.data() + view.data_offset;
}
const uint8_t* sessiongw_c_native_fetch_column_sizes(const sessiongw_c_native_fetch* fetch,
                                                     size_t column, size_t* size)
{
    if (fetch == nullptr || column >= fetch->columns.size() || fetch->columns[column].kind != 7U)
    { if (size != nullptr) *size = 0; return nullptr; }
    const auto& view = fetch->columns[column];
    if (size != nullptr) *size = view.sizes_size;
    return view.sizes_size == 0U ? nullptr : fetch->value.native_rows.data() + view.sizes_offset;
}
const uint8_t* sessiongw_c_native_fetch_column_variable_data(const sessiongw_c_native_fetch* fetch,
                                                             size_t column, size_t* size)
{
    if (fetch == nullptr || column >= fetch->columns.size() || fetch->columns[column].kind != 7U)
    { if (size != nullptr) *size = 0; return nullptr; }
    const auto& view = fetch->columns[column];
    if (size != nullptr) *size = view.data_size;
    return view.data_size == 0U ? nullptr : fetch->value.native_rows.data() + view.data_offset;
}
const uint64_t* sessiongw_c_native_fetch_row_locations(const sessiongw_c_native_fetch* fetch,
                                                       size_t* count)
{
    if (count != nullptr) *count = fetch == nullptr ? 0U : fetch->locations.size();
    return fetch == nullptr || fetch->locations.empty() ? nullptr : fetch->locations.data();
}

int sessiongw_c_close_cursor(sessiongw_c_session* session, sessiongw_c_cursor* cursor)
{ return protect([&] { require(session != nullptr && cursor != nullptr, "SessionGW cursor arguments are required"); session->value.closeCursor(cursor->value); }); }

int sessiongw_c_open_table_insert(sessiongw_c_session* session, const char* schema, const char* table,
                                  const char* const* columns, size_t count, uint32_t max_rows,
                                  const uint8_t* schema_ipc, size_t schema_size, sessiongw_c_operation** out)
{
    return protect([&] {
        require(session != nullptr && out != nullptr, "SessionGW operation arguments are required");
        const auto names = columnNames(columns, count);
        const auto projected = projectSchema(decodeSchema(schema_ipc, schema_size), names);
        *out = new sessiongw_c_operation{
            session->value.openTableInsert(tableName(schema, table), names, max_rows, projected)};
    });
}
int sessiongw_c_open_table_update(sessiongw_c_session* session, const char* schema, const char* table,
                                  const char* const* columns, size_t count, uint32_t max_rows,
                                  const uint8_t* schema_ipc, size_t schema_size, sessiongw_c_operation** out)
{
    return protect([&] {
        require(session != nullptr && out != nullptr, "SessionGW operation arguments are required");
        const auto names = columnNames(columns, count);
        const auto projected = projectSchema(decodeSchema(schema_ipc, schema_size), names);
        *out = new sessiongw_c_operation{
            session->value.openTableUpdate(tableName(schema, table), names, max_rows, projected)};
    });
}
int sessiongw_c_open_table_delete(sessiongw_c_session* session, const char* schema, const char* table,
                                  uint32_t max_rows, sessiongw_c_operation** out)
{ return protect([&] { require(session != nullptr && out != nullptr, "SessionGW operation arguments are required"); *out = new sessiongw_c_operation{session->value.openTableDelete(tableName(schema, table), max_rows)}; }); }
void sessiongw_c_operation_destroy(sessiongw_c_operation* operation) { delete operation; }
uint64_t sessiongw_c_operation_id(const sessiongw_c_operation* operation) { return operation == nullptr ? 0U : operation->value.id; }
const uint8_t* sessiongw_c_operation_schema_ipc(const sessiongw_c_operation* operation, size_t* size)
{ if (operation == nullptr) { if (size != nullptr) *size = 0; return nullptr; } return bytes(operation->value.accepted_schema_ipc, size); }
int sessiongw_c_insert_rows(sessiongw_c_session* session, const sessiongw_c_operation* operation,
                            const uint8_t* batch, size_t size, uint64_t* affected)
{ return protect([&] { require(session != nullptr && operation != nullptr && affected != nullptr && (size == 0U || batch != nullptr), "SessionGW insert arguments are required"); *affected = session->value.insertRows(operation->value, {batch, size}); }); }
int sessiongw_c_update_rows(sessiongw_c_session* session, const sessiongw_c_operation* operation,
                            const uint64_t* rows, size_t count, const uint8_t* batch, size_t size,
                            uint64_t* affected)
{ return protect([&] { require(session != nullptr && operation != nullptr && affected != nullptr && (size == 0U || batch != nullptr), "SessionGW update arguments are required"); const auto requested = locations(rows, count); *affected = session->value.updateRows(operation->value, requested, {batch, size}); }); }
int sessiongw_c_delete_rows(sessiongw_c_session* session, const sessiongw_c_operation* operation,
                            const uint64_t* rows, size_t count, uint64_t* affected)
{ return protect([&] { require(session != nullptr && operation != nullptr && affected != nullptr, "SessionGW delete arguments are required"); const auto requested = locations(rows, count); *affected = session->value.deleteRows(operation->value, requested); }); }
int sessiongw_c_close_operation(sessiongw_c_session* session, sessiongw_c_operation* operation)
{ return protect([&] { require(session != nullptr && operation != nullptr, "SessionGW operation arguments are required"); session->value.closeOperation(operation->value); }); }

int sessiongw_c_set_autocommit(sessiongw_c_session* session, int enabled)
{ return protect([&] { require(session != nullptr, "SessionGW session is required"); session->value.setAutocommit(enabled != 0); }); }
int sessiongw_c_commit(sessiongw_c_session* session)
{ return protect([&] { require(session != nullptr, "SessionGW session is required"); session->value.commit(); }); }
int sessiongw_c_rollback(sessiongw_c_session* session)
{ return protect([&] { require(session != nullptr, "SessionGW session is required"); session->value.rollback(); }); }

} // extern "C"
