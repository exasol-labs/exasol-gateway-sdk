#include <sessiongw/client.hpp>
#include <sessiongw/capabilities.hpp>
#include <sessiongw/errors.hpp>
#include <sessiongw/native_write_batch.hpp>
#include <sessiongw/websocket.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace
{

void printUsage(const char* argv0)
{
    std::cout << "Usage: " << argv0 << " <command>\n\n"
              << "Commands:\n"
              << "  help                 Show this help text\n"
              << "  capabilities-offline Print SDK-advertised client capabilities\n"
              << "  ping --host HOST --port PORT --user USER --password PASSWORD [--skip-tls-verify|--ca-file FILE|--plain]\n"
              << "                       Authenticate via WebSocket API v5, enter SessionGW, Ping, Close\n"
              << "  describe --host HOST --port PORT --user USER --password PASSWORD --schema S --table T [TLS OPTIONS]\n"
              << "                       Describe a table through SessionGW metadata frames\n"
              << "  query --host HOST --port PORT --user USER --password PASSWORD --sql SQL [--max-rows N] [--max-bytes N] [QUERY ASSERTIONS] [TLS OPTIONS]\n"
              << "                       Open a pushed-query cursor, fetch one Arrow IPC batch, close\n"
              << "  scan --host HOST --port PORT --user USER --password PASSWORD --schema S --table T --column C [--column C ...] [--include-row-handles] [--max-rows N] [--max-bytes N] [QUERY ASSERTIONS] [TLS OPTIONS]\n"
              << "                       Open a table-scan cursor, fetch one Arrow IPC batch, optionally print row handles, close\n"
              << "  insert --host HOST --port PORT --user USER --password PASSWORD --schema S --table T --int32-column C|--int64-column C|--string-column C --row V[,V...] [--row V[,V...]] [--rows-per-insert-batch N] [--autocommit true|false] [--commit|--rollback] [TLS OPTIONS]\n"
              << "                       Open a table insert operation, send native columnar DMP-shaped rows, close, optionally commit/rollback\n"
              << "  update --host HOST --port PORT --user USER --password PASSWORD --schema S --table T --int32-column C|--int64-column C|--string-column C --row V[,V...] --row-handle NODE:ROW [TLS OPTIONS]\n"
              << "                       Open a table update operation and update rows identified by scan row handles\n"
              << "  delete --host HOST --port PORT --user USER --password PASSWORD --schema S --table T --row-handle NODE:ROW [--row-handle NODE:ROW ...] [TLS OPTIONS]\n"
              << "                       Open a table delete operation and delete rows identified by scan row handles\n"
              << "\nTLS OPTIONS:\n"
              << "  --tls                 Use WSS/TLS with certificate verification (default)\n"
              << "  --skip-tls-verify     Use WSS/TLS but skip certificate verification for local nano/self-signed tests\n"
              << "  --ca-file FILE        Use WSS/TLS and verify with the given CA/certificate file\n"
              << "  --plain               Use plain ws:// for explicitly unsecured local tests only\n"
              << "\nQUERY ASSERTIONS:\n"
              << "  --expect-end-of-cursor true|false\n"
              << "  --expect-arrow-batch true|false    Assert whether the fetch returned non-empty Arrow batch bytes\n"
              << "  --skip-close-cursor-for-cleanup-test\n"
              << "                         Leave the cursor open and rely on SessionGW Close cleanup\n";
}

struct DescribeOptions
{
    sessiongw::WebSocketOptions websocket;
    std::string schema;
    std::string table;
};

struct QueryOptions
{
    sessiongw::WebSocketOptions websocket;
    std::string sql;
    std::uint32_t maxRows = 100;
    std::uint32_t maxBytes = 0;
    std::optional<bool> expectEndOfCursor;
    std::optional<bool> expectArrowBatch;
    bool skipCloseCursorForCleanupTest = false;
};

struct ScanOptions
{
    sessiongw::WebSocketOptions websocket;
    std::string schema;
    std::string table;
    std::vector<std::string> columns;
    std::uint32_t maxRows = 100;
    std::uint32_t maxBytes = 0;
    std::optional<bool> expectEndOfCursor;
    std::optional<bool> expectArrowBatch;
    bool includeRowHandles = false;
};

enum class InsertColumnType
{
    int32,
    int64,
    string
};

struct InsertColumn
{
    std::string name;
    InsertColumnType type = InsertColumnType::string;
};

struct InsertOptions
{
    sessiongw::WebSocketOptions websocket;
    std::string schema;
    std::string table;
    std::vector<InsertColumn> columns;
    std::vector<std::vector<std::string>> rows;
    std::optional<bool> autocommit;
    bool commit = false;
    bool rollback = false;
    std::size_t rowsPerInsertBatch = 0;
};

struct RowHandle
{
    std::uint64_t rowNumber = 0;
};

struct DeleteOptions
{
    sessiongw::WebSocketOptions websocket;
    std::string schema;
    std::string table;
    std::vector<RowHandle> rowHandles;
    std::optional<bool> autocommit;
    bool commit = false;
    bool rollback = false;
};

struct UpdateOptions
{
    InsertOptions write;
    std::vector<RowHandle> rowHandles;
};

void appendU16(std::vector<std::uint8_t>& out, const std::uint16_t value)
{
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
    out.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

void appendU32(std::vector<std::uint8_t>& out, const std::uint32_t value)
{
    out.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xffU));
    out.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xffU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
    out.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

void appendU64(std::vector<std::uint8_t>& out, const std::uint64_t value)
{
    for (std::size_t index = 0; index < 8U; ++index)
    {
        const unsigned shift = static_cast<unsigned>(56U - (index * 8U));
        out.push_back(static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

std::uint16_t readU16(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    if (offset + 2U > bytes.size())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Truncated SessionGW metadata payload");
    }
    const std::uint16_t value = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(bytes[offset]) << 8U) |
        static_cast<std::uint16_t>(bytes[offset + 1U]));
    offset += 2U;
    return value;
}

std::uint32_t readU32(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    if (offset + 4U > bytes.size())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Truncated SessionGW metadata payload");
    }
    const std::uint32_t value = (static_cast<std::uint32_t>(bytes[offset]) << 24U) |
                                (static_cast<std::uint32_t>(bytes[offset + 1U]) << 16U) |
                                (static_cast<std::uint32_t>(bytes[offset + 2U]) << 8U) |
                                static_cast<std::uint32_t>(bytes[offset + 3U]);
    offset += 4U;
    return value;
}

std::uint64_t readU64(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    if (offset + 8U > bytes.size())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Truncated SessionGW cursor payload");
    }
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < 8U; ++index)
    {
        value = (value << 8U) | static_cast<std::uint64_t>(bytes[offset + index]);
    }
    offset += 8U;
    return value;
}

void appendString(std::vector<std::uint8_t>& out, const std::string& value)
{
    appendU16(out, static_cast<std::uint16_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

void appendLongString(std::vector<std::uint8_t>& out, const std::string& value)
{
    appendU32(out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

std::string readString(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    const std::uint16_t size = readU16(bytes, offset);
    if (offset + size > bytes.size())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Truncated SessionGW metadata string");
    }
    std::string value(reinterpret_cast<const char*>(bytes.data() + offset), size);
    offset += size;
    return value;
}

bool parseBool(const std::string& value, const char* optionName)
{
    if (value == "true")
    {
        return true;
    }
    if (value == "false")
    {
        return false;
    }
    throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                           std::string(optionName) + " requires true or false");
}

std::vector<std::uint8_t> encodeTableNamePayload(const std::string& schema,
                                                 const std::string& table)
{
    std::vector<std::uint8_t> payload;
    appendString(payload, schema);
    appendString(payload, table);
    return payload;
}

std::vector<std::uint8_t> encodeOpenPushedQueryPayload(const std::string& sql)
{
    std::vector<std::uint8_t> payload;
    appendLongString(payload, sql);
    return payload;
}

std::vector<std::uint8_t> encodeOpenTableScanPayload(const ScanOptions& options)
{
    std::vector<std::uint8_t> payload;
    appendLongString(payload, options.schema);
    appendLongString(payload, options.table);
    appendU32(payload, static_cast<std::uint32_t>(options.columns.size()));
    for (const std::string& column : options.columns)
    {
        appendLongString(payload, column);
    }
    payload.push_back(static_cast<std::uint8_t>(options.includeRowHandles ? 1U : 0U));
    return payload;
}

/// Encodes the fixed next-batch payload; positioned reads have a dedicated message.
std::vector<std::uint8_t> encodeFetchPayload(const std::uint64_t cursorId,
                                             const std::uint32_t maxRows,
                                             const std::uint32_t maxBytes)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursorId);
    appendU32(payload, maxRows);
    appendU32(payload, maxBytes);
    return payload;
}

void appendBytes(std::vector<std::uint8_t>& out, const std::shared_ptr<arrow::Buffer>& buffer)
{
    appendU32(out, static_cast<std::uint32_t>(buffer->size()));
    out.insert(out.end(), buffer->data(), buffer->data() + buffer->size());
}

std::vector<std::uint8_t> encodeCloseCursorPayload(const std::uint64_t cursorId)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursorId);
    return payload;
}

std::shared_ptr<arrow::Buffer> serializeSchema(const std::shared_ptr<arrow::Schema>& schema)
{
    auto result = arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool());
    if (!result.ok())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::internal_error,
                               "Arrow schema serialization failed: " + result.status().ToString());
    }
    return result.ValueOrDie();
}

std::shared_ptr<arrow::Schema> makeInsertSchema(const InsertOptions& options)
{
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(options.columns.size());
    for (const InsertColumn& column : options.columns)
    {
        std::shared_ptr<arrow::DataType> arrowType = arrow::utf8();
        if (column.type == InsertColumnType::int32)
        {
            arrowType = arrow::int32();
        }
        else if (column.type == InsertColumnType::int64)
        {
            arrowType = arrow::int64();
        }
        fields.push_back(arrow::field(column.name, arrowType, true));
    }
    return arrow::schema(std::move(fields));
}

void patchU32(std::vector<std::uint8_t>& out, const std::size_t offset, const std::uint32_t value)
{
    out.at(offset) = static_cast<std::uint8_t>((value >> 24U) & 0xffU);
    out.at(offset + 1U) = static_cast<std::uint8_t>((value >> 16U) & 0xffU);
    out.at(offset + 2U) = static_cast<std::uint8_t>((value >> 8U) & 0xffU);
    out.at(offset + 3U) = static_cast<std::uint8_t>(value & 0xffU);
}

void appendNativeWriteColumn(sessiongw::NativeWriteBatchBuilder& builder,
                             const InsertOptions& options,
                             const std::size_t column,
                             const std::size_t beginRow,
                             const std::size_t endRow,
                             std::vector<std::uint8_t>& nulls,
                             std::vector<std::uint8_t>& fixedColumnData,
                             std::vector<std::size_t>& variableSizes,
                             std::vector<std::uint8_t>& variableData)
{
    const std::size_t rowCount = endRow - beginRow;
    nulls.assign(rowCount, sessiongw::native_write_not_null);

    if (options.columns[column].type == InsertColumnType::int32)
    {
        fixedColumnData.clear();
        fixedColumnData.reserve(rowCount * sizeof(std::int32_t));
        for (std::size_t rowIndex = beginRow; rowIndex < endRow; ++rowIndex)
        {
            const std::int32_t value = static_cast<std::int32_t>(std::stol(options.rows[rowIndex][column]));
            sessiongw::appendNativeWriteBytes(fixedColumnData, &value, sizeof(value));
        }
        builder.appendFixedColumn(nulls, fixedColumnData);
        return;
    }

    if (options.columns[column].type == InsertColumnType::int64)
    {
        fixedColumnData.clear();
        fixedColumnData.reserve(rowCount * sizeof(std::int64_t));
        for (std::size_t rowIndex = beginRow; rowIndex < endRow; ++rowIndex)
        {
            const std::int64_t value = std::stoll(options.rows[rowIndex][column]);
            sessiongw::appendNativeWriteBytes(fixedColumnData, &value, sizeof(value));
        }
        builder.appendFixedColumn(nulls, fixedColumnData);
        return;
    }

    variableSizes.clear();
    variableData.clear();
    variableSizes.reserve(rowCount);
    std::size_t dataBytes = 0;
    for (std::size_t rowIndex = beginRow; rowIndex < endRow; ++rowIndex)
    {
        variableSizes.push_back(options.rows[rowIndex][column].size());
        dataBytes += options.rows[rowIndex][column].size();
    }
    variableData.reserve(dataBytes);
    for (std::size_t rowIndex = beginRow; rowIndex < endRow; ++rowIndex)
    {
        const std::string& value = options.rows[rowIndex][column];
        variableData.insert(variableData.end(), value.begin(), value.end());
    }
    builder.appendVariableColumn(nulls, variableSizes, variableData);
}

std::vector<std::uint8_t> encodeOpenTableInsertPayload(const InsertOptions& options,
                                                       const std::shared_ptr<arrow::Schema>& schema)
{
    std::vector<std::uint8_t> payload;
    appendLongString(payload, options.schema);
    appendLongString(payload, options.table);
    appendU32(payload, static_cast<std::uint32_t>(options.columns.size()));
    for (const InsertColumn& column : options.columns)
    {
        appendLongString(payload, column.name);
    }
    appendU32(payload, static_cast<std::uint32_t>(std::max<std::size_t>(1U, options.rows.size())));
    appendBytes(payload, serializeSchema(schema));
    return payload;
}

void encodeInsertRowsPayloadInto(std::vector<std::uint8_t>& payload,
                                 const std::uint64_t operationId,
                                 const InsertOptions& options,
                                 const std::size_t beginRow,
                                 const std::size_t endRow,
                                 std::vector<std::uint8_t>& nulls,
                                 std::vector<std::uint8_t>& fixedColumnData,
                                 std::vector<std::size_t>& variableSizes,
                                 std::vector<std::uint8_t>& variableData)
{
    payload.clear();
    appendU64(payload, operationId);
    const std::size_t nativeBatchSizeOffset = payload.size();
    appendU32(payload, 0);
    sessiongw::alignNativeWriteBatch(payload);

    sessiongw::NativeWriteBatchBuilder builder(payload);
    builder.begin(static_cast<std::uint32_t>(endRow - beginRow),
                  static_cast<std::uint32_t>(options.columns.size()),
                  false);
    for (std::size_t column = 0; column < options.columns.size(); ++column)
    {
        appendNativeWriteColumn(builder,
                                options,
                                column,
                                beginRow,
                                endRow,
                                nulls,
                                fixedColumnData,
                                variableSizes,
                                variableData);
    }
    builder.finish();
    patchU32(payload, nativeBatchSizeOffset, static_cast<std::uint32_t>(builder.batchSize()));
}

void appendRowHandles(std::vector<std::uint8_t>& payload, const std::vector<RowHandle>& rowHandles);
std::vector<RowHandle> readRowHandles(const std::vector<std::uint8_t>& payload, std::size_t& offset);

std::vector<std::uint8_t> encodeCloseOperationPayload(const std::uint64_t operationId)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, operationId);
    return payload;
}

std::vector<std::uint8_t> encodeOpenTableDeletePayload(const DeleteOptions& options)
{
    std::vector<std::uint8_t> payload;
    appendLongString(payload, options.schema);
    appendLongString(payload, options.table);
    appendU32(payload, static_cast<std::uint32_t>(std::max<std::size_t>(1U, options.rowHandles.size())));
    return payload;
}

std::vector<std::uint8_t> encodeDeleteRowsPayload(const std::uint64_t operationId,
                                                  const std::vector<RowHandle>& rowHandles)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, operationId);
    appendRowHandles(payload, rowHandles);
    return payload;
}

std::vector<std::uint8_t> encodeOpenTableUpdatePayload(const UpdateOptions& options,
                                                       const std::shared_ptr<arrow::Schema>& schema)
{
    return encodeOpenTableInsertPayload(options.write, schema);
}

void encodeUpdateRowsPayloadInto(std::vector<std::uint8_t>& payload,
                                 const std::uint64_t operationId,
                                 const UpdateOptions& options,
                                 std::vector<std::uint8_t>& nulls,
                                 std::vector<std::uint8_t>& fixedColumnData,
                                 std::vector<std::size_t>& variableSizes,
                                 std::vector<std::uint8_t>& variableData)
{
    payload.clear();
    appendU64(payload, operationId);
    appendRowHandles(payload, options.rowHandles);
    const std::size_t nativeBatchSizeOffset = payload.size();
    appendU32(payload, 0);
    sessiongw::alignNativeWriteBatch(payload);

    sessiongw::NativeWriteBatchBuilder builder(payload);
    builder.begin(static_cast<std::uint32_t>(options.write.rows.size()),
                  static_cast<std::uint32_t>(options.write.columns.size()),
                  false);
    for (std::size_t column = 0; column < options.write.columns.size(); ++column)
    {
        appendNativeWriteColumn(builder,
                                options.write,
                                column,
                                0,
                                options.write.rows.size(),
                                nulls,
                                fixedColumnData,
                                variableSizes,
                                variableData);
    }
    builder.finish();
    patchU32(payload, nativeBatchSizeOffset, static_cast<std::uint32_t>(builder.batchSize()));
}

std::vector<std::uint8_t> encodeSetAutocommitPayload(const bool enabled)
{
    return {static_cast<std::uint8_t>(enabled ? 1U : 0U)};
}

RowHandle parseRowHandle(const std::string& value)
{
    return {static_cast<std::uint64_t>(std::stoull(value))};
}

void appendRowHandles(std::vector<std::uint8_t>& payload, const std::vector<RowHandle>& rowHandles)
{
    appendU32(payload, static_cast<std::uint32_t>(rowHandles.size()));
    for (const RowHandle& rowHandle : rowHandles)
    {
        appendU64(payload, rowHandle.rowNumber);
    }
}

std::vector<RowHandle> readRowHandles(const std::vector<std::uint8_t>& payload, std::size_t& offset)
{
    std::vector<RowHandle> rowHandles;
    const std::uint32_t rowHandleCount = readU32(payload, offset);
    rowHandles.reserve(rowHandleCount);
    for (std::uint32_t index = 0; index < rowHandleCount; ++index)
    {
        rowHandles.push_back({readU64(payload, offset)});
    }
    return rowHandles;
}

std::vector<std::string> splitCsvRow(const std::string& value)
{
    std::vector<std::string> result;
    std::string current;
    for (const char character : value)
    {
        if (character == ',')
        {
            result.push_back(current);
            current.clear();
        }
        else
        {
            current.push_back(character);
        }
    }
    result.push_back(current);
    return result;
}

sessiongw::ErrorCategory decodeErrorCategory(const std::uint16_t category)
{
    switch (category)
    {
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::protocol_error):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::authentication_failed):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::not_authorized):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::object_not_found):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::unsupported_type):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::unsupported_operation):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::transaction_conflict):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::constraint_violation):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::resource_limit):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::cursor_not_found):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::internal_error):
        case static_cast<std::uint16_t>(sessiongw::ErrorCategory::transport_error):
            return static_cast<sessiongw::ErrorCategory>(category);
        default:
            return sessiongw::ErrorCategory::protocol_error;
    }
}

void throwIfErrorFrame(const sessiongw::Frame& frame)
{
    if (frame.header.message_type != sessiongw::MessageType::error)
    {
        return;
    }
    std::size_t offset = 0;
    const std::uint16_t category = readU16(frame.payload, offset);
    const std::string message = readString(frame.payload, offset);
    throw sessiongw::Error(decodeErrorCategory(category), message);
}

template <typename RequireValue>
bool parseCommonWebSocketOption(const std::string_view arg,
                                RequireValue requireValue,
                                sessiongw::WebSocketOptions& options)
{
    if (arg == "--host")
    {
        options.host = requireValue("--host");
    }
    else if (arg == "--port")
    {
        options.port = static_cast<std::uint16_t>(std::stoul(requireValue("--port")));
    }
    else if (arg == "--user")
    {
        options.user = requireValue("--user");
    }
    else if (arg == "--password")
    {
        options.password = requireValue("--password");
    }
    else if (arg == "--tls")
    {
        options.tls_mode = sessiongw::WebSocketTlsMode::tls_verify;
    }
    else if (arg == "--skip-tls-verify")
    {
        options.tls_mode = sessiongw::WebSocketTlsMode::tls_skip_verify_for_test_only;
    }
    else if (arg == "--ca-file")
    {
        options.ca_file = requireValue("--ca-file");
        options.tls_mode = sessiongw::WebSocketTlsMode::tls_verify;
    }
    else if (arg == "--plain")
    {
        options.tls_mode = sessiongw::WebSocketTlsMode::plain_for_test_only;
    }
    else
    {
        return false;
    }
    return true;
}

sessiongw::WebSocketOptions parseWebSocketOptions(const int argc, char** argv)
{
    sessiongw::WebSocketOptions options;
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };

        if (!parseCommonWebSocketOption(arg, requireValue, options))
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   std::string("Unknown WebSocket option: ") + std::string(arg));
        }
    }
    return options;
}

DescribeOptions parseDescribeOptions(const int argc, char** argv)
{
    DescribeOptions options;
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };

        if (parseCommonWebSocketOption(arg, requireValue, options.websocket))
        {
            continue;
        }
        if (arg == "--schema")
        {
            options.schema = requireValue("--schema");
        }
        else if (arg == "--table")
        {
            options.table = requireValue("--table");
        }
        else
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   std::string("Unknown describe option: ") + std::string(arg));
        }
    }
    if (options.schema.empty() || options.table.empty())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "describe requires --schema and --table");
    }
    return options;
}

QueryOptions parseQueryOptions(const int argc, char** argv)
{
    QueryOptions options;
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };

        if (parseCommonWebSocketOption(arg, requireValue, options.websocket))
        {
            continue;
        }
        if (arg == "--sql")
        {
            options.sql = requireValue("--sql");
        }
        else if (arg == "--max-rows")
        {
            options.maxRows = static_cast<std::uint32_t>(std::stoul(requireValue("--max-rows")));
        }
        else if (arg == "--max-bytes")
        {
            options.maxBytes = static_cast<std::uint32_t>(std::stoul(requireValue("--max-bytes")));
        }
        else if (arg == "--expect-end-of-cursor")
        {
            options.expectEndOfCursor = parseBool(requireValue("--expect-end-of-cursor"),
                                                  "--expect-end-of-cursor");
        }
        else if (arg == "--expect-arrow-batch")
        {
            options.expectArrowBatch = parseBool(requireValue("--expect-arrow-batch"),
                                                 "--expect-arrow-batch");
        }
        else if (arg == "--skip-close-cursor-for-cleanup-test")
        {
            options.skipCloseCursorForCleanupTest = true;
        }
        else
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   std::string("Unknown query option: ") + std::string(arg));
        }
    }
    if (options.sql.empty())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "query requires --sql");
    }
    return options;
}

ScanOptions parseScanOptions(const int argc, char** argv)
{
    ScanOptions options;
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };

        if (parseCommonWebSocketOption(arg, requireValue, options.websocket))
        {
            continue;
        }
        if (arg == "--schema")
        {
            options.schema = requireValue("--schema");
        }
        else if (arg == "--table")
        {
            options.table = requireValue("--table");
        }
        else if (arg == "--column")
        {
            options.columns.push_back(requireValue("--column"));
        }
        else if (arg == "--max-rows")
        {
            options.maxRows = static_cast<std::uint32_t>(std::stoul(requireValue("--max-rows")));
        }
        else if (arg == "--max-bytes")
        {
            options.maxBytes = static_cast<std::uint32_t>(std::stoul(requireValue("--max-bytes")));
        }
        else if (arg == "--expect-end-of-cursor")
        {
            options.expectEndOfCursor = parseBool(requireValue("--expect-end-of-cursor"),
                                                  "--expect-end-of-cursor");
        }
        else if (arg == "--expect-arrow-batch")
        {
            options.expectArrowBatch = parseBool(requireValue("--expect-arrow-batch"),
                                                 "--expect-arrow-batch");
        }
        else if (arg == "--include-row-handles")
        {
            options.includeRowHandles = true;
        }
        else
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   std::string("Unknown scan option: ") + std::string(arg));
        }
    }
    if (options.schema.empty() || options.table.empty() || options.columns.empty())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "scan requires --schema, --table, and at least one --column");
    }
    return options;
}

template <typename Options>
void parseTransactionOption(const std::string_view arg,
                            const auto& requireValue,
                            Options& options,
                            bool& handled)
{
    handled = true;
    if (arg == "--autocommit")
    {
        options.autocommit = parseBool(requireValue("--autocommit"), "--autocommit");
    }
    else if (arg == "--commit")
    {
        options.commit = true;
    }
    else if (arg == "--rollback")
    {
        options.rollback = true;
    }
    else
    {
        handled = false;
    }
}

InsertOptions parseInsertOptions(const int argc, char** argv)
{
    InsertOptions options;
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };

        if (parseCommonWebSocketOption(arg, requireValue, options.websocket))
        {
            continue;
        }
        if (arg == "--schema")
        {
            options.schema = requireValue("--schema");
        }
        else if (arg == "--table")
        {
            options.table = requireValue("--table");
        }
        else if (arg == "--int32-column")
        {
            options.columns.push_back({requireValue("--int32-column"), InsertColumnType::int32});
        }
        else if (arg == "--int64-column")
        {
            options.columns.push_back({requireValue("--int64-column"), InsertColumnType::int64});
        }
        else if (arg == "--string-column")
        {
            options.columns.push_back({requireValue("--string-column"), InsertColumnType::string});
        }
        else if (arg == "--row")
        {
            options.rows.push_back(splitCsvRow(requireValue("--row")));
        }
        else if (arg == "--row-handle")
        {
            (void)requireValue("--row-handle");
        }
        else if (arg == "--rows-per-insert-batch")
        {
            options.rowsPerInsertBatch = static_cast<std::size_t>(std::stoull(requireValue("--rows-per-insert-batch")));
        }
        else if (arg == "--autocommit")
        {
            options.autocommit = parseBool(requireValue("--autocommit"), "--autocommit");
        }
        else if (arg == "--commit")
        {
            options.commit = true;
        }
        else if (arg == "--rollback")
        {
            options.rollback = true;
        }
        else
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   std::string("Unknown insert option: ") + std::string(arg));
        }
    }
    if (options.schema.empty() || options.table.empty() || options.columns.empty() || options.rows.empty())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "insert requires --schema, --table, at least one column, and at least one --row");
    }
    if (options.commit && options.rollback)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "insert cannot use both --commit and --rollback");
    }
    if (options.rowsPerInsertBatch == 0)
    {
        options.rowsPerInsertBatch = options.rows.size();
    }
    for (const auto& row : options.rows)
    {
        if (row.size() != options.columns.size())
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   "insert --row value count must match column count");
        }
    }
    return options;
}

DeleteOptions parseDeleteOptions(const int argc, char** argv)
{
    DeleteOptions options;
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };

        if (parseCommonWebSocketOption(arg, requireValue, options.websocket))
        {
            continue;
        }
        bool handled = false;
        parseTransactionOption(arg, requireValue, options, handled);
        if (handled)
        {
            continue;
        }
        if (arg == "--schema")
        {
            options.schema = requireValue("--schema");
        }
        else if (arg == "--table")
        {
            options.table = requireValue("--table");
        }
        else if (arg == "--row-handle")
        {
            options.rowHandles.push_back(parseRowHandle(requireValue("--row-handle")));
        }
        else
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   std::string("Unknown delete option: ") + std::string(arg));
        }
    }
    if (options.schema.empty() || options.table.empty() || options.rowHandles.empty())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "delete requires --schema, --table, and at least one --row-handle");
    }
    if (options.commit && options.rollback)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "delete cannot use both --commit and --rollback");
    }
    return options;
}

UpdateOptions parseUpdateOptions(const int argc, char** argv)
{
    UpdateOptions options;
    options.write = parseInsertOptions(argc, argv);
    for (int index = 2; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        auto requireValue = [&](const char* name) -> std::string {
            if (++index >= argc)
            {
                throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                       std::string("Missing value for ") + name);
            }
            return argv[index];
        };
        if (parseCommonWebSocketOption(arg, requireValue, options.write.websocket))
        {
            continue;
        }
        if (arg == "--row-handle")
        {
            options.rowHandles.push_back(parseRowHandle(requireValue("--row-handle")));
        }
        else if (arg.starts_with("--"))
        {
            (void)requireValue;
        }
    }
    if (options.rowHandles.empty())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "update requires at least one --row-handle");
    }
    if (options.rowHandles.size() != options.write.rows.size())
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "update row handle count must match --row count");
    }
    return options;
}

void enterAndHello(sessiongw::WebSocketConnection& connection)
{
    connection.enterSessionGateway();

    sessiongw::Frame hello;
    hello.header.message_type = sessiongw::MessageType::hello;
    hello.header.request_id = 1;
    connection.sendFrame(hello);
    const sessiongw::Frame helloOk = connection.receiveFrame();
    if (helloOk.header.message_type != sessiongw::MessageType::hello_ok)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW HelloOk");
    }
}

void closeSessionGateway(sessiongw::WebSocketConnection& connection, const std::uint64_t requestId)
{
    sessiongw::Frame close;
    close.header.message_type = sessiongw::MessageType::close;
    close.header.request_id = requestId;
    connection.sendFrame(close);
    const sessiongw::Frame ok = connection.receiveFrame();
    if (ok.header.message_type != sessiongw::MessageType::ok)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW Close Ok");
    }
}

int pingServer(const int argc, char** argv)
{
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(parseWebSocketOptions(argc, argv));
    enterAndHello(connection);

    sessiongw::Frame ping;
    ping.header.message_type = sessiongw::MessageType::ping;
    ping.header.request_id = 2;
    connection.sendFrame(ping);
    const sessiongw::Frame pong = connection.receiveFrame();
    if (pong.header.message_type != sessiongw::MessageType::pong)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW Pong");
    }

    closeSessionGateway(connection, 3);

    std::cout << "SessionGW ping over WebSocket succeeded\n";
    return 0;
}

int describeTable(const int argc, char** argv)
{
    const DescribeOptions options = parseDescribeOptions(argc, argv);
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
    enterAndHello(connection);

    sessiongw::Frame describe;
    describe.header.message_type = sessiongw::MessageType::describe_table;
    describe.header.request_id = 2;
    describe.payload = encodeTableNamePayload(options.schema, options.table);
    connection.sendFrame(describe);
    const sessiongw::Frame describeResult = connection.receiveFrame();
    throwIfErrorFrame(describeResult);
    if (describeResult.header.message_type != sessiongw::MessageType::describe_table_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW DescribeTableResult");
    }

    std::size_t describeOffset = 0;
    const std::string schemaName = readString(describeResult.payload, describeOffset);
    const std::string tableName = readString(describeResult.payload, describeOffset);
    const std::string tableVersion = readString(describeResult.payload, describeOffset);
    const std::uint32_t arrowSchemaBytes = readU32(describeResult.payload, describeOffset);

    sessiongw::Frame version;
    version.header.message_type = sessiongw::MessageType::get_table_version;
    version.header.request_id = 3;
    version.payload = encodeTableNamePayload(options.schema, options.table);
    connection.sendFrame(version);
    const sessiongw::Frame versionResult = connection.receiveFrame();
    throwIfErrorFrame(versionResult);
    if (versionResult.header.message_type != sessiongw::MessageType::get_table_version_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW GetTableVersionResult");
    }

    std::size_t versionOffset = 0;
    (void)readString(versionResult.payload, versionOffset);
    (void)readString(versionResult.payload, versionOffset);
    const std::string versionString = readString(versionResult.payload, versionOffset);

    closeSessionGateway(connection, 4);

    std::cout << "SessionGW describe succeeded: " << schemaName << '.' << tableName
              << " version=" << tableVersion
              << " getTableVersion=" << versionString
              << " arrow_schema_bytes=" << arrowSchemaBytes << '\n';
    return 0;
}

int queryServer(const int argc, char** argv)
{
    const QueryOptions options = parseQueryOptions(argc, argv);
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
    enterAndHello(connection);

    sessiongw::Frame open;
    open.header.message_type = sessiongw::MessageType::open_pushed_query;
    open.header.request_id = 2;
    open.payload = encodeOpenPushedQueryPayload(options.sql);
    connection.sendFrame(open);
    const sessiongw::Frame openResult = connection.receiveFrame();
    throwIfErrorFrame(openResult);
    if (openResult.header.message_type != sessiongw::MessageType::open_cursor_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW OpenCursorResult");
    }

    std::size_t openOffset = 0;
    const std::uint64_t cursorId = readU64(openResult.payload, openOffset);
    const std::uint32_t arrowSchemaBytes = readU32(openResult.payload, openOffset);

    sessiongw::Frame fetch;
    fetch.header.message_type = sessiongw::MessageType::fetch;
    fetch.header.request_id = 3;
    fetch.payload = encodeFetchPayload(cursorId, options.maxRows, options.maxBytes);
    connection.sendFrame(fetch);
    const sessiongw::Frame fetchResult = connection.receiveFrame();
    throwIfErrorFrame(fetchResult);
    if (fetchResult.header.message_type != sessiongw::MessageType::fetch_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW FetchResult");
    }

    std::size_t fetchOffset = 0;
    (void)readU64(fetchResult.payload, fetchOffset);
    const bool endOfCursor = fetchResult.payload.at(fetchOffset++) != 0;
    const std::uint32_t arrowBatchBytes = readU32(fetchResult.payload, fetchOffset);
    if (options.expectEndOfCursor.has_value() && endOfCursor != *options.expectEndOfCursor)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "SessionGW query assertion failed for end_of_cursor");
    }
    const bool hasArrowBatch = arrowBatchBytes > 0;
    if (options.expectArrowBatch.has_value() && hasArrowBatch != *options.expectArrowBatch)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "SessionGW query assertion failed for Arrow batch presence");
    }

    std::uint64_t closeRequestId = 4;
    if (!options.skipCloseCursorForCleanupTest)
    {
        sessiongw::Frame closeCursor;
        closeCursor.header.message_type = sessiongw::MessageType::close_cursor;
        closeCursor.header.request_id = closeRequestId++;
        closeCursor.payload = encodeCloseCursorPayload(cursorId);
        connection.sendFrame(closeCursor);
        const sessiongw::Frame closeCursorResult = connection.receiveFrame();
        throwIfErrorFrame(closeCursorResult);
        if (closeCursorResult.header.message_type != sessiongw::MessageType::ok)
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   "Expected SessionGW CloseCursor Ok");
        }
    }

    closeSessionGateway(connection, closeRequestId);

    std::cout << "SessionGW query succeeded: cursor=" << cursorId
              << " arrow_schema_bytes=" << arrowSchemaBytes
              << " arrow_batch_bytes=" << arrowBatchBytes
              << " end_of_cursor=" << (endOfCursor ? "true" : "false")
              << " cleanup=" << (options.skipCloseCursorForCleanupTest ? "session_close" : "close_cursor")
              << '\n';
    return 0;
}

int scanTable(const int argc, char** argv)
{
    const ScanOptions options = parseScanOptions(argc, argv);
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
    enterAndHello(connection);

    sessiongw::Frame open;
    open.header.message_type = sessiongw::MessageType::open_table_scan;
    open.header.request_id = 2;
    open.payload = encodeOpenTableScanPayload(options);
    connection.sendFrame(open);
    const sessiongw::Frame openResult = connection.receiveFrame();
    throwIfErrorFrame(openResult);
    if (openResult.header.message_type != sessiongw::MessageType::open_cursor_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW OpenCursorResult");
    }

    std::size_t openOffset = 0;
    const std::uint64_t cursorId = readU64(openResult.payload, openOffset);
    const std::uint32_t arrowSchemaBytes = readU32(openResult.payload, openOffset);

    sessiongw::Frame fetch;
    fetch.header.message_type = sessiongw::MessageType::fetch;
    fetch.header.request_id = 3;
    fetch.payload = encodeFetchPayload(cursorId, options.maxRows, options.maxBytes);
    connection.sendFrame(fetch);
    const sessiongw::Frame fetchResult = connection.receiveFrame();
    throwIfErrorFrame(fetchResult);
    if (fetchResult.header.message_type != sessiongw::MessageType::fetch_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW FetchResult");
    }

    std::size_t fetchOffset = 0;
    (void)readU64(fetchResult.payload, fetchOffset);
    const bool endOfCursor = fetchResult.payload.at(fetchOffset++) != 0;
    const std::uint32_t arrowBatchBytes = readU32(fetchResult.payload, fetchOffset);
    if (options.expectEndOfCursor.has_value() && endOfCursor != *options.expectEndOfCursor)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "SessionGW scan assertion failed for end_of_cursor");
    }
    fetchOffset += arrowBatchBytes;
    std::vector<RowHandle> rowHandles;
    if (fetchOffset < fetchResult.payload.size())
    {
        rowHandles = readRowHandles(fetchResult.payload, fetchOffset);
    }
    const bool hasArrowBatch = arrowBatchBytes > 0;
    if (options.expectArrowBatch.has_value() && hasArrowBatch != *options.expectArrowBatch)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "SessionGW scan assertion failed for Arrow batch presence");
    }

    sessiongw::Frame closeCursor;
    closeCursor.header.message_type = sessiongw::MessageType::close_cursor;
    closeCursor.header.request_id = 4;
    closeCursor.payload = encodeCloseCursorPayload(cursorId);
    connection.sendFrame(closeCursor);
    const sessiongw::Frame closeCursorResult = connection.receiveFrame();
    throwIfErrorFrame(closeCursorResult);
    if (closeCursorResult.header.message_type != sessiongw::MessageType::ok)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW CloseCursor Ok");
    }

    closeSessionGateway(connection, 5);

    std::cout << "SessionGW scan succeeded: cursor=" << cursorId
              << " arrow_schema_bytes=" << arrowSchemaBytes
              << " arrow_batch_bytes=" << arrowBatchBytes
              << " end_of_cursor=" << (endOfCursor ? "true" : "false")
              << " row_handles=" << rowHandles.size();
    for (const RowHandle& rowHandle : rowHandles)
    {
        std::cout << ' ' << rowHandle.rowNumber;
    }
    std::cout << '\n';
    return 0;
}

int insertRows(const int argc, char** argv)
{
    const InsertOptions options = parseInsertOptions(argc, argv);
    const std::shared_ptr<arrow::Schema> schema = makeInsertSchema(options);

    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
    enterAndHello(connection);

    std::uint64_t requestId = 2;
    if (options.autocommit.has_value())
    {
        sessiongw::Frame setAutocommit;
        setAutocommit.header.message_type = sessiongw::MessageType::set_autocommit;
        setAutocommit.header.request_id = requestId++;
        setAutocommit.payload = encodeSetAutocommitPayload(*options.autocommit);
        connection.sendFrame(setAutocommit);
        const sessiongw::Frame setAutocommitResult = connection.receiveFrame();
        throwIfErrorFrame(setAutocommitResult);
        if (setAutocommitResult.header.message_type != sessiongw::MessageType::ok)
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   "Expected SessionGW SetAutocommit Ok");
        }
    }

    sessiongw::Frame open;
    open.header.message_type = sessiongw::MessageType::open_table_insert;
    open.header.request_id = requestId++;
    open.payload = encodeOpenTableInsertPayload(options, schema);
    connection.sendFrame(open);
    const sessiongw::Frame openResult = connection.receiveFrame();
    throwIfErrorFrame(openResult);
    if (openResult.header.message_type != sessiongw::MessageType::open_table_operation_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW OpenTableOperationResult");
    }

    std::size_t openOffset = 0;
    const std::uint64_t operationId = readU64(openResult.payload, openOffset);
    const std::uint32_t acceptedSchemaBytes = readU32(openResult.payload, openOffset);

    std::uint64_t affectedRows = 0;
    sessiongw::Frame insert;
    insert.header.message_type = sessiongw::MessageType::insert_rows;
    std::vector<std::uint8_t> nulls;
    std::vector<std::uint8_t> fixedColumnData;
    std::vector<std::size_t> variableSizes;
    std::vector<std::uint8_t> variableData;
    for (std::size_t beginRow = 0; beginRow < options.rows.size(); beginRow += options.rowsPerInsertBatch)
    {
        const std::size_t endRow = std::min(options.rows.size(), beginRow + options.rowsPerInsertBatch);
        insert.header.request_id = requestId++;
        encodeInsertRowsPayloadInto(insert.payload,
                                    operationId,
                                    options,
                                    beginRow,
                                    endRow,
                                    nulls,
                                    fixedColumnData,
                                    variableSizes,
                                    variableData);
        connection.sendFrame(insert);
        const sessiongw::Frame insertResult = connection.receiveFrame();
        throwIfErrorFrame(insertResult);
        if (insertResult.header.message_type != sessiongw::MessageType::affected_rows_result)
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   "Expected SessionGW AffectedRowsResult");
        }
        std::size_t affectedOffset = 0;
        affectedRows += readU64(insertResult.payload, affectedOffset);
    }

    sessiongw::Frame closeOperation;
    closeOperation.header.message_type = sessiongw::MessageType::close_operation;
    closeOperation.header.request_id = requestId++;
    closeOperation.payload = encodeCloseOperationPayload(operationId);
    connection.sendFrame(closeOperation);
    const sessiongw::Frame closeOperationResult = connection.receiveFrame();
    throwIfErrorFrame(closeOperationResult);
    if (closeOperationResult.header.message_type != sessiongw::MessageType::ok)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW CloseOperation Ok");
    }

    if (options.commit || options.rollback)
    {
        sessiongw::Frame tx;
        tx.header.message_type = options.commit ? sessiongw::MessageType::commit
                                                : sessiongw::MessageType::rollback;
        tx.header.request_id = requestId++;
        connection.sendFrame(tx);
        const sessiongw::Frame txResult = connection.receiveFrame();
        throwIfErrorFrame(txResult);
        if (txResult.header.message_type != sessiongw::MessageType::ok)
        {
            throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                                   "Expected SessionGW transaction Ok");
        }
    }

    closeSessionGateway(connection, requestId);

    std::cout << "SessionGW insert succeeded: operation=" << operationId
              << " rows=" << options.rows.size()
              << " affected_rows=" << affectedRows
              << " accepted_schema_bytes=" << acceptedSchemaBytes
              << " transaction=" << (options.commit ? "commit" : (options.rollback ? "rollback" : "none"))
              << '\n';
    return 0;
}

int deleteRows(const int argc, char** argv)
{
    const DeleteOptions options = parseDeleteOptions(argc, argv);
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
    enterAndHello(connection);

    std::uint64_t requestId = 2;
    if (options.autocommit.has_value())
    {
        sessiongw::Frame setAutocommit;
        setAutocommit.header.message_type = sessiongw::MessageType::set_autocommit;
        setAutocommit.header.request_id = requestId++;
        setAutocommit.payload = encodeSetAutocommitPayload(*options.autocommit);
        connection.sendFrame(setAutocommit);
        throwIfErrorFrame(connection.receiveFrame());
    }

    sessiongw::Frame open;
    open.header.message_type = sessiongw::MessageType::open_table_delete;
    open.header.request_id = requestId++;
    open.payload = encodeOpenTableDeletePayload(options);
    connection.sendFrame(open);
    const sessiongw::Frame openResult = connection.receiveFrame();
    throwIfErrorFrame(openResult);
    if (openResult.header.message_type != sessiongw::MessageType::open_table_operation_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW OpenTableOperationResult");
    }
    std::size_t openOffset = 0;
    const std::uint64_t operationId = readU64(openResult.payload, openOffset);

    sessiongw::Frame deleteFrame;
    deleteFrame.header.message_type = sessiongw::MessageType::delete_rows;
    deleteFrame.header.request_id = requestId++;
    deleteFrame.payload = encodeDeleteRowsPayload(operationId, options.rowHandles);
    connection.sendFrame(deleteFrame);
    const sessiongw::Frame deleteResult = connection.receiveFrame();
    throwIfErrorFrame(deleteResult);
    if (deleteResult.header.message_type != sessiongw::MessageType::affected_rows_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW AffectedRowsResult");
    }
    std::size_t affectedOffset = 0;
    const std::uint64_t affectedRows = readU64(deleteResult.payload, affectedOffset);

    sessiongw::Frame closeOperation;
    closeOperation.header.message_type = sessiongw::MessageType::close_operation;
    closeOperation.header.request_id = requestId++;
    closeOperation.payload = encodeCloseOperationPayload(operationId);
    connection.sendFrame(closeOperation);
    throwIfErrorFrame(connection.receiveFrame());

    if (options.commit || options.rollback)
    {
        sessiongw::Frame tx;
        tx.header.message_type = options.commit ? sessiongw::MessageType::commit
                                                : sessiongw::MessageType::rollback;
        tx.header.request_id = requestId++;
        connection.sendFrame(tx);
        throwIfErrorFrame(connection.receiveFrame());
    }
    closeSessionGateway(connection, requestId);
    std::cout << "SessionGW delete succeeded: operation=" << operationId
              << " row_handles=" << options.rowHandles.size()
              << " affected_rows=" << affectedRows << '\n';
    return 0;
}

int updateRows(const int argc, char** argv)
{
    const UpdateOptions options = parseUpdateOptions(argc, argv);
    const std::shared_ptr<arrow::Schema> schema = makeInsertSchema(options.write);
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.write.websocket);
    enterAndHello(connection);

    std::uint64_t requestId = 2;
    sessiongw::Frame open;
    open.header.message_type = sessiongw::MessageType::open_table_update;
    open.header.request_id = requestId++;
    open.payload = encodeOpenTableUpdatePayload(options, schema);
    connection.sendFrame(open);
    const sessiongw::Frame openResult = connection.receiveFrame();
    throwIfErrorFrame(openResult);
    if (openResult.header.message_type != sessiongw::MessageType::open_table_operation_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW OpenTableOperationResult");
    }
    std::size_t openOffset = 0;
    const std::uint64_t operationId = readU64(openResult.payload, openOffset);

    sessiongw::Frame updateFrame;
    updateFrame.header.message_type = sessiongw::MessageType::update_rows;
    updateFrame.header.request_id = requestId++;
    std::vector<std::uint8_t> nulls;
    std::vector<std::uint8_t> fixedColumnData;
    std::vector<std::size_t> variableSizes;
    std::vector<std::uint8_t> variableData;
    encodeUpdateRowsPayloadInto(updateFrame.payload,
                                operationId,
                                options,
                                nulls,
                                fixedColumnData,
                                variableSizes,
                                variableData);
    connection.sendFrame(updateFrame);
    const sessiongw::Frame updateResult = connection.receiveFrame();
    throwIfErrorFrame(updateResult);
    if (updateResult.header.message_type != sessiongw::MessageType::affected_rows_result)
    {
        throw sessiongw::Error(sessiongw::ErrorCategory::protocol_error,
                               "Expected SessionGW AffectedRowsResult");
    }
    std::size_t affectedOffset = 0;
    const std::uint64_t affectedRows = readU64(updateResult.payload, affectedOffset);

    sessiongw::Frame closeOperation;
    closeOperation.header.message_type = sessiongw::MessageType::close_operation;
    closeOperation.header.request_id = requestId++;
    closeOperation.payload = encodeCloseOperationPayload(operationId);
    connection.sendFrame(closeOperation);
    throwIfErrorFrame(connection.receiveFrame());

    closeSessionGateway(connection, requestId);
    std::cout << "SessionGW update succeeded: operation=" << operationId
              << " row_handles=" << options.rowHandles.size()
              << " affected_rows=" << affectedRows << '\n';
    return 0;
}

int printCapabilities()
{
    const sessiongw::Client client = sessiongw::Client::createOfflineForTesting();
    for (const sessiongw::Capability capability : client.clientCapabilities().values())
    {
        std::cout << sessiongw::toString(capability) << '\n';
    }
    return 0;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        printUsage(argv[0]);
        return 2;
    }

    const std::string_view command(argv[1]);
    if (command == "help" || command == "--help" || command == "-h")
    {
        printUsage(argv[0]);
        return 0;
    }

    if (command == "capabilities-offline")
    {
        return printCapabilities();
    }

    if (command == "ping" || command == "describe" || command == "query" || command == "scan" ||
        command == "insert" || command == "update" || command == "delete")
    {
        try
        {
            if (command == "ping")
            {
                return pingServer(argc, argv);
            }
            if (command == "describe")
            {
                return describeTable(argc, argv);
            }
            if (command == "query")
            {
                return queryServer(argc, argv);
            }
            if (command == "scan")
            {
                return scanTable(argc, argv);
            }
            if (command == "insert")
            {
                return insertRows(argc, argv);
            }
            if (command == "update")
            {
                return updateRows(argc, argv);
            }
            return deleteRows(argc, argv);
        }
        catch (const sessiongw::Error& error)
        {
            std::cerr << sessiongw::toString(error.category()) << ": " << error.what() << '\n';
            return 3;
        }
        catch (const std::exception& error)
        {
            std::cerr << "ERROR: " << error.what() << '\n';
            return 3;
        }
    }

    std::cerr << "Unknown command: " << command << "\n";
    printUsage(argv[0]);
    return 2;
}
