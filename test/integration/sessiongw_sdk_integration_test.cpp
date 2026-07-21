#include <sessiongw/errors.hpp>
#include <sessiongw/frame.hpp>
#include <sessiongw/native_write_batch.hpp>
#include <sessiongw/websocket.hpp>

#ifdef SESSIONGW_TYPED_FACADE
#include <sessiongw/sessiongw.hpp>
#include <arrow/api.h>
#endif

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace
{

struct Options
{
    sessiongw::WebSocketOptions websocket;
    std::string aclPassword;
    std::string schema = "SGW_SDK_ITEST";
    std::string table = "WORKLOAD";
    std::string typedTable = "TYPE_MATRIX";
    std::string largeTable = "LARGE_WORKLOAD";
    std::string conflictTable = "MIXED_WRITER_CONFLICT";
    std::uint32_t concurrency = 8;
    std::uint32_t concurrencyIterations = 4;
    std::uint32_t largeRows = 100000;
    std::uint32_t largeInsertBatchRows = 1000;
    std::uint32_t largeFetchBatchRows = 777;
    std::uint32_t largeUpdateRows = 0;
    std::uint32_t largeUpdateBatchRows = 333;
    std::uint32_t largeDeleteRows = 0;
    std::uint32_t largeDeleteBatchRows = 257;
    std::uint32_t lifetimeOpens = 0;
    bool quick = false;
};

struct RowHandle
{
    std::uint64_t rowNumber = 0;
};

struct FetchSummary
{
    std::uint64_t cursorId = 0;
    bool endOfCursor = false;
    std::uint32_t arrowBatchBytes = 0;
    std::vector<RowHandle> rowHandles;
};

struct DescribeSummary
{
    std::string schema;
    std::string table;
    std::string version;
    std::vector<std::uint8_t> arrowSchema;
};

struct StepResult
{
    std::string name;
    bool passed = false;
    std::string detail;
    std::chrono::milliseconds duration{0};
};

struct LargeWorkloadSummary
{
    std::uint32_t rows = 0;
    std::uint32_t insertBatchRows = 0;
    std::uint32_t fetchBatchRows = 0;
    std::uint32_t updateRows = 0;
    std::uint32_t updateBatchRows = 0;
    std::uint32_t deleteRows = 0;
    std::uint32_t deleteBatchRows = 0;
};

struct MixedWriterSummary
{
    std::uint32_t attempts = 0;
    std::uint32_t committed = 0;
    std::uint32_t conflicts = 0;
};

std::mutex sdkStatisticsMutex;
sessiongw::WebSocketStatistics sdkStatistics;

void addSdkStatistics(const sessiongw::WebSocketStatistics& value)
{
    std::lock_guard lock(sdkStatisticsMutex);
    sdkStatistics.connect_nanoseconds += value.connect_nanoseconds;
    sdkStatistics.transport_write_calls += value.transport_write_calls;
    sdkStatistics.transport_read_calls += value.transport_read_calls;
    sdkStatistics.transport_bytes_written += value.transport_bytes_written;
    sdkStatistics.transport_bytes_read += value.transport_bytes_read;
    sdkStatistics.transport_write_nanoseconds += value.transport_write_nanoseconds;
    sdkStatistics.transport_read_nanoseconds += value.transport_read_nanoseconds;
    sdkStatistics.sessiongw_frames_sent += value.sessiongw_frames_sent;
    sdkStatistics.sessiongw_frames_received += value.sessiongw_frames_received;
    sdkStatistics.sessiongw_payload_bytes_sent += value.sessiongw_payload_bytes_sent;
    sdkStatistics.sessiongw_payload_bytes_received += value.sessiongw_payload_bytes_received;
    sdkStatistics.websocket_mask_copy_bytes += value.websocket_mask_copy_bytes;
}

void appendU8(std::vector<std::uint8_t>& out, const std::uint8_t value)
{
    out.push_back(value);
}

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

std::uint8_t readU8(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    if (offset + 1U > bytes.size())
    {
        throw std::runtime_error("truncated u8");
    }
    return bytes[offset++];
}

std::uint16_t readU16(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    if (offset + 2U > bytes.size())
    {
        throw std::runtime_error("truncated u16");
    }
    const std::uint16_t value = static_cast<std::uint16_t>(
        (static_cast<std::uint16_t>(bytes[offset]) << 8U) | bytes[offset + 1U]);
    offset += 2U;
    return value;
}

std::uint32_t readU32(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    if (offset + 4U > bytes.size())
    {
        throw std::runtime_error("truncated u32");
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
        throw std::runtime_error("truncated u64");
    }
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < 8U; ++index)
    {
        value = (value << 8U) | bytes[offset + index];
    }
    offset += 8U;
    return value;
}

void appendString16(std::vector<std::uint8_t>& out, const std::string& value)
{
    if (value.size() > 0xffffU)
    {
        throw std::runtime_error("string16 too large");
    }
    appendU16(out, static_cast<std::uint16_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

void appendString32(std::vector<std::uint8_t>& out, const std::string& value)
{
    appendU32(out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

std::string jsonEscapeForCommand(const std::string& value)
{
    std::string out;
    out.reserve(value.size() + 8U);
    for (const char ch : value)
    {
        switch (ch)
        {
            case '\\': out += "\\\\"; break;
            case '"': out += "\\\""; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out += ch; break;
        }
    }
    return out;
}

std::string jsonStringValueForKey(const std::string& json, const std::string& key)
{
    const std::string quotedKey = "\"" + key + "\"";
    std::size_t pos = json.find(quotedKey);
    if (pos == std::string::npos)
    {
        throw std::runtime_error("JSON response does not contain key: " + key + "; json=" + json);
    }
    pos = json.find(':', pos + quotedKey.size());
    if (pos == std::string::npos)
    {
        throw std::runtime_error("JSON response has malformed key: " + key + "; json=" + json);
    }
    pos = json.find('"', pos + 1U);
    if (pos == std::string::npos)
    {
        throw std::runtime_error("JSON response value is not a string: " + key + "; json=" + json);
    }
    ++pos;

    std::string value;
    bool escaped = false;
    for (; pos < json.size(); ++pos)
    {
        const char ch = json[pos];
        if (escaped)
        {
            value += ch;
            escaped = false;
            continue;
        }
        if (ch == '\\')
        {
            escaped = true;
            continue;
        }
        if (ch == '"')
        {
            return value;
        }
        value += ch;
    }
    throw std::runtime_error("JSON response has unterminated string: " + key + "; json=" + json);
}

void requireJsonStatusOk(const std::string& json)
{
    if (jsonStringValueForKey(json, "status") != "ok")
    {
        throw std::runtime_error("WebSocket SQL command returned non-ok status: " + json);
    }
}

std::string firstJsonResultScalar(const std::string& json)
{
    std::size_t pos = json.find("\"data\"");
    if (pos == std::string::npos)
    {
        throw std::runtime_error("WebSocket SQL response has no result data: " + json);
    }
    pos = json.find("[[", pos);
    if (pos == std::string::npos)
    {
        throw std::runtime_error("WebSocket SQL response has no first scalar: " + json);
    }
    pos += 2U;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos])))
    {
        ++pos;
    }
    if (pos >= json.size())
    {
        throw std::runtime_error("WebSocket SQL response has empty first scalar: " + json);
    }
    if (json[pos] == '"')
    {
        ++pos;
        std::string value;
        bool escaped = false;
        for (; pos < json.size(); ++pos)
        {
            const char ch = json[pos];
            if (escaped)
            {
                value += ch;
                escaped = false;
                continue;
            }
            if (ch == '\\')
            {
                escaped = true;
                continue;
            }
            if (ch == '"')
            {
                return value;
            }
            value += ch;
        }
        throw std::runtime_error("WebSocket SQL response has unterminated scalar: " + json);
    }

    const std::size_t end = json.find_first_of(",]", pos);
    if (end == std::string::npos)
    {
        throw std::runtime_error("WebSocket SQL response has unterminated scalar: " + json);
    }
    return json.substr(pos, end - pos);
}

std::string readString16(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    const std::uint16_t size = readU16(bytes, offset);
    if (offset + size > bytes.size())
    {
        throw std::runtime_error("truncated string16");
    }
    std::string value(reinterpret_cast<const char*>(bytes.data() + offset), size);
    offset += size;
    return value;
}

std::vector<std::uint8_t> readBytes32(const std::span<const std::uint8_t> bytes, std::size_t& offset)
{
    const std::uint32_t size = readU32(bytes, offset);
    if (offset + size > bytes.size())
    {
        throw std::runtime_error("truncated bytes32");
    }
    std::vector<std::uint8_t> value(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                    bytes.begin() + static_cast<std::ptrdiff_t>(offset + size));
    offset += size;
    return value;
}

void appendBytes32(std::vector<std::uint8_t>& out, const std::span<const std::uint8_t> bytes)
{
    appendU32(out, static_cast<std::uint32_t>(bytes.size()));
    out.insert(out.end(), bytes.begin(), bytes.end());
}

std::vector<std::uint8_t> tableNamePayload(const std::string& schema, const std::string& table)
{
    std::vector<std::uint8_t> payload;
    appendString16(payload, schema);
    appendString16(payload, table);
    return payload;
}

std::vector<std::uint8_t> openPushedQueryPayload(const std::string& sql)
{
    std::vector<std::uint8_t> payload;
    appendString32(payload, sql);
    return payload;
}

/// Builds a forward-scan open payload; explicit positions use fetchPositionedRowsPayload().
std::vector<std::uint8_t> openTableScanPayload(const std::string& schema,
                                               const std::string& table,
                                               const std::vector<std::string>& columns,
                                               const bool includeRowHandles)
{
    std::vector<std::uint8_t> payload;
    appendString32(payload, schema);
    appendString32(payload, table);
    appendU32(payload, static_cast<std::uint32_t>(columns.size()));
    for (const auto& column : columns)
    {
        appendString32(payload, column);
    }
    appendU8(payload, includeRowHandles ? 1U : 0U);
    return payload;
}

/// Builds the fixed-format ordinary next-batch payload.
std::vector<std::uint8_t> fetchPayload(const std::uint64_t cursorId,
                                       const std::uint32_t maxRows,
                                       const std::uint32_t maxBytes = 0)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursorId);
    appendU32(payload, maxRows);
    appendU32(payload, maxBytes);
    return payload;
}

/// Builds the dedicated positioned-read payload used by adapter integration checks.
std::vector<std::uint8_t> fetchPositionedRowsPayload(const std::uint64_t cursorId,
                                                      const std::span<const RowHandle> rowHandles,
                                                      const std::uint32_t maxBytes = 0)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursorId);
    appendU32(payload, maxBytes);
    appendU32(payload, static_cast<std::uint32_t>(rowHandles.size()));
    for (const RowHandle& rowHandle : rowHandles)
    {
        appendU64(payload, rowHandle.rowNumber);
    }
    return payload;
}

std::vector<std::uint8_t> closeCursorPayload(const std::uint64_t cursorId)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, cursorId);
    return payload;
}

std::vector<std::uint8_t> closeOperationPayload(const std::uint64_t operationId)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, operationId);
    return payload;
}

std::vector<std::uint8_t> setAutocommitPayload(const bool enabled)
{
    return {static_cast<std::uint8_t>(enabled ? 1U : 0U)};
}

void appendRowHandles(std::vector<std::uint8_t>& payload, const std::span<const RowHandle> rowHandles)
{
    appendU32(payload, static_cast<std::uint32_t>(rowHandles.size()));
    for (const RowHandle& rowHandle : rowHandles)
    {
        appendU64(payload, rowHandle.rowNumber);
    }
}

std::vector<std::uint8_t> openTableWritePayload(const std::string& schema,
                                                const std::string& table,
                                                const std::vector<std::string>& columns,
                                                const std::uint32_t maxRowsPerBatch,
                                                const std::span<const std::uint8_t> arrowSchema)
{
    std::vector<std::uint8_t> payload;
    appendString32(payload, schema);
    appendString32(payload, table);
    appendU32(payload, static_cast<std::uint32_t>(columns.size()));
    for (const auto& column : columns)
    {
        appendString32(payload, column);
    }
    appendU32(payload, maxRowsPerBatch);
    appendBytes32(payload, arrowSchema);
    return payload;
}

std::vector<std::uint8_t> openTableDeletePayload(const std::string& schema,
                                                 const std::string& table,
                                                 const std::uint32_t maxRowsPerBatch)
{
    std::vector<std::uint8_t> payload;
    appendString32(payload, schema);
    appendString32(payload, table);
    appendU32(payload, maxRowsPerBatch);
    return payload;
}

void appendStringColumn(sessiongw::NativeWriteBatchBuilder& builder,
                        const std::vector<std::optional<std::string>>& values)
{
    std::vector<std::optional<std::string_view>> views;
    views.reserve(values.size());
    for (const std::optional<std::string>& value : values)
    {
        views.emplace_back(value.has_value() ? std::optional<std::string_view>(*value) : std::nullopt);
    }
    builder.appendStringColumn(views);
}

template <typename T>
void appendTypedColumn(sessiongw::NativeWriteBatchBuilder& builder,
                       const std::initializer_list<std::optional<T>> values,
                       void (sessiongw::NativeWriteBatchBuilder::*append)(
                           std::span<const std::optional<T>>))
{
    const std::vector<std::optional<T>> column(values);
    (builder.*append)(column);
}

std::vector<std::uint8_t> nativeNullableStringRowsBatch(
    const std::vector<std::optional<std::string>>& keys,
    const std::vector<std::optional<std::string>>& names)
{
    if (keys.size() != names.size())
    {
        throw std::runtime_error("nativeNullableStringRowsBatch key/name size mismatch");
    }

    std::vector<std::uint8_t> payload;
    sessiongw::NativeWriteBatchBuilder builder(payload);
    builder.begin(static_cast<std::uint32_t>(keys.size()), 2U);
    appendStringColumn(builder, keys);
    appendStringColumn(builder, names);
    builder.finish();
    return payload;
}

std::vector<std::uint8_t> nativeStringRowsBatch(const std::vector<std::string>& keys,
                                                const std::vector<std::string>& names)
{
    std::vector<std::optional<std::string>> optionalKeys(keys.begin(), keys.end());
    std::vector<std::optional<std::string>> optionalNames(names.begin(), names.end());
    return nativeNullableStringRowsBatch(optionalKeys, optionalNames);
}

std::vector<std::uint8_t> nativeTypedRowsBatch(const bool updatedRow)
{
    // Table order: KEYTXT, FLAG, SMALL_DEC, INT_DEC, BIG_DEC, SCALED_DEC,
    // DBL, DT, TS, CHR, TXT. This covers all DTM families currently supported
    // by SessionGW Arrow scan conversion and native fixed/variable write paths.
    std::vector<std::uint8_t> payload;
    sessiongw::NativeWriteBatchBuilder builder(payload);
    builder.begin(updatedRow ? 1U : 3U, 11U);

    const auto strings = [&](const std::initializer_list<std::optional<std::string>> values) {
        appendStringColumn(builder, std::vector<std::optional<std::string>>(values));
    };
    if (updatedRow)
    {
        strings({std::string("typed_min")});
        appendTypedColumn<bool>(builder, {true}, &sessiongw::NativeWriteBatchBuilder::appendBooleanColumn);
        appendTypedColumn<std::int32_t>(builder, {321}, &sessiongw::NativeWriteBatchBuilder::appendDecimal32Column);
        appendTypedColumn<std::int64_t>(builder, {9876543210LL}, &sessiongw::NativeWriteBatchBuilder::appendDecimal64Column);
        appendTypedColumn<sessiongw::NativeDecimal128>(
            builder, {sessiongw::NativeDecimal128::fromInt64(777777777777LL)},
            &sessiongw::NativeWriteBatchBuilder::appendDecimal128Column);
        appendTypedColumn<std::int64_t>(builder, {4242}, &sessiongw::NativeWriteBatchBuilder::appendDecimal64Column);
        appendTypedColumn<double>(builder, {42.5}, &sessiongw::NativeWriteBatchBuilder::appendDoubleColumn);
        appendTypedColumn<sessiongw::NativeDate>(
            builder, {sessiongw::NativeDate::fromYmd(2026, 7, 9)},
            &sessiongw::NativeWriteBatchBuilder::appendDateColumn);
        appendTypedColumn<sessiongw::NativeTimestamp>(
            builder, {sessiongw::NativeTimestamp::fromComponents(2026, 7, 9, 10, 11, 12, 123456000)},
            &sessiongw::NativeWriteBatchBuilder::appendTimestampColumn);
        strings({std::string("upd")});
        strings({std::string("typed_updated")});
    }
    else
    {
        strings({std::string("typed_min"), std::string("typed_max"), std::string("typed_null")});
        appendTypedColumn<bool>(builder, {false, true, std::nullopt},
                                &sessiongw::NativeWriteBatchBuilder::appendBooleanColumn);
        appendTypedColumn<std::int32_t>(builder, {-123456789, 123456789, std::nullopt},
                                        &sessiongw::NativeWriteBatchBuilder::appendDecimal32Column);
        appendTypedColumn<std::int64_t>(builder, {-2147483648LL, 2147483647LL, std::nullopt},
                                        &sessiongw::NativeWriteBatchBuilder::appendDecimal64Column);
        appendTypedColumn<sessiongw::NativeDecimal128>(
            builder,
            {sessiongw::NativeDecimal128::fromInt64(std::numeric_limits<std::int64_t>::min()),
             sessiongw::NativeDecimal128::fromInt64(std::numeric_limits<std::int64_t>::max()),
             std::nullopt},
            &sessiongw::NativeWriteBatchBuilder::appendDecimal128Column);
        appendTypedColumn<std::int64_t>(builder, {-9999999999LL, 9999999999LL, std::nullopt},
                                        &sessiongw::NativeWriteBatchBuilder::appendDecimal64Column);
        appendTypedColumn<double>(builder, {-12345.5, 12345.5, std::nullopt},
                                  &sessiongw::NativeWriteBatchBuilder::appendDoubleColumn);
        appendTypedColumn<sessiongw::NativeDate>(
            builder,
            {sessiongw::NativeDate::fromYmd(1000, 1, 1), sessiongw::NativeDate::fromYmd(9999, 12, 31),
             std::nullopt},
            &sessiongw::NativeWriteBatchBuilder::appendDateColumn);
        appendTypedColumn<sessiongw::NativeTimestamp>(
            builder,
            {sessiongw::NativeTimestamp::fromComponents(1970, 1, 2, 3, 4, 5, 1000),
             sessiongw::NativeTimestamp::fromComponents(2037, 12, 31, 23, 59, 59, 999999000),
             std::nullopt},
            &sessiongw::NativeWriteBatchBuilder::appendTimestampColumn);
        strings({std::string("abc"), std::string("xyz"), std::nullopt});
        strings({std::string("unicode_äöü"), std::string("12345678901234567890"), std::nullopt});
    }

    builder.finish();
    return payload;
}


std::vector<std::uint8_t> insertRowsPayload(const std::uint64_t operationId,
                                            const std::span<const std::uint8_t> nativeBatch)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, operationId);
    appendU32(payload, static_cast<std::uint32_t>(nativeBatch.size()));
    sessiongw::alignNativeWriteBatch(payload);
    payload.insert(payload.end(), nativeBatch.begin(), nativeBatch.end());
    return payload;
}

std::vector<std::uint8_t> updateRowsPayload(const std::uint64_t operationId,
                                            const std::span<const RowHandle> rowHandles,
                                            const std::span<const std::uint8_t> nativeBatch)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, operationId);
    appendRowHandles(payload, rowHandles);
    appendU32(payload, static_cast<std::uint32_t>(nativeBatch.size()));
    sessiongw::alignNativeWriteBatch(payload);
    payload.insert(payload.end(), nativeBatch.begin(), nativeBatch.end());
    return payload;
}

std::vector<std::uint8_t> deleteRowsPayload(const std::uint64_t operationId,
                                            const std::span<const RowHandle> rowHandles)
{
    std::vector<std::uint8_t> payload;
    appendU64(payload, operationId);
    appendRowHandles(payload, rowHandles);
    return payload;
}

sessiongw::ErrorCategory decodeErrorCategory(const std::uint16_t value)
{
    switch (value)
    {
        case 1: return sessiongw::ErrorCategory::protocol_error;
        case 2: return sessiongw::ErrorCategory::authentication_failed;
        case 3: return sessiongw::ErrorCategory::not_authorized;
        case 4: return sessiongw::ErrorCategory::object_not_found;
        case 5: return sessiongw::ErrorCategory::unsupported_type;
        case 6: return sessiongw::ErrorCategory::unsupported_operation;
        case 7: return sessiongw::ErrorCategory::transaction_conflict;
        case 8: return sessiongw::ErrorCategory::constraint_violation;
        case 9: return sessiongw::ErrorCategory::resource_limit;
        case 10: return sessiongw::ErrorCategory::cursor_not_found;
        case 11: return sessiongw::ErrorCategory::internal_error;
        case 12: return sessiongw::ErrorCategory::transport_error;
        default: return sessiongw::ErrorCategory::protocol_error;
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
    const std::string message = readString16(frame.payload, offset);
    throw sessiongw::Error(decodeErrorCategory(category), message);
}

class SqlControlSession
{
public:
    explicit SqlControlSession(const sessiongw::WebSocketOptions& options)
        : connection_(sessiongw::WebSocketConnection::connectAndLogin(options))
    {
    }

    ~SqlControlSession()
    {
        try
        {
            connection_.close();
        }
        catch (...)
        {
        }
        addSdkStatistics(connection_.statistics());
    }

    std::string execute(const std::string& sql)
    {
        const std::string request = "{\"command\":\"execute\",\"sqlText\":\"" +
                                    jsonEscapeForCommand(sql) + "\"}";
        const std::string response = connection_.sendJsonCommand(request);
        requireJsonStatusOk(response);
        return response;
    }

    std::uint64_t executeScalarU64(const std::string& sql)
    {
        const std::string scalar = firstJsonResultScalar(execute(sql));
        return static_cast<std::uint64_t>(std::stoull(scalar));
    }

private:
    sessiongw::WebSocketConnection connection_;
};

class GatewaySession
{
public:
    explicit GatewaySession(const sessiongw::WebSocketOptions& options,
                            const std::vector<std::string>& preGatewaySql = {},
                            const bool captureSessionId = false)
        : connection_(sessiongw::WebSocketConnection::connectAndLogin(options))
    {
        if (captureSessionId)
        {
            sessionId_ = executeScalarU64BeforeGateway("select current_session");
        }
        for (const std::string& sql : preGatewaySql)
        {
            executeSqlBeforeGateway(sql);
        }
        connection_.enterSessionGateway();
        send(sessiongw::MessageType::hello, {});
        expect(sessiongw::MessageType::hello_ok);
    }

    ~GatewaySession()
    {
        if (!closed_)
        {
            try
            {
                close();
            }
            catch (...)
            {
            }
        }
        addSdkStatistics(connection_.statistics());
    }

    sessiongw::Frame request(const sessiongw::MessageType type,
                             std::vector<std::uint8_t> payload = {},
                             const sessiongw::MessageType expected = sessiongw::MessageType::ok)
    {
        send(type, std::move(payload));
        return expect(expected);
    }

    std::uint64_t sessionId() const
    {
        return sessionId_.value_or(0U);
    }

    void close()
    {
        send(sessiongw::MessageType::close, {});
        expect(sessiongw::MessageType::ok);
        closed_ = true;
        connection_.close();
    }

    void dropTransportForTest()
    {
        closed_ = true;
        connection_.close();
    }

private:
    void executeSqlBeforeGateway(const std::string& sql)
    {
        const std::string request = "{\"command\":\"execute\",\"sqlText\":\"" +
                                    jsonEscapeForCommand(sql) + "\"}";
        requireJsonStatusOk(connection_.sendJsonCommand(request));
    }

    std::uint64_t executeScalarU64BeforeGateway(const std::string& sql)
    {
        const std::string request = "{\"command\":\"execute\",\"sqlText\":\"" +
                                    jsonEscapeForCommand(sql) + "\"}";
        const std::string response = connection_.sendJsonCommand(request);
        requireJsonStatusOk(response);
        return static_cast<std::uint64_t>(std::stoull(firstJsonResultScalar(response)));
    }

    void send(const sessiongw::MessageType type, std::vector<std::uint8_t> payload)
    {
        sessiongw::Frame frame;
        frame.header.message_type = type;
        frame.header.request_id = requestId_++;
        lastRequestId_ = frame.header.request_id;
        frame.payload = std::move(payload);
        connection_.sendFrame(frame);
    }

    sessiongw::Frame expect(const sessiongw::MessageType expected)
    {
        sessiongw::Frame frame = connection_.receiveFrame(lastRequestId_);
        throwIfErrorFrame(frame);
        if (frame.header.message_type != expected)
        {
            std::ostringstream out;
            out << "unexpected SessionGW frame type " << static_cast<std::uint16_t>(frame.header.message_type)
                << ", expected " << static_cast<std::uint16_t>(expected);
            throw std::runtime_error(out.str());
        }
        return frame;
    }

    sessiongw::WebSocketConnection connection_;
    std::uint64_t requestId_ = 1;
    std::uint64_t lastRequestId_ = 0;
    std::optional<std::uint64_t> sessionId_;
    bool closed_ = false;
};

std::uint64_t parseOpenCursorId(const sessiongw::Frame& frame, std::vector<std::uint8_t>* schemaBytes = nullptr)
{
    std::size_t offset = 0;
    const std::uint64_t cursorId = readU64(frame.payload, offset);
    const std::vector<std::uint8_t> schema = readBytes32(frame.payload, offset);
    if (schemaBytes != nullptr)
    {
        *schemaBytes = schema;
    }
    if (cursorId == 0 || schema.empty())
    {
        throw std::runtime_error("invalid OpenCursorResult");
    }
    return cursorId;
}

FetchSummary parseFetchResult(const sessiongw::Frame& frame)
{
    FetchSummary result;
    std::size_t offset = 0;
    result.cursorId = readU64(frame.payload, offset);
    result.endOfCursor = readU8(frame.payload, offset) != 0U;
    result.arrowBatchBytes = readU32(frame.payload, offset);
    if (offset + result.arrowBatchBytes > frame.payload.size())
    {
        throw std::runtime_error("truncated Arrow batch in FetchResult");
    }
    offset += result.arrowBatchBytes;
    if (offset < frame.payload.size())
    {
        const std::uint32_t count = readU32(frame.payload, offset);
        result.rowHandles.reserve(count);
        for (std::uint32_t index = 0; index < count; ++index)
        {
            result.rowHandles.push_back({readU64(frame.payload, offset)});
        }
    }
    return result;
}

std::uint64_t parseOpenOperationId(const sessiongw::Frame& frame,
                                   std::vector<std::uint8_t>* acceptedSchemaBytes = nullptr)
{
    std::size_t offset = 0;
    const std::uint64_t operationId = readU64(frame.payload, offset);
    if (acceptedSchemaBytes != nullptr && offset < frame.payload.size())
    {
        *acceptedSchemaBytes = readBytes32(frame.payload, offset);
    }
    if (operationId == 0)
    {
        throw std::runtime_error("invalid OpenTableOperationResult");
    }
    return operationId;
}

std::uint64_t parseAffectedRows(const sessiongw::Frame& frame)
{
    std::size_t offset = 0;
    return readU64(frame.payload, offset);
}

std::string expectSessionError(GatewaySession& session,
                               const sessiongw::MessageType requestType,
                               std::vector<std::uint8_t> payload,
                               const sessiongw::MessageType unexpectedSuccessType)
{
    try
    {
        (void)session.request(requestType, std::move(payload), unexpectedSuccessType);
    }
    catch (const sessiongw::Error& error)
    {
        return std::string(sessiongw::toString(error.category())) + ": " + error.what();
    }
    throw std::runtime_error("expected structured SessionGW error but request succeeded");
}

enum class DirectTableOperation
{
    select,
    insert,
    update,
    deleteRows
};

std::string_view directTableOperationName(const DirectTableOperation operation)
{
    switch (operation)
    {
        case DirectTableOperation::select: return "SELECT";
        case DirectTableOperation::insert: return "INSERT";
        case DirectTableOperation::update: return "UPDATE";
        case DirectTableOperation::deleteRows: return "DELETE";
    }
    throw std::runtime_error("unknown direct table operation");
}

void checkDirectTableOperation(const Options& options,
                               const std::span<const std::uint8_t> arrowSchema,
                               const std::string& user,
                               const DirectTableOperation operation,
                               const bool shouldSucceed)
{
    sessiongw::WebSocketOptions websocket = options.websocket;
    websocket.user = user;
    websocket.password = options.aclPassword;
    GatewaySession session(websocket);

    try
    {
        if (operation == DirectTableOperation::select)
        {
            const sessiongw::Frame open = session.request(
                sessiongw::MessageType::open_table_scan,
                openTableScanPayload(options.schema, options.table, {"KEYTXT"}, false),
                sessiongw::MessageType::open_cursor_result);
            const std::uint64_t cursorId = parseOpenCursorId(open);
            if (shouldSucceed)
            {
                const FetchSummary fetched = parseFetchResult(session.request(
                    sessiongw::MessageType::fetch,
                    fetchPayload(cursorId, 1U, 1024U * 1024U),
                    sessiongw::MessageType::fetch_result));
                if (fetched.arrowBatchBytes == 0U)
                {
                    throw std::runtime_error("authorized SELECT returned no data batch");
                }
            }
            session.request(sessiongw::MessageType::close_cursor,
                            closeCursorPayload(cursorId),
                            sessiongw::MessageType::ok);
        }
        else
        {
            sessiongw::MessageType requestType = sessiongw::MessageType::open_table_insert;
            std::vector<std::uint8_t> payload = openTableWritePayload(
                options.schema, options.table, {"KEYTXT", "NAME"}, 1U, arrowSchema);
            if (operation == DirectTableOperation::update)
            {
                requestType = sessiongw::MessageType::open_table_update;
            }
            else if (operation == DirectTableOperation::deleteRows)
            {
                requestType = sessiongw::MessageType::open_table_delete;
                payload = openTableDeletePayload(options.schema, options.table, 1U);
            }
            if (shouldSucceed && operation == DirectTableOperation::insert)
            {
                session.request(sessiongw::MessageType::set_autocommit,
                                setAutocommitPayload(false),
                                sessiongw::MessageType::ok);
            }
            const sessiongw::Frame open = session.request(
                requestType, std::move(payload), sessiongw::MessageType::open_table_operation_result);
            const std::uint64_t operationId = parseOpenOperationId(open);
            if (shouldSucceed && operation == DirectTableOperation::insert)
            {
                const std::vector<std::uint8_t> batch =
                    nativeStringRowsBatch({"acl_insert_probe"}, {"rolled_back"});
                const std::uint64_t affected = parseAffectedRows(session.request(
                    sessiongw::MessageType::insert_rows,
                    insertRowsPayload(operationId, batch),
                    sessiongw::MessageType::affected_rows_result));
                if (affected != 1U)
                {
                    throw std::runtime_error("authorized INSERT data path affected an unexpected row count");
                }
            }
            session.request(sessiongw::MessageType::close_operation,
                            closeOperationPayload(operationId),
                            sessiongw::MessageType::ok);
            if (shouldSucceed && operation == DirectTableOperation::insert)
            {
                session.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
            }
        }
        if (!shouldSucceed)
        {
            throw std::runtime_error(user + " unexpectedly authorized for " +
                                     std::string(directTableOperationName(operation)));
        }
    }
    catch (const sessiongw::Error& error)
    {
        if (shouldSucceed)
        {
            throw;
        }
        if (error.category() != sessiongw::ErrorCategory::not_authorized)
        {
            throw std::runtime_error(user + " received " +
                                     std::string(sessiongw::toString(error.category())) +
                                     " instead of not_authorized for " +
                                     std::string(directTableOperationName(operation)));
        }
    }
    session.close();
}

void exerciseAuthorizedMutation(const Options& options,
                                const std::span<const std::uint8_t> arrowSchema,
                                const std::string& user,
                                const DirectTableOperation operation)
{
    sessiongw::WebSocketOptions websocket = options.websocket;
    websocket.user = user;
    websocket.password = options.aclPassword;
    GatewaySession session(websocket);
    session.request(sessiongw::MessageType::set_autocommit,
                    setAutocommitPayload(false),
                    sessiongw::MessageType::ok);

    const sessiongw::Frame scanOpen = session.request(
        sessiongw::MessageType::open_table_scan,
        openTableScanPayload(options.schema, options.table, {"KEYTXT", "NAME"}, true),
        sessiongw::MessageType::open_cursor_result);
    const std::uint64_t cursorId = parseOpenCursorId(scanOpen);
    const FetchSummary fetched = parseFetchResult(session.request(
        sessiongw::MessageType::fetch,
        fetchPayload(cursorId, 1U, 1024U * 1024U),
        sessiongw::MessageType::fetch_result));
    session.request(sessiongw::MessageType::close_cursor,
                    closeCursorPayload(cursorId),
                    sessiongw::MessageType::ok);
    if (fetched.rowHandles.size() != 1U)
    {
        throw std::runtime_error(user + " did not receive one transaction-scoped row location");
    }

    sessiongw::MessageType openType = sessiongw::MessageType::open_table_update;
    std::vector<std::uint8_t> openPayload = openTableWritePayload(
        options.schema, options.table, {"KEYTXT", "NAME"}, 1U, arrowSchema);
    if (operation == DirectTableOperation::deleteRows)
    {
        openType = sessiongw::MessageType::open_table_delete;
        openPayload = openTableDeletePayload(options.schema, options.table, 1U);
    }
    const std::uint64_t operationId = parseOpenOperationId(session.request(
        openType, std::move(openPayload), sessiongw::MessageType::open_table_operation_result));
    if (operation == DirectTableOperation::update)
    {
        const std::vector<std::uint8_t> batch =
            nativeStringRowsBatch({"acl_update_probe"}, {"rolled_back"});
        const std::uint64_t affected = parseAffectedRows(session.request(
            sessiongw::MessageType::update_rows,
            updateRowsPayload(operationId, fetched.rowHandles, batch),
            sessiongw::MessageType::affected_rows_result));
        if (affected != 1U)
        {
            throw std::runtime_error("authorized UPDATE data path affected an unexpected row count");
        }
    }
    else
    {
        const std::uint64_t affected = parseAffectedRows(session.request(
            sessiongw::MessageType::delete_rows,
            deleteRowsPayload(operationId, fetched.rowHandles),
            sessiongw::MessageType::affected_rows_result));
        if (affected != 1U)
        {
            throw std::runtime_error("authorized DELETE data path affected an unexpected row count");
        }
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
    session.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
    session.close();
}

void directTablePrivilegeStep(const Options& options, const std::span<const std::uint8_t> arrowSchema)
{
    const std::vector<DirectTableOperation> operations = {
        DirectTableOperation::select,
        DirectTableOperation::insert,
        DirectTableOperation::update,
        DirectTableOperation::deleteRows};
    const std::vector<std::pair<std::string, std::optional<DirectTableOperation>>> users = {
        {"SGW_ACL_NONE", std::nullopt},
        {"SGW_ACL_SELECT", DirectTableOperation::select},
        {"SGW_ACL_INSERT", DirectTableOperation::insert},
        {"SGW_ACL_UPDATE", DirectTableOperation::update},
        {"SGW_ACL_DELETE", DirectTableOperation::deleteRows},
        {"SGW_ACL_SELECT_ANY", DirectTableOperation::select},
        {"SGW_ACL_INSERT_ANY", DirectTableOperation::insert},
        {"SGW_ACL_UPDATE_ANY", DirectTableOperation::update},
        {"SGW_ACL_DELETE_ANY", DirectTableOperation::deleteRows}};

    for (const auto& [user, allowedOperation] : users)
    {
        for (const DirectTableOperation operation : operations)
        {
            checkDirectTableOperation(options,
                                      arrowSchema,
                                      user,
                                      operation,
                                      allowedOperation.has_value() && *allowedOperation == operation);
        }
    }

    // Option A: mutation authorization remains operation-specific, while the
    // normal row-discovery workflow additionally requires SELECT.
    exerciseAuthorizedMutation(options, arrowSchema, "SGW_ACL_SELECT_UPDATE", DirectTableOperation::update);
    exerciseAuthorizedMutation(options, arrowSchema, "SGW_ACL_SELECT_DELETE", DirectTableOperation::deleteRows);
}

DescribeSummary describeNamedTable(GatewaySession& session,
                                    const Options& options,
                                    const std::string& tableName)
{
    const sessiongw::Frame frame = session.request(sessiongw::MessageType::describe_table,
                                                   tableNamePayload(options.schema, tableName),
                                                   sessiongw::MessageType::describe_table_result);
    DescribeSummary result;
    std::size_t offset = 0;
    result.schema = readString16(frame.payload, offset);
    result.table = readString16(frame.payload, offset);
    result.version = readString16(frame.payload, offset);
    result.arrowSchema = readBytes32(frame.payload, offset);
    if (result.schema != options.schema || result.table != tableName || result.arrowSchema.empty())
    {
        throw std::runtime_error("DescribeTable result mismatch");
    }
    return result;
}

DescribeSummary describeTable(GatewaySession& session, const Options& options)
{
    return describeNamedTable(session, options, options.table);
}

void pingStep(const Options& options)
{
    GatewaySession session(options.websocket);
    session.request(sessiongw::MessageType::ping, {}, sessiongw::MessageType::pong);
    session.close();
}

#ifdef SESSIONGW_TYPED_FACADE
void publicFacadeStep(const Options& options)
{
    sessiongw::Session session = sessiongw::Session::connect(options.websocket);
    if (!session.capabilities().supports(sessiongw::Capability::metadata_v1) ||
        !session.capabilities().supports(sessiongw::Capability::native_table_write_batch_v2))
    {
        throw std::runtime_error("public facade did not negotiate production capabilities");
    }

    const sessiongw::TableName table{options.schema, options.table};
    const sessiongw::TableMetadata metadata = session.describeTable(table);
    if (metadata.schema == nullptr || metadata.schema->num_fields() != 2 ||
        !metadata.row_count.has_value() ||
        session.getTableVersion(table) != metadata.version)
    {
        throw std::runtime_error("public facade returned inconsistent typed metadata");
    }

    sessiongw::Cursor query = session.openPushedQuery("select cast(1 as integer) as id");
    const sessiongw::FetchBatch queryBatch = session.fetch(query, 1U);
    if (queryBatch.rows == nullptr || queryBatch.rows->num_rows() != 1)
    {
        throw std::runtime_error("public facade did not decode pushed-query Arrow rows");
    }
    session.closeCursor(query);

    const auto scanOne = [&]() {
        sessiongw::Cursor cursor = session.openTableScan(table, {"KEYTXT", "NAME"}, true);
        sessiongw::FetchBatch batch = session.fetch(cursor, 1U);
        session.closeCursor(cursor);
        if (batch.rows == nullptr || batch.rows->num_rows() != 1 || batch.row_locations.size() != 1U)
        {
            throw std::runtime_error("public facade did not decode scan rows and row locations");
        }
        return batch.row_locations.front();
    };

    session.setAutocommit(false);
    sessiongw::TableOperation insert = session.openTableInsert(
        table, {"KEYTXT", "NAME"}, 1U, metadata.schema);
    const std::vector<std::uint8_t> insertBatch =
        nativeStringRowsBatch({"public_facade_rollback"}, {"must_not_commit"});
    if (session.insertRows(insert, insertBatch) != 1U)
    {
        throw std::runtime_error("public facade insert affected-row count mismatch");
    }
    session.closeOperation(insert);
    session.rollback();

    const sessiongw::RowLocation updateLocation = scanOne();
    session.setAutocommit(false);
    sessiongw::TableOperation update = session.openTableUpdate(
        table, {"KEYTXT", "NAME"}, 1U, metadata.schema);
    const std::vector<std::uint8_t> updateBatch =
        nativeStringRowsBatch({"public_facade_update"}, {"must_not_commit"});
    if (session.updateRows(update, std::span<const sessiongw::RowLocation>(&updateLocation, 1U), updateBatch) != 1U)
    {
        throw std::runtime_error("public facade update affected-row count mismatch");
    }
    session.closeOperation(update);
    session.rollback();

    const sessiongw::RowLocation deleteLocation = scanOne();
    session.setAutocommit(false);
    sessiongw::TableOperation deletion = session.openTableDelete(table, 1U);
    if (session.deleteRows(deletion, std::span<const sessiongw::RowLocation>(&deleteLocation, 1U)) != 1U)
    {
        throw std::runtime_error("public facade delete affected-row count mismatch");
    }
    session.closeOperation(deletion);
    session.rollback();
    session.close();
}
#endif

void expectMalformedCarrierRejected(const Options& options,
                                    const std::span<const std::uint8_t> binary,
                                    const std::string_view text)
{
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
    connection.enterSessionGateway();
    connection.sendFrame(sessiongw::MessageType::hello, 700U);
    const sessiongw::Frame hello = connection.receiveFrame();
    if (hello.header.message_type != sessiongw::MessageType::hello_ok)
    {
        throw std::runtime_error("malformed-carrier probe did not negotiate Hello");
    }

    if (!binary.empty())
    {
        connection.sendRawBinaryMessageForTest(binary);
    }
    else
    {
        connection.sendRawTextMessageForTest(text);
    }
    const sessiongw::Frame error = connection.receiveFrame();
    if (error.header.message_type != sessiongw::MessageType::error || error.header.request_id != 0U)
    {
        throw std::runtime_error("malformed WebSocket carrier message did not return uncorrelated Error");
    }
    std::size_t offset = 0;
    if (decodeErrorCategory(readU16(error.payload, offset)) != sessiongw::ErrorCategory::protocol_error)
    {
        throw std::runtime_error("malformed WebSocket carrier message did not return protocol_error");
    }
    connection.close();
}

void strictWebSocketMessageBoundaryStep(const Options& options)
{
    {
        sessiongw::WebSocketConnection rejected =
            sessiongw::WebSocketConnection::connectAndLogin(options.websocket);
        const std::string response = rejected.sendJsonCommand(
            R"({"command":"enterSessionGateway","protocolVersion":2})");
        if (jsonStringValueForKey(response, "status") != "error" ||
            response.find("Unsupported Session Gateway protocol version") == std::string::npos)
        {
            throw std::runtime_error("unsupported SessionGW version did not return a structured command error: " +
                                     response);
        }
        bool disconnected = false;
        try
        {
            (void)rejected.sendJsonCommand(R"({"command":"getAttributes"})");
        }
        catch (const sessiongw::Error& error)
        {
            disconnected = error.category() == sessiongw::ErrorCategory::transport_error;
        }
        if (!disconnected)
        {
            throw std::runtime_error(
                "TLS write after rejected SessionGW upgrade did not return transport_error");
        }
        rejected.close();
    }

    sessiongw::Frame first;
    first.header.message_type = sessiongw::MessageType::ping;
    first.header.request_id = 701U;
    sessiongw::Frame second = first;
    second.header.request_id = 702U;
    std::vector<std::uint8_t> concatenated = sessiongw::encodeFrame(first);
    const std::vector<std::uint8_t> secondBytes = sessiongw::encodeFrame(second);
    concatenated.insert(concatenated.end(), secondBytes.begin(), secondBytes.end());
    expectMalformedCarrierRejected(options, concatenated, {});

    std::vector<std::uint8_t> truncated = sessiongw::encodeFrame(first);
    truncated.pop_back();
    expectMalformedCarrierRejected(options, truncated, {});

    std::vector<std::uint8_t> oversized = sessiongw::encodeFrame(first);
    oversized[20] = 0x00U;
    oversized[21] = 0x10U;
    oversized[22] = 0x00U;
    oversized[23] = 0x01U; // 1 MiB carrier limit plus one byte.
    expectMalformedCarrierRejected(options, oversized, {});

    expectMalformedCarrierRejected(options, {}, "{\"command\":\"ping\"}");

    // Each rejection closes its upgraded connection; a new authenticated session remains healthy.
    pingStep(options);
}

DescribeSummary metadataStep(const Options& options)
{
    GatewaySession session(options.websocket);
    DescribeSummary describe = describeTable(session, options);
    const sessiongw::Frame version = session.request(sessiongw::MessageType::get_table_version,
                                                     tableNamePayload(options.schema, options.table),
                                                     sessiongw::MessageType::get_table_version_result);
    std::size_t offset = 0;
    const std::string schema = readString16(version.payload, offset);
    const std::string table = readString16(version.payload, offset);
    const std::string tableVersion = readString16(version.payload, offset);
    if (schema != describe.schema || table != describe.table || tableVersion != describe.version)
    {
        throw std::runtime_error("GetTableVersion mismatch");
    }
    session.close();
    return describe;
}

void expectDescribeError(const Options& options,
                         const sessiongw::WebSocketOptions& websocket,
                         const std::string& tableName,
                         const sessiongw::ErrorCategory expected)
{
    try
    {
        GatewaySession session(websocket);
        (void)describeNamedTable(session, options, tableName);
        throw std::runtime_error("DescribeTable unexpectedly succeeded for " + tableName);
    }
    catch (const sessiongw::Error& error)
    {
        if (error.category() != expected)
        {
            throw std::runtime_error("DescribeTable for " + tableName + " returned " +
                                     std::string(sessiongw::toString(error.category())) +
                                     " instead of " + std::string(sessiongw::toString(expected)) +
                                     ": " + error.what());
        }
    }
}

void metadataErrorsAndDdlVersionStep(const Options& options)
{
    expectDescribeError(options,
                        options.websocket,
                        "SESSIONGW_MISSING_METADATA_TABLE",
                        sessiongw::ErrorCategory::object_not_found);

    sessiongw::WebSocketOptions unauthorized = options.websocket;
    unauthorized.user = "SGW_ACL_NONE";
    unauthorized.password = options.aclPassword;
    expectDescribeError(options,
                        unauthorized,
                        options.table,
                        sessiongw::ErrorCategory::not_authorized);

    expectDescribeError(options,
                        options.websocket,
                        "METADATA_UNSUPPORTED_TYPE",
                        sessiongw::ErrorCategory::unsupported_type);

    constexpr std::string_view versionTable = "METADATA_VERSION_PROBE";
    SqlControlSession sql(options.websocket);
    sql.execute("create or replace table " + options.schema + "." + std::string(versionTable) +
                " (ID decimal(18,0) not null)");

    GatewaySession beforeSession(options.websocket);
    const DescribeSummary before = describeNamedTable(beforeSession, options, std::string(versionTable));
    beforeSession.close();

    sql.execute("alter table " + options.schema + "." + std::string(versionTable) +
                " add column NAME varchar(40)");

    GatewaySession afterSession(options.websocket);
    const DescribeSummary after = describeNamedTable(afterSession, options, std::string(versionTable));
    afterSession.close();
    if (before.version == after.version || before.arrowSchema == after.arrowSchema)
    {
        throw std::runtime_error("DescribeTable DDL change mismatch: version_before=" + before.version +
                                 " version_after=" + after.version +
                                 " schema_changed=" +
                                 std::string(before.arrowSchema == after.arrowSchema ? "false" : "true"));
    }
    sql.execute("drop table " + options.schema + "." + std::string(versionTable));
}

void pushedQueryStep(const Options& options)
{
    GatewaySession session(options.websocket);
    const auto open = session.request(sessiongw::MessageType::open_pushed_query,
                                      openPushedQueryPayload("select cast(1 as integer) as id union all select cast(2 as integer) as id order by id"),
                                      sessiongw::MessageType::open_cursor_result);
    const std::uint64_t cursorId = parseOpenCursorId(open);
    const FetchSummary fetch = parseFetchResult(session.request(sessiongw::MessageType::fetch,
                                                                fetchPayload(cursorId, 1),
                                                                sessiongw::MessageType::fetch_result));
    if (fetch.arrowBatchBytes == 0 || fetch.endOfCursor)
    {
        throw std::runtime_error("expected first pushed-query fetch to return a non-final Arrow batch");
    }
    session.request(sessiongw::MessageType::close_cursor,
                    closeCursorPayload(cursorId),
                    sessiongw::MessageType::ok);
    session.close();
}

std::vector<RowHandle> scanTableHandles(GatewaySession& session,
                                        const Options& options,
                                        const std::string& tableName,
                                        const std::vector<std::string>& columns,
                                        const std::uint32_t minHandles,
                                        const std::uint32_t fetchRows = 100)
{
    const auto open = session.request(sessiongw::MessageType::open_table_scan,
                                      openTableScanPayload(options.schema, tableName, columns, true),
                                      sessiongw::MessageType::open_cursor_result);
    const std::uint64_t cursorId = parseOpenCursorId(open);
    std::vector<RowHandle> handles;
    bool sawArrowBatch = false;
    for (;;)
    {
        const FetchSummary fetch = parseFetchResult(session.request(sessiongw::MessageType::fetch,
                                                                    fetchPayload(cursorId, fetchRows),
                                                                    sessiongw::MessageType::fetch_result));
        sawArrowBatch = sawArrowBatch || fetch.arrowBatchBytes != 0U;
        handles.insert(handles.end(), fetch.rowHandles.begin(), fetch.rowHandles.end());
        if (fetch.endOfCursor)
        {
            break;
        }
        if (fetch.rowHandles.empty() && fetch.arrowBatchBytes == 0U)
        {
            throw std::runtime_error("scan made no progress before end-of-cursor");
        }
    }
    if (!sawArrowBatch || handles.size() < minHandles)
    {
        throw std::runtime_error("expected scan to return Arrow bytes and row handles");
    }
    session.request(sessiongw::MessageType::close_cursor,
                    closeCursorPayload(cursorId),
                    sessiongw::MessageType::ok);
    return handles;
}

std::vector<RowHandle> scanTableHandles(const Options& options,
                                        const std::string& tableName,
                                        const std::vector<std::string>& columns,
                                        const std::uint32_t minHandles,
                                        const std::uint32_t fetchRows = 100)
{
    GatewaySession session(options.websocket);
    std::vector<RowHandle> handles = scanTableHandles(session, options, tableName, columns, minHandles, fetchRows);
    session.close();
    return handles;
}

std::vector<RowHandle> scanHandlesStep(const Options& options, const std::uint32_t minHandles)
{
    return scanTableHandles(options, options.table, {"KEYTXT", "NAME"}, minHandles);
}

/// Proves both maxBytes failure rollback and short-prefix continuation preserve scan order.
void sequentialRowHandleByteLimitContinuationStep(const Options& options)
{
    const std::vector<RowHandle> expected =
        scanTableHandles(options, options.table, {"KEYTXT", "NAME"}, 3U);
    GatewaySession session(options.websocket);
    const std::uint64_t cursorId = parseOpenCursorId(session.request(
        sessiongw::MessageType::open_table_scan,
        openTableScanPayload(options.schema, options.table, {"KEYTXT", "NAME"}, true),
        sessiongw::MessageType::open_cursor_result));
    const std::string byteLimitError = expectSessionError(
        session,
        sessiongw::MessageType::fetch,
        fetchPayload(cursorId, 3U, 1U),
        sessiongw::MessageType::fetch_result);
    if (byteLimitError.empty())
    {
        throw std::runtime_error("ordinary row-handle fetch ignored a restrictive maxBytes");
    }

    // One seed row occupies 18 table-read result bytes (payload plus null/size metadata).
    // This exact limit admits one row, exercising collective short-prefix commit.
    const FetchSummary prefix = parseFetchResult(session.request(
        sessiongw::MessageType::fetch,
        fetchPayload(cursorId, 3U, 18U),
        sessiongw::MessageType::fetch_result));
    if (prefix.endOfCursor || prefix.arrowBatchBytes == 0U || prefix.rowHandles.empty() ||
        prefix.rowHandles.size() >= expected.size())
    {
        throw std::runtime_error("ordinary maxBytes fetch did not return a valid short prefix");
    }

    std::vector<RowHandle> handles = prefix.rowHandles;
    for (;;)
    {
        const FetchSummary fetch = parseFetchResult(session.request(
            sessiongw::MessageType::fetch,
            fetchPayload(cursorId, 1U),
            sessiongw::MessageType::fetch_result));
        if (fetch.arrowBatchBytes != 0U && fetch.rowHandles.size() != 1U)
        {
            throw std::runtime_error("ordinary row-handle continuation returned a misaligned batch");
        }
        handles.insert(handles.end(), fetch.rowHandles.begin(), fetch.rowHandles.end());
        if (fetch.endOfCursor)
        {
            break;
        }
    }
    if (handles.size() != expected.size() ||
        !std::equal(handles.begin(), handles.end(), expected.begin(),
                    [](const RowHandle& actual, const RowHandle& reference) {
                        return actual.rowNumber == reference.rowNumber;
                    }))
    {
        throw std::runtime_error("ordinary short-prefix continuation skipped, reordered, or duplicated handles");
    }
    session.request(sessiongw::MessageType::close_cursor,
                    closeCursorPayload(cursorId),
                    sessiongw::MessageType::ok);
    session.close();
}

/// Repeats size-one out-of-band reads and proves the forward scan continues independently.
void positionedFetchContinuationStep(const Options& options)
{
    GatewaySession session(options.websocket);
    const auto open = session.request(sessiongw::MessageType::open_table_scan,
                                      openTableScanPayload(options.schema, options.table,
                                                            {"KEYTXT", "NAME"}, true),
                                      sessiongw::MessageType::open_cursor_result);
    const std::uint64_t cursorId = parseOpenCursorId(open);
    const FetchSummary first = parseFetchResult(session.request(sessiongw::MessageType::fetch,
                                                                 fetchPayload(cursorId, 1),
                                                                 sessiongw::MessageType::fetch_result));
    if (first.endOfCursor || first.arrowBatchBytes == 0U || first.rowHandles.size() != 1U)
    {
        throw std::runtime_error("expected the first sequential table-scan row and handle");
    }

    const std::span<const RowHandle> firstHandle(first.rowHandles.data(), 1U);
    for (int attempt = 0; attempt < 2; ++attempt)
    {
        const FetchSummary positioned = parseFetchResult(session.request(
            sessiongw::MessageType::fetch_positioned_rows,
            fetchPositionedRowsPayload(cursorId, firstHandle),
            sessiongw::MessageType::fetch_result));
        if (positioned.endOfCursor || positioned.arrowBatchBytes == 0U ||
            positioned.rowHandles.size() != 1U ||
            positioned.rowHandles[0].rowNumber != first.rowHandles[0].rowNumber)
        {
            throw std::runtime_error("positioned fetch did not return the requested size-one handle");
        }
    }

    const std::string byteLimitError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(cursorId, firstHandle, 1U),
        sessiongw::MessageType::fetch_result);
    if (byteLimitError.empty())
    {
        throw std::runtime_error("positioned fetch ignored a restrictive nonzero maxBytes");
    }

    const FetchSummary continuation = parseFetchResult(session.request(
        sessiongw::MessageType::fetch, fetchPayload(cursorId, 1), sessiongw::MessageType::fetch_result));
    if (continuation.arrowBatchBytes == 0U || continuation.rowHandles.size() != 1U ||
        continuation.rowHandles[0].rowNumber == first.rowHandles[0].rowNumber)
    {
        throw std::runtime_error("positioned fetch advanced or corrupted the sequential table scan");
    }

    session.request(sessiongw::MessageType::commit, {}, sessiongw::MessageType::ok);
    const std::string staleHandleError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(cursorId, firstHandle),
        sessiongw::MessageType::fetch_result);
    const std::string staleCursorError = expectSessionError(
        session,
        sessiongw::MessageType::close_cursor,
        closeCursorPayload(cursorId),
        sessiongw::MessageType::ok);
    session.close();
    if (staleHandleError.empty() || staleCursorError.find("CURSOR_NOT_FOUND") == std::string::npos)
    {
        throw std::runtime_error("post-commit cursor or positioned handle remained usable");
    }
}

/// Verifies that the dedicated positioned API enforces transaction and table provenance.
void rowHandleProvenanceStep(const Options& options)
{
    GatewaySession session(options.websocket);
    const auto openScan = [&](const std::string& tableName) {
        return parseOpenCursorId(session.request(sessiongw::MessageType::open_table_scan,
                                                 openTableScanPayload(options.schema,
                                                                      tableName,
                                                                      {"KEYTXT"},
                                                                      false),
                                                 sessiongw::MessageType::open_cursor_result));
    };

    std::vector<RowHandle> handles = scanTableHandles(session, options, options.table,
                                                       {"KEYTXT", "NAME"}, 3U);
    const std::uint64_t tableCursor = openScan(options.table);
    session.request(sessiongw::MessageType::commit, {}, sessiongw::MessageType::ok);
    const std::string postCommitError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(tableCursor, std::span<const RowHandle>(&handles.at(0), 1U)),
        sessiongw::MessageType::fetch_result);
    if (postCommitError.empty())
    {
        throw std::runtime_error("post-commit row handle was not rejected");
    }
    const std::string postCommitCloseError = expectSessionError(
        session,
        sessiongw::MessageType::close_cursor,
        closeCursorPayload(tableCursor),
        sessiongw::MessageType::ok);
    if (postCommitCloseError.find("CURSOR_NOT_FOUND") == std::string::npos)
    {
        throw std::runtime_error("post-commit cursor id remained open");
    }

    handles = scanTableHandles(session, options, options.table, {"KEYTXT", "NAME"}, 3U);
    const std::uint64_t typedCursor = openScan(options.typedTable);
    const std::string crossTableError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(typedCursor, std::span<const RowHandle>(&handles.at(0), 1U)),
        sessiongw::MessageType::fetch_result);
    if (crossTableError.empty())
    {
        throw std::runtime_error("cross-table row handle was not rejected");
    }
    session.request(sessiongw::MessageType::close_cursor,
                    closeCursorPayload(typedCursor),
                    sessiongw::MessageType::ok);

    const std::uint64_t validationCursor = openScan(options.table);
    const RowHandle unknown{std::numeric_limits<std::uint64_t>::max()};
    const std::string unknownError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(validationCursor, std::span<const RowHandle>(&unknown, 1U)),
        sessiongw::MessageType::fetch_result);
    if (unknownError.empty())
    {
        throw std::runtime_error("unknown row handle was not rejected");
    }

    const std::span<const RowHandle> firstThreeHandles(handles.data(), 3U);
    const std::string limitedMultiError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(validationCursor, firstThreeHandles, 1U),
        sessiongw::MessageType::fetch_result);
    if (limitedMultiError.empty())
    {
        throw std::runtime_error("partial multi-handle positioned result was not rejected");
    }

    session.request(sessiongw::MessageType::set_autocommit,
                    setAutocommitPayload(false),
                    sessiongw::MessageType::ok);
    const std::uint64_t deleteOperationId = parseOpenOperationId(session.request(
        sessiongw::MessageType::open_table_delete,
        openTableDeletePayload(options.schema, options.table, 1U),
        sessiongw::MessageType::open_table_operation_result));
    const std::span<const RowHandle> deletedHandle(&handles.at(1), 1U);
    if (parseAffectedRows(session.request(sessiongw::MessageType::delete_rows,
                                          deleteRowsPayload(deleteOperationId, deletedHandle),
                                          sessiongw::MessageType::affected_rows_result)) != 1U)
    {
        throw std::runtime_error("positioned-read deleted-handle setup affected no row");
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(deleteOperationId),
                    sessiongw::MessageType::ok);
    const std::span<const RowHandle> handlesWithDeletedMiddle(handles.data(), 3U);
    const std::string deletedError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(validationCursor, handlesWithDeletedMiddle),
        sessiongw::MessageType::fetch_result);
    if (deletedError.empty())
    {
        throw std::runtime_error("positioned fetch accepted a multi-handle vector with a deleted middle row");
    }

    session.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
    const std::string postRollbackError = expectSessionError(
        session,
        sessiongw::MessageType::fetch_positioned_rows,
        fetchPositionedRowsPayload(validationCursor, std::span<const RowHandle>(&handles.at(2), 1U)),
        sessiongw::MessageType::fetch_result);
    const std::string postRollbackCloseError = expectSessionError(
        session,
        sessiongw::MessageType::close_cursor,
        closeCursorPayload(validationCursor),
        sessiongw::MessageType::ok);
    session.close();
    if (postRollbackError.empty() || postRollbackCloseError.find("CURSOR_NOT_FOUND") == std::string::npos)
    {
        throw std::runtime_error("post-rollback cursor or row handle remained usable");
    }
}

void transactionBoundaryInvalidationStep(const Options& options)
{
    const auto exerciseBoundary = [&](const sessiongw::MessageType boundary,
                                      const std::string& insertedKey) {
        GatewaySession session(options.websocket);
        const DescribeSummary describe = describeNamedTable(session, options, options.table);
        session.request(sessiongw::MessageType::set_autocommit,
                        setAutocommitPayload(false),
                        sessiongw::MessageType::ok);

        const std::uint64_t cursorId = parseOpenCursorId(session.request(
            sessiongw::MessageType::open_table_scan,
            openTableScanPayload(options.schema, options.table, {"KEYTXT", "NAME"}, true),
            sessiongw::MessageType::open_cursor_result));
        const FetchSummary fetched = parseFetchResult(session.request(
            sessiongw::MessageType::fetch,
            fetchPayload(cursorId, 1U),
            sessiongw::MessageType::fetch_result));
        if (fetched.rowHandles.size() != 1U)
        {
            throw std::runtime_error("transaction-boundary probe did not receive one row location");
        }

        const std::uint64_t operationId = parseOpenOperationId(session.request(
            sessiongw::MessageType::open_table_insert,
            openTableWritePayload(options.schema,
                                  options.table,
                                  {"KEYTXT", "NAME"},
                                  1U,
                                  describe.arrowSchema),
            sessiongw::MessageType::open_table_operation_result));
        const std::vector<std::uint8_t> insertedBatch =
            nativeStringRowsBatch({insertedKey}, {"must_clean_abort"});
        if (parseAffectedRows(session.request(sessiongw::MessageType::insert_rows,
                                              insertRowsPayload(operationId, insertedBatch),
                                              sessiongw::MessageType::affected_rows_result)) != 1U)
        {
            throw std::runtime_error("transaction-boundary probe did not accept its open-operation row");
        }

        if (boundary == sessiongw::MessageType::commit)
        {
            const std::string commitError = expectSessionError(
                session, boundary, {}, sessiongw::MessageType::ok);
            if (commitError.find("PROTOCOL_ERROR") == std::string::npos ||
                commitError.find("transaction rolled back") == std::string::npos)
            {
                throw std::runtime_error("Commit with an open operation was not rejected and rolled back: " +
                                         commitError);
            }
        }
        else
        {
            session.request(boundary, {}, sessiongw::MessageType::ok);
        }

        const std::string staleFetch = expectSessionError(
            session,
            sessiongw::MessageType::fetch,
            fetchPayload(cursorId, 1U),
            sessiongw::MessageType::fetch_result);
        const std::string stalePositioned = expectSessionError(
            session,
            sessiongw::MessageType::fetch_positioned_rows,
            fetchPositionedRowsPayload(cursorId, fetched.rowHandles),
            sessiongw::MessageType::fetch_result);
        const std::string staleCursorClose = expectSessionError(
            session,
            sessiongw::MessageType::close_cursor,
            closeCursorPayload(cursorId),
            sessiongw::MessageType::ok);
        const std::string staleWrite = expectSessionError(
            session,
            sessiongw::MessageType::insert_rows,
            insertRowsPayload(operationId, insertedBatch),
            sessiongw::MessageType::affected_rows_result);
        const std::string staleOperationClose = expectSessionError(
            session,
            sessiongw::MessageType::close_operation,
            closeOperationPayload(operationId),
            sessiongw::MessageType::ok);
        for (const std::string* error : {&staleFetch, &stalePositioned, &staleCursorClose,
                                         &staleWrite, &staleOperationClose})
        {
            if (error->find("CURSOR_NOT_FOUND") == std::string::npos)
            {
                throw std::runtime_error("transaction boundary did not reject stale cursor/operation state: " +
                                         *error);
            }
        }

        const std::uint64_t freshCursorId = parseOpenCursorId(session.request(
            sessiongw::MessageType::open_table_scan,
            openTableScanPayload(options.schema, options.table, {"KEYTXT"}, false),
            sessiongw::MessageType::open_cursor_result));
        (void)parseFetchResult(session.request(sessiongw::MessageType::fetch,
                                               fetchPayload(freshCursorId, 1U),
                                               sessiongw::MessageType::fetch_result));
        session.request(sessiongw::MessageType::close_cursor,
                        closeCursorPayload(freshCursorId),
                        sessiongw::MessageType::ok);

        if (boundary == sessiongw::MessageType::commit)
        {
            const std::uint64_t updateOperationId = parseOpenOperationId(session.request(
                sessiongw::MessageType::open_table_update,
                openTableWritePayload(options.schema,
                                      options.table,
                                      {"KEYTXT", "NAME"},
                                      1U,
                                      describe.arrowSchema),
                sessiongw::MessageType::open_table_operation_result));
            const std::vector<std::uint8_t> updateBatch =
                nativeStringRowsBatch({"stale_commit"}, {"must_not_apply"});
            const std::string staleRowLocation = expectSessionError(
                session,
                sessiongw::MessageType::update_rows,
                updateRowsPayload(updateOperationId, fetched.rowHandles, updateBatch),
                sessiongw::MessageType::affected_rows_result);
            if (staleRowLocation.empty())
            {
                throw std::runtime_error("post-commit update accepted an old row location");
            }
        }
        else
        {
            const std::uint64_t deleteOperationId = parseOpenOperationId(session.request(
                sessiongw::MessageType::open_table_delete,
                openTableDeletePayload(options.schema, options.table, 1U),
                sessiongw::MessageType::open_table_operation_result));
            const std::string staleRowLocation = expectSessionError(
                session,
                sessiongw::MessageType::delete_rows,
                deleteRowsPayload(deleteOperationId, fetched.rowHandles),
                sessiongw::MessageType::affected_rows_result);
            if (staleRowLocation.empty())
            {
                throw std::runtime_error("post-rollback delete accepted an old row location");
            }
        }

        session.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
        session.close();
    };

    exerciseBoundary(sessiongw::MessageType::commit, "boundary_commit_prefix");
    exerciseBoundary(sessiongw::MessageType::rollback, "boundary_rollback_prefix");
}

void directConstraintEnforcementStep(const Options& options)
{
    constexpr std::string_view tableName = "CONSTRAINT_GUARD";

    GatewaySession insertSession(options.websocket);
    const DescribeSummary insertDescribe =
        describeNamedTable(insertSession, options, std::string(tableName));
    insertSession.request(sessiongw::MessageType::set_autocommit,
                          setAutocommitPayload(false),
                          sessiongw::MessageType::ok);
    const std::uint64_t insertOperationId = parseOpenOperationId(insertSession.request(
        sessiongw::MessageType::open_table_insert,
        openTableWritePayload(options.schema,
                              std::string(tableName),
                              {"KEYTXT", "NAME"},
                              2U,
                              insertDescribe.arrowSchema),
        sessiongw::MessageType::open_table_operation_result));
    const std::vector<std::uint8_t> acceptedInsert =
        nativeStringRowsBatch({"prefix"}, {"must_rollback"});
    if (parseAffectedRows(insertSession.request(sessiongw::MessageType::insert_rows,
                                                insertRowsPayload(insertOperationId, acceptedInsert),
                                                sessiongw::MessageType::affected_rows_result)) != 1U)
    {
        throw std::runtime_error("constraint probe insert prefix was not accepted");
    }
    const std::vector<std::uint8_t> rejectedInsert = nativeNullableStringRowsBatch(
        {std::string("mixed_valid"), std::nullopt},
        {std::string("mixed_valid"), std::string("invalid_null_key")});
    const std::string insertError = expectSessionError(
        insertSession,
        sessiongw::MessageType::insert_rows,
        insertRowsPayload(insertOperationId, rejectedInsert),
        sessiongw::MessageType::affected_rows_result);
    if (insertError.find("CONSTRAINT_VIOLATION") == std::string::npos)
    {
        throw std::runtime_error("NOT NULL InsertRows did not return constraint_violation: " + insertError);
    }
    insertSession.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
    insertSession.close();

    GatewaySession updateSession(options.websocket);
    const DescribeSummary updateDescribe =
        describeNamedTable(updateSession, options, std::string(tableName));
    updateSession.request(sessiongw::MessageType::set_autocommit,
                          setAutocommitPayload(false),
                          sessiongw::MessageType::ok);
    const std::vector<RowHandle> handles = scanTableHandles(
        updateSession, options, std::string(tableName), {"KEYTXT", "NAME"}, 2U);
    const std::uint64_t updateOperationId = parseOpenOperationId(updateSession.request(
        sessiongw::MessageType::open_table_update,
        openTableWritePayload(options.schema,
                              std::string(tableName),
                              {"KEYTXT", "NAME"},
                              1U,
                              updateDescribe.arrowSchema),
        sessiongw::MessageType::open_table_operation_result));
    const std::vector<std::uint8_t> acceptedUpdate =
        nativeStringRowsBatch({"seed_1"}, {"must_rollback"});
    if (parseAffectedRows(updateSession.request(
            sessiongw::MessageType::update_rows,
            updateRowsPayload(updateOperationId,
                              std::span<const RowHandle>(&handles.at(0), 1U),
                              acceptedUpdate),
            sessiongw::MessageType::affected_rows_result)) != 1U)
    {
        throw std::runtime_error("constraint probe update prefix was not accepted");
    }
    const std::vector<std::uint8_t> rejectedUpdate = nativeNullableStringRowsBatch(
        {std::string("seed_2")}, {std::nullopt});
    const std::string updateError = expectSessionError(
        updateSession,
        sessiongw::MessageType::update_rows,
        updateRowsPayload(updateOperationId,
                          std::span<const RowHandle>(&handles.at(1), 1U),
                          rejectedUpdate),
        sessiongw::MessageType::affected_rows_result);
    if (updateError.find("CONSTRAINT_VIOLATION") == std::string::npos)
    {
        throw std::runtime_error("NOT NULL UpdateRows did not return constraint_violation: " + updateError);
    }
    updateSession.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
    updateSession.close();

    GatewaySession unsupportedSession(options.websocket);
    const DescribeSummary unsupportedDescribe =
        describeNamedTable(unsupportedSession, options, "CONSTRAINT_UNSUPPORTED");
    const std::string unsupportedError = expectSessionError(
        unsupportedSession,
        sessiongw::MessageType::open_table_insert,
        openTableWritePayload(options.schema,
                              "CONSTRAINT_UNSUPPORTED",
                              {"KEYTXT", "NAME"},
                              1U,
                              unsupportedDescribe.arrowSchema),
        sessiongw::MessageType::open_table_operation_result);
    unsupportedSession.close();
    if (unsupportedError.find("UNSUPPORTED_OPERATION") == std::string::npos)
    {
        throw std::runtime_error("constrained table direct write was not rejected: " + unsupportedError);
    }
}

std::string lifetimeIdExhaustionStep(const Options& options, const std::uint32_t opens)
{
    if (opens <= static_cast<std::uint32_t>(std::numeric_limits<std::int16_t>::max()))
    {
        throw std::runtime_error("lifetime id test must cross the signed 16-bit boundary");
    }

    GatewaySession session(options.websocket);
    const DescribeSummary describe = describeNamedTable(session, options, options.table);
    session.request(sessiongw::MessageType::set_autocommit,
                    setAutocommitPayload(false),
                    sessiongw::MessageType::ok);

    std::uint64_t firstCursorId = 0;
    std::uint64_t previousCursorId = 0;
    for (std::uint32_t index = 0; index < opens; ++index)
    {
        const std::uint64_t cursorId = parseOpenCursorId(session.request(
            sessiongw::MessageType::open_pushed_query,
            openPushedQueryPayload("SELECT 1"),
            sessiongw::MessageType::open_cursor_result));
        if (index == 0U)
        {
            firstCursorId = cursorId;
        }
        if (cursorId <= previousCursorId)
        {
            throw std::runtime_error("cursor ids were reused or did not increase");
        }
        previousCursorId = cursorId;
        session.request(sessiongw::MessageType::close_cursor,
                        closeCursorPayload(cursorId),
                        sessiongw::MessageType::ok);
    }

    const std::uint64_t scanCursorId = parseOpenCursorId(session.request(
        sessiongw::MessageType::open_table_scan,
        openTableScanPayload(options.schema, options.table, {"KEYTXT"}, false),
        sessiongw::MessageType::open_cursor_result));
    if (scanCursorId <= previousCursorId)
    {
        throw std::runtime_error("table-scan cursor did not retain the 64-bit cursor sequence");
    }
    (void)parseFetchResult(session.request(sessiongw::MessageType::fetch,
                                           fetchPayload(scanCursorId, 1U),
                                           sessiongw::MessageType::fetch_result));
    session.request(sessiongw::MessageType::close_cursor,
                    closeCursorPayload(scanCursorId),
                    sessiongw::MessageType::ok);

    std::uint64_t firstOperationId = 0;
    std::uint64_t previousOperationId = 0;
    for (std::uint32_t index = 0; index < opens; ++index)
    {
        const std::uint64_t operationId = parseOpenOperationId(session.request(
            sessiongw::MessageType::open_table_insert,
            openTableWritePayload(options.schema,
                                  options.table,
                                  {"KEYTXT", "NAME"},
                                  1U,
                                  describe.arrowSchema),
            sessiongw::MessageType::open_table_operation_result));
        if (index == 0U)
        {
            firstOperationId = operationId;
        }
        if (operationId <= previousOperationId)
        {
            throw std::runtime_error("table operation ids were reused or did not increase");
        }
        previousOperationId = operationId;
        session.request(sessiongw::MessageType::close_operation,
                        closeOperationPayload(operationId),
                        sessiongw::MessageType::ok);
    }

    const std::string staleCursorError = expectSessionError(
        session,
        sessiongw::MessageType::close_cursor,
        closeCursorPayload(firstCursorId),
        sessiongw::MessageType::ok);
    const std::string staleOperationError = expectSessionError(
        session,
        sessiongw::MessageType::close_operation,
        closeOperationPayload(firstOperationId),
        sessiongw::MessageType::ok);
    if (staleCursorError.find("CURSOR_NOT_FOUND") == std::string::npos ||
        staleOperationError.find("CURSOR_NOT_FOUND") == std::string::npos)
    {
        throw std::runtime_error("stale lifetime ids were not rejected deterministically");
    }

    session.request(sessiongw::MessageType::rollback, {}, sessiongw::MessageType::ok);
    session.close();
    return "opens=" + std::to_string(opens) +
           " final_cursor_id=" + std::to_string(scanCursorId) +
           " final_operation_id=" + std::to_string(previousOperationId) +
           " stale_ids_rejected";
}

void insertRowsStep(const Options& options,
                    const std::span<const std::uint8_t> arrowSchema,
                    const std::vector<std::string>& keys,
                    const std::vector<std::string>& names,
                    const std::optional<bool> autocommit = std::nullopt,
                    const bool commit = false,
                    const bool rollback = false)
{
    GatewaySession session(options.websocket);
    if (autocommit.has_value())
    {
        session.request(sessiongw::MessageType::set_autocommit,
                        setAutocommitPayload(*autocommit),
                        sessiongw::MessageType::ok);
    }
    const auto open = session.request(sessiongw::MessageType::open_table_insert,
                                      openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"},
                                                            std::max<std::uint32_t>(1U, keys.size()), arrowSchema),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    const std::vector<std::uint8_t> nativeBatch = nativeStringRowsBatch(keys, names);
    const std::uint64_t affected = parseAffectedRows(session.request(sessiongw::MessageType::insert_rows,
                                                                     insertRowsPayload(operationId, nativeBatch),
                                                                     sessiongw::MessageType::affected_rows_result));
    if (affected != keys.size())
    {
        throw std::runtime_error("insert affected row mismatch");
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
    if (commit || rollback)
    {
        session.request(commit ? sessiongw::MessageType::commit : sessiongw::MessageType::rollback,
                        {},
                        sessiongw::MessageType::ok);
    }
    session.close();
}

RowHandle updateRowStep(const Options& options,
                        const std::span<const std::uint8_t> arrowSchema,
                        const std::string& newKey,
                        const std::string& newName)
{
    GatewaySession session(options.websocket);
    const RowHandle rowHandle = scanTableHandles(session, options, options.table,
                                                 {"KEYTXT", "NAME"}, 1U).at(0);
    const auto open = session.request(sessiongw::MessageType::open_table_update,
                                      openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"}, 1, arrowSchema),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    const std::vector<std::uint8_t> nativeBatch = nativeStringRowsBatch({newKey}, {newName});
    const std::uint64_t affected = parseAffectedRows(session.request(sessiongw::MessageType::update_rows,
                                                                     updateRowsPayload(operationId,
                                                                                       std::span<const RowHandle>(&rowHandle, 1),
                                                                                       nativeBatch),
                                                                     sessiongw::MessageType::affected_rows_result));
    if (affected != 1U)
    {
        throw std::runtime_error("update affected row mismatch");
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
    session.close();
    return rowHandle;
}

RowHandle deleteRowStep(const Options& options)
{
    GatewaySession session(options.websocket);
    const RowHandle rowHandle = scanTableHandles(session, options, options.table,
                                                 {"KEYTXT", "NAME"}, 2U).at(1);
    const auto open = session.request(sessiongw::MessageType::open_table_delete,
                                      openTableDeletePayload(options.schema, options.table, 1),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    const std::uint64_t affected = parseAffectedRows(session.request(sessiongw::MessageType::delete_rows,
                                                                     deleteRowsPayload(operationId,
                                                                                       std::span<const RowHandle>(&rowHandle, 1)),
                                                                     sessiongw::MessageType::affected_rows_result));
    if (affected != 1U)
    {
        throw std::runtime_error("delete affected row mismatch");
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
    session.close();
    return rowHandle;
}

#ifndef SESSIONGW_TYPED_FACADE
void insertNativeBatchIntoTable(const Options& options,
                                const std::string& tableName,
                                const std::vector<std::string>& columns,
                                const std::span<const std::uint8_t> arrowSchema,
                                const std::span<const std::uint8_t> nativeBatch,
                                const std::uint32_t maxRowsPerBatch)
{
    GatewaySession session(options.websocket);
    const auto open = session.request(sessiongw::MessageType::open_table_insert,
                                      openTableWritePayload(options.schema, tableName, columns,
                                                            maxRowsPerBatch, arrowSchema),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    const std::uint64_t affected = parseAffectedRows(session.request(sessiongw::MessageType::insert_rows,
                                                                     insertRowsPayload(operationId, nativeBatch),
                                                                     sessiongw::MessageType::affected_rows_result));
    if (affected == 0U)
    {
        throw std::runtime_error("typed/large insert affected no rows");
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
    session.close();
}
#endif

std::vector<std::string> largeKeys(const std::uint32_t firstId, const std::uint32_t count)
{
    std::vector<std::string> keys;
    keys.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index)
    {
        keys.push_back("lk" + std::to_string(firstId + index));
    }
    return keys;
}

std::vector<std::string> largePayloads(const std::uint32_t firstId,
                                       const std::uint32_t count,
                                       const bool updated)
{
    std::vector<std::string> names;
    names.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index)
    {
        const std::uint32_t id = firstId + index;
        names.push_back(updated ? "large_updated_" + std::to_string(id)
                                : "large_payload_" + std::to_string(id % 97U));
    }
    return names;
}

void insertStringRowsBatchedIntoTable(const Options& options,
                                      const std::string& tableName,
                                      const std::span<const std::uint8_t> arrowSchema,
                                      const std::uint32_t rows,
                                      const std::uint32_t batchRows)
{
    GatewaySession session(options.websocket);
    const auto open = session.request(sessiongw::MessageType::open_table_insert,
                                      openTableWritePayload(options.schema, tableName, {"KEYTXT", "NAME"},
                                                            batchRows, arrowSchema),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    for (std::uint32_t inserted = 0; inserted < rows;)
    {
        const std::uint32_t current = std::min<std::uint32_t>(batchRows, rows - inserted);
        const std::uint32_t firstId = inserted + 1U;
        const std::vector<std::uint8_t> nativeBatch = nativeStringRowsBatch(largeKeys(firstId, current),
                                                                            largePayloads(firstId, current, false));
        const std::uint64_t affected = parseAffectedRows(session.request(sessiongw::MessageType::insert_rows,
                                                                         insertRowsPayload(operationId, nativeBatch),
                                                                         sessiongw::MessageType::affected_rows_result));
        if (affected != current)
        {
            throw std::runtime_error("large batched insert affected row mismatch");
        }
        inserted += current;
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
    session.close();
}

void updateStringRowsBatched(GatewaySession& session,
                             const Options& options,
                             const std::span<const std::uint8_t> arrowSchema,
                             const std::span<const RowHandle> handles,
                             const std::uint32_t firstId,
                             const std::uint32_t rows,
                             const std::uint32_t batchRows)
{
    const auto open = session.request(sessiongw::MessageType::open_table_update,
                                      openTableWritePayload(options.schema, options.largeTable, {"KEYTXT", "NAME"},
                                                            batchRows, arrowSchema),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    for (std::uint32_t updated = 0; updated < rows;)
    {
        const std::uint32_t current = std::min<std::uint32_t>(batchRows, rows - updated);
        const std::uint32_t id = firstId + updated;
        const std::vector<std::uint8_t> nativeBatch = nativeStringRowsBatch(largeKeys(id, current),
                                                                            largePayloads(id, current, true));
        const std::uint64_t affected = parseAffectedRows(session.request(
            sessiongw::MessageType::update_rows,
            updateRowsPayload(operationId, handles.subspan(updated, current), nativeBatch),
            sessiongw::MessageType::affected_rows_result));
        if (affected != current)
        {
            throw std::runtime_error("large batched update affected row mismatch");
        }
        updated += current;
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
}

void deleteRowsBatched(GatewaySession& session,
                       const Options& options,
                       const std::span<const RowHandle> handles,
                       const std::uint32_t rows,
                       const std::uint32_t batchRows)
{
    const auto open = session.request(sessiongw::MessageType::open_table_delete,
                                      openTableDeletePayload(options.schema, options.largeTable, batchRows),
                                      sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t operationId = parseOpenOperationId(open);
    for (std::uint32_t deleted = 0; deleted < rows;)
    {
        const std::uint32_t current = std::min<std::uint32_t>(batchRows, rows - deleted);
        const std::uint64_t affected = parseAffectedRows(session.request(
            sessiongw::MessageType::delete_rows,
            deleteRowsPayload(operationId, handles.subspan(deleted, current)),
            sessiongw::MessageType::affected_rows_result));
        if (affected != current)
        {
            throw std::runtime_error("large batched delete affected row mismatch");
        }
        deleted += current;
    }
    const std::string consumedError = expectSessionError(
        session,
        sessiongw::MessageType::delete_rows,
        deleteRowsPayload(operationId, handles.first(1U)),
        sessiongw::MessageType::affected_rows_result);
    if (consumedError.empty())
    {
        throw std::runtime_error("consumed row handle was not rejected");
    }
    session.request(sessiongw::MessageType::close_operation,
                    closeOperationPayload(operationId),
                    sessiongw::MessageType::ok);
}

void transportAbortCleanupStep(const Options& options, const std::span<const std::uint8_t> arrowSchema)
{
    {
        GatewaySession cursorAbortSession(options.websocket);
        const auto open = cursorAbortSession.request(
            sessiongw::MessageType::open_pushed_query,
            openPushedQueryPayload("select cast(1 as integer) as id union all select cast(2 as integer) as id"),
            sessiongw::MessageType::open_cursor_result);
        (void)parseOpenCursorId(open);
        // Deliberately close the WebSocket transport without SessionGW Close or
        // CloseCursor. Server-side transport cleanup must release the cursor.
        cursorAbortSession.dropTransportForTest();
    }

    {
        GatewaySession insertAbortSession(options.websocket);
        const auto open = insertAbortSession.request(
            sessiongw::MessageType::open_table_insert,
            openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"}, 1U, arrowSchema),
            sessiongw::MessageType::open_table_operation_result);
        const std::uint64_t operationId = parseOpenOperationId(open);
        const std::vector<std::uint8_t> batch = nativeStringRowsBatch({"abort_insert"}, {"transport_abort"});
        const std::uint64_t affected = parseAffectedRows(insertAbortSession.request(
            sessiongw::MessageType::insert_rows,
            insertRowsPayload(operationId, batch),
            sessiongw::MessageType::affected_rows_result));
        if (affected != 1U)
        {
            throw std::runtime_error("transport-abort insert affected row mismatch");
        }
        // Deliberately do not CloseOperation. Cleanup must close the DMP with
        // CLEAN semantics so the row is not committed.
        insertAbortSession.dropTransportForTest();
    }

    GatewaySession followupSession(options.websocket);
    followupSession.request(sessiongw::MessageType::ping, {}, sessiongw::MessageType::pong);
    followupSession.close();
}

void timeoutKillReconnectStep(const Options& options)
{
    {
        GatewaySession timeoutSession(options.websocket, {"alter session set query_timeout = 1"});
        bool sawTimeoutError = false;
        try
        {
            (void)timeoutSession.request(
                sessiongw::MessageType::open_pushed_query,
                openPushedQueryPayload("select \"$SLEEP\"(3)"),
                sessiongw::MessageType::open_cursor_result);
        }
        catch (const sessiongw::Error&)
        {
            sawTimeoutError = true;
        }
        if (!sawTimeoutError)
        {
            throw std::runtime_error("expected query-timeout SessionGW error");
        }
        timeoutSession.request(sessiongw::MessageType::ping, {}, sessiongw::MessageType::pong);
        timeoutSession.close();
    }

    {
        GatewaySession victim(options.websocket, {}, true);
        const std::uint64_t victimSessionId = victim.sessionId();
        if (victimSessionId == 0U)
        {
            throw std::runtime_error("failed to capture victim SessionGW session id");
        }

        std::atomic<bool> sawKillException{false};
        std::string killExceptionMessage;
        std::thread worker([&]() {
            try
            {
                (void)victim.request(
                    sessiongw::MessageType::open_pushed_query,
                    openPushedQueryPayload("select \"$SLEEP\"(30)"),
                    sessiongw::MessageType::open_cursor_result);
            }
            catch (const std::exception& ex)
            {
                sawKillException.store(true);
                killExceptionMessage = ex.what();
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        SqlControlSession killer(options.websocket);
        killer.execute("kill session " + std::to_string(victimSessionId));
        worker.join();

        if (!sawKillException.load())
        {
            throw std::runtime_error("expected killed SessionGW session to surface an SDK exception");
        }
        victim.dropTransportForTest();
    }

    GatewaySession reconnect(options.websocket);
    reconnect.request(sessiongw::MessageType::ping, {}, sessiongw::MessageType::pong);
    reconnect.close();
}

void negativeDmlLifecycleStep(const Options& options, const std::span<const std::uint8_t> arrowSchema)
{
    // Unknown operation handles must produce structured errors, not silent OKs.
    GatewaySession unknownSession(options.websocket);
    const std::string unknownCloseError = expectSessionError(unknownSession,
                                                            sessiongw::MessageType::close_operation,
                                                            closeOperationPayload(0xabcdefU),
                                                            sessiongw::MessageType::ok);
    unknownSession.close();
    if (unknownCloseError.empty())
    {
        throw std::runtime_error("missing unknown-operation error");
    }

    // InsertRows must honor maxRowsPerBatch and reject an oversized batch while
    // cleaning the failed operation on session close.
    GatewaySession oversizedInsertSession(options.websocket);
    const auto oversizedOpen = oversizedInsertSession.request(
        sessiongw::MessageType::open_table_insert,
        openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"}, 1U, arrowSchema),
        sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t oversizedOperationId = parseOpenOperationId(oversizedOpen);
    const std::vector<std::uint8_t> oversizedBatch = nativeStringRowsBatch({"neg_over_1", "neg_over_2"},
                                                                           {"bad_1", "bad_2"});
    const std::string oversizedError = expectSessionError(oversizedInsertSession,
                                                          sessiongw::MessageType::insert_rows,
                                                          insertRowsPayload(oversizedOperationId, oversizedBatch),
                                                          sessiongw::MessageType::affected_rows_result);
    oversizedInsertSession.close();
    if (oversizedError.empty())
    {
        throw std::runtime_error("missing oversized InsertRows error");
    }

    const auto rejectMalformedNativeBatch = [&](std::vector<std::uint8_t> batch,
                                                const std::string& label) {
        GatewaySession session(options.websocket);
        const auto opened = session.request(
            sessiongw::MessageType::open_table_insert,
            openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"}, 1U, arrowSchema),
            sessiongw::MessageType::open_table_operation_result);
        const std::uint64_t operationId = parseOpenOperationId(opened);
        const std::string error = expectSessionError(session,
                                                     sessiongw::MessageType::insert_rows,
                                                     insertRowsPayload(operationId, batch),
                                                     sessiongw::MessageType::affected_rows_result);
        session.close();
        if (error.empty())
        {
            throw std::runtime_error("missing malformed native-batch error: " + label);
        }
    };

    std::vector<std::uint8_t> incompatibleAbi =
        nativeStringRowsBatch({"neg_abi"}, {"must_not_apply"});
    incompatibleAbi.at(19) = 8U; // advertised alignment 16 -> 8
    rejectMalformedNativeBatch(std::move(incompatibleAbi), "ABI mismatch");

    std::vector<std::uint8_t> invalidNull =
        nativeStringRowsBatch({"neg_null_byte"}, {"must_not_apply"});
    invalidNull.at(32) = 1U; // first column null-vector byte
    rejectMalformedNativeBatch(std::move(invalidNull), "invalid null byte");

    std::vector<std::uint8_t> overflowingSize =
        nativeStringRowsBatch({"neg_size"}, {"must_not_apply"});
    std::fill(overflowingSize.begin() + 48, overflowingSize.begin() + 56, 0xffU);
    rejectMalformedNativeBatch(std::move(overflowingSize), "overflowing variable size");

    std::vector<std::uint8_t> uncoveredVariableData =
        nativeStringRowsBatch({"neg_extent"}, {"must_not_apply"});
    std::fill(uncoveredVariableData.begin() + 48, uncoveredVariableData.begin() + 56, 0U);
    rejectMalformedNativeBatch(std::move(uncoveredVariableData), "uncovered variable data");

    // A closed operation handle must not accept additional writes.
    GatewaySession insertAfterCloseSession(options.websocket);
    const auto insertAfterCloseOpen = insertAfterCloseSession.request(
        sessiongw::MessageType::open_table_insert,
        openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"}, 1U, arrowSchema),
        sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t closedOperationId = parseOpenOperationId(insertAfterCloseOpen);
    insertAfterCloseSession.request(sessiongw::MessageType::close_operation,
                                    closeOperationPayload(closedOperationId),
                                    sessiongw::MessageType::ok);
    const std::vector<std::uint8_t> afterCloseBatch = nativeStringRowsBatch({"neg_after_close"}, {"bad"});
    const std::string afterCloseError = expectSessionError(insertAfterCloseSession,
                                                           sessiongw::MessageType::insert_rows,
                                                           insertRowsPayload(closedOperationId, afterCloseBatch),
                                                           sessiongw::MessageType::affected_rows_result);
    insertAfterCloseSession.close();
    if (afterCloseError.empty())
    {
        throw std::runtime_error("missing InsertRows-after-close error");
    }

    // UpdateRows must reject a handle/value cardinality mismatch.
    GatewaySession mismatchUpdateSession(options.websocket);
    const std::vector<RowHandle> handles = scanTableHandles(mismatchUpdateSession, options, options.table,
                                                             {"KEYTXT", "NAME"}, 1U);
    const auto mismatchUpdateOpen = mismatchUpdateSession.request(
        sessiongw::MessageType::open_table_update,
        openTableWritePayload(options.schema, options.table, {"KEYTXT", "NAME"}, 2U, arrowSchema),
        sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t mismatchOperationId = parseOpenOperationId(mismatchUpdateOpen);
    const std::vector<std::uint8_t> mismatchBatch = nativeStringRowsBatch({"neg_mismatch_1", "neg_mismatch_2"},
                                                                          {"bad_1", "bad_2"});
    const std::string mismatchError = expectSessionError(
        mismatchUpdateSession,
        sessiongw::MessageType::update_rows,
        updateRowsPayload(mismatchOperationId, std::span<const RowHandle>(&handles.at(0), 1), mismatchBatch),
        sessiongw::MessageType::affected_rows_result);
    mismatchUpdateSession.close();
    if (mismatchError.empty())
    {
        throw std::runtime_error("missing mismatched UpdateRows error");
    }
}

MixedWriterSummary mixedFrontendWriterConflictStep(const Options& options)
{
    GatewaySession describeSession(options.websocket);
    const DescribeSummary describe = describeNamedTable(describeSession, options, options.conflictTable);
    describeSession.close();

    MixedWriterSummary summary;
    const std::uint32_t rounds = options.quick ? 5U : 20U;
    for (std::uint32_t round = 0; round < rounds; ++round)
    {
        std::barrier start(3);
        bool sqlCommitted = false;
        bool gatewayCommitted = false;
        bool sqlConflict = false;
        bool gatewayConflict = false;
        std::string sqlFailure;
        std::string gatewayFailure;

        std::thread sqlWriter([&]() {
            std::optional<SqlControlSession> sqlSession;
            try
            {
                sqlSession.emplace(options.websocket);
            }
            catch (const std::exception& ex)
            {
                sqlFailure = ex.what();
                start.arrive_and_drop();
                return;
            }

            start.arrive_and_wait();
            try
            {
                sqlSession->execute("insert into \"" + options.schema + "\".\"" + options.conflictTable +
                                    "\" values ('sql_" + std::to_string(round) + "', 'sql')");
                sqlCommitted = true;
            }
            catch (const std::exception& ex)
            {
                const std::string message = ex.what();
                std::string lower = message;
                std::transform(lower.begin(), lower.end(), lower.begin(), [](const unsigned char ch) {
                    return static_cast<char>(std::tolower(ch));
                });
                sqlConflict = lower.find("transaction") != std::string::npos &&
                              (lower.find("collision") != std::string::npos ||
                               lower.find("rollback") != std::string::npos);
                if (!sqlConflict)
                {
                    sqlFailure = message;
                }
            }
        });

        std::thread gatewayWriter([&]() {
            std::optional<GatewaySession> session;
            try
            {
                session.emplace(options.websocket);
            }
            catch (const std::exception& ex)
            {
                gatewayFailure = ex.what();
                start.arrive_and_drop();
                return;
            }

            start.arrive_and_wait();
            try
            {
                try
                {
                    const auto open = session->request(
                        sessiongw::MessageType::open_table_insert,
                        openTableWritePayload(options.schema, options.conflictTable, {"KEYTXT", "NAME"},
                                              1U, describe.arrowSchema),
                        sessiongw::MessageType::open_table_operation_result);
                    const std::uint64_t operationId = parseOpenOperationId(open);
                    const std::vector<std::uint8_t> batch = nativeStringRowsBatch(
                        {"gateway_" + std::to_string(round)}, {"gateway"});
                    const std::uint64_t affected = parseAffectedRows(session->request(
                        sessiongw::MessageType::insert_rows,
                        insertRowsPayload(operationId, batch),
                        sessiongw::MessageType::affected_rows_result));
                    if (affected != 1U)
                    {
                        throw std::runtime_error("mixed-writer SessionGW insert affected row mismatch");
                    }
                    session->request(sessiongw::MessageType::close_operation,
                                     closeOperationPayload(operationId),
                                     sessiongw::MessageType::ok);
                    gatewayCommitted = true;
                }
                catch (const sessiongw::Error& error)
                {
                    if (error.category() != sessiongw::ErrorCategory::transaction_conflict)
                    {
                        throw;
                    }
                    gatewayConflict = true;
                    session->request(sessiongw::MessageType::ping, {}, sessiongw::MessageType::pong);
                }
                session->close();
            }
            catch (const std::exception& ex)
            {
                gatewayFailure = ex.what();
            }
        });

        start.arrive_and_wait();
        sqlWriter.join();
        gatewayWriter.join();
        if (!sqlFailure.empty())
        {
            throw std::runtime_error("mixed-writer SQL failure was not a transaction conflict: " + sqlFailure);
        }
        if (!gatewayFailure.empty())
        {
            throw std::runtime_error("mixed-writer SessionGW failure was not a transaction conflict: " +
                                     gatewayFailure);
        }

        summary.attempts += 2U;
        summary.committed += static_cast<std::uint32_t>(sqlCommitted) +
                             static_cast<std::uint32_t>(gatewayCommitted);
        summary.conflicts += static_cast<std::uint32_t>(sqlConflict) +
                             static_cast<std::uint32_t>(gatewayConflict);
        if (static_cast<std::uint32_t>(sqlCommitted) + static_cast<std::uint32_t>(sqlConflict) != 1U ||
            static_cast<std::uint32_t>(gatewayCommitted) + static_cast<std::uint32_t>(gatewayConflict) != 1U)
        {
            throw std::runtime_error("mixed writer had neither one commit nor one transaction conflict");
        }
    }

    SqlControlSession verifier(options.websocket);
    const std::uint64_t actualCommitted = verifier.executeScalarU64(
        "select count(*) from \"" + options.schema + "\".\"" + options.conflictTable + "\"");
    if (actualCommitted != summary.committed)
    {
        throw std::runtime_error("mixed-writer committed row count mismatch: expected " +
                                 std::to_string(summary.committed) + ", got " +
                                 std::to_string(actualCommitted));
    }
    return summary;
}

void typedMatrixStep(const Options& options)
{
    const std::vector<std::string> columns = {"KEYTXT", "FLAG", "SMALL_DEC", "INT_DEC", "BIG_DEC",
                                              "SCALED_DEC", "DBL", "DT", "TS", "CHR", "TXT"};
#ifdef SESSIONGW_TYPED_FACADE
    sessiongw::Session session = sessiongw::Session::connect(options.websocket);
    const sessiongw::TableName table{options.schema, options.typedTable};
    const sessiongw::TableMetadata metadata = session.describeTable(table);
    if (metadata.schema == nullptr || metadata.schema->num_fields() != 11)
    {
        throw std::runtime_error("public typed metadata did not expose the complete type matrix");
    }

    sessiongw::TableOperation insertion = session.openTableInsert(table, columns, 3U, metadata.schema);
    if (session.insertRows(insertion, nativeTypedRowsBatch(false)) != 3U)
    {
        throw std::runtime_error("public typed insert affected row mismatch");
    }
    session.closeOperation(insertion);

    const auto scan = [&]() {
        sessiongw::Cursor cursor = session.openTableScan(table, columns, true);
        sessiongw::FetchBatch batch = session.fetch(cursor, 3U, 1024U * 1024U);
        session.closeCursor(cursor);
        if (batch.rows == nullptr || !batch.rows->schema()->Equals(*metadata.schema) ||
            batch.rows->num_rows() != static_cast<std::int64_t>(batch.row_locations.size()))
        {
            throw std::runtime_error("public typed scan schema/row-location mismatch");
        }
        return batch;
    };

    sessiongw::FetchBatch scanned = scan();
    if (scanned.row_locations.size() != 3U)
    {
        throw std::runtime_error("public typed scan expected three inserted rows");
    }
    sessiongw::TableOperation update = session.openTableUpdate(table, columns, 1U, metadata.schema);
    if (session.updateRows(update,
                           std::span<const sessiongw::RowLocation>(&scanned.row_locations.at(0), 1U),
                           nativeTypedRowsBatch(true)) != 1U)
    {
        throw std::runtime_error("public typed update affected row mismatch");
    }
    session.closeOperation(update);

    scanned = scan();
    sessiongw::TableOperation deletion = session.openTableDelete(table, 1U);
    if (session.deleteRows(deletion,
                           std::span<const sessiongw::RowLocation>(&scanned.row_locations.at(1), 1U)) != 1U)
    {
        throw std::runtime_error("public typed delete affected row mismatch");
    }
    session.closeOperation(deletion);

    scanned = scan();
    if (scanned.row_locations.size() != 2U)
    {
        throw std::runtime_error("public typed deleted-row scan expected two visible rows");
    }
    session.close();
#else
    GatewaySession describeSession(options.websocket);
    const DescribeSummary describe = describeNamedTable(describeSession, options, options.typedTable);
    describeSession.close();
    insertNativeBatchIntoTable(options, options.typedTable, columns, describe.arrowSchema,
                               nativeTypedRowsBatch(false), 3U);

    GatewaySession updateSession(options.websocket);
    std::vector<RowHandle> handles = scanTableHandles(updateSession, options, options.typedTable, columns, 3U);
    const auto openUpdate = updateSession.request(sessiongw::MessageType::open_table_update,
                                                  openTableWritePayload(options.schema, options.typedTable,
                                                                        columns, 1U, describe.arrowSchema),
                                                  sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t updateOperationId = parseOpenOperationId(openUpdate);
    const std::vector<std::uint8_t> updateBatch = nativeTypedRowsBatch(true);
    const std::uint64_t updated = parseAffectedRows(updateSession.request(
        sessiongw::MessageType::update_rows,
        updateRowsPayload(updateOperationId, std::span<const RowHandle>(&handles.at(0), 1), updateBatch),
        sessiongw::MessageType::affected_rows_result));
    if (updated != 1U)
    {
        throw std::runtime_error("typed update affected row mismatch");
    }
    updateSession.request(sessiongw::MessageType::close_operation,
                          closeOperationPayload(updateOperationId), sessiongw::MessageType::ok);
    updateSession.close();

    GatewaySession deleteSession(options.websocket);
    handles = scanTableHandles(deleteSession, options, options.typedTable, columns, 3U);
    const auto openDelete = deleteSession.request(sessiongw::MessageType::open_table_delete,
                                                  openTableDeletePayload(options.schema, options.typedTable, 1U),
                                                  sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t deleteOperationId = parseOpenOperationId(openDelete);
    const std::uint64_t deleted = parseAffectedRows(deleteSession.request(
        sessiongw::MessageType::delete_rows,
        deleteRowsPayload(deleteOperationId, std::span<const RowHandle>(&handles.at(1), 1)),
        sessiongw::MessageType::affected_rows_result));
    if (deleted != 1U)
    {
        throw std::runtime_error("typed delete affected row mismatch");
    }
    deleteSession.request(sessiongw::MessageType::close_operation,
                          closeOperationPayload(deleteOperationId), sessiongw::MessageType::ok);
    deleteSession.close();

    handles = scanTableHandles(options, options.typedTable, columns, 2U);
    if (handles.size() != 2U)
    {
        throw std::runtime_error("typed deleted-row scan expected exactly two visible rows");
    }
#endif
}

LargeWorkloadSummary largeTableStep(const Options& options)
{
    GatewaySession describeSession(options.websocket);
    const DescribeSummary describe = describeNamedTable(describeSession, options, options.largeTable);
    describeSession.close();

    LargeWorkloadSummary summary;
    summary.rows = options.quick ? std::min<std::uint32_t>(1000U, options.largeRows)
                                 : options.largeRows;
    const auto quickBatchCap = [](const std::uint32_t rows, const std::uint32_t divisor) {
        return std::max<std::uint32_t>(1U, rows / divisor);
    };
    summary.insertBatchRows = std::min(options.largeInsertBatchRows,
                                       options.quick ? quickBatchCap(summary.rows, 5U) : summary.rows);
    summary.fetchBatchRows = std::min(options.largeFetchBatchRows,
                                      options.quick ? quickBatchCap(summary.rows, 3U) : summary.rows);
    summary.updateRows = options.largeUpdateRows == 0U ? std::max<std::uint32_t>(1U, summary.rows / 10U)
                                                       : options.largeUpdateRows;
    summary.deleteRows = options.largeDeleteRows == 0U ? std::max<std::uint32_t>(1U, summary.rows / 20U)
                                                       : options.largeDeleteRows;
    if (summary.updateRows + summary.deleteRows > summary.rows)
    {
        throw std::runtime_error("large update/delete row counts exceed table size");
    }
    summary.updateBatchRows = std::min(options.largeUpdateBatchRows,
                                       options.quick ? quickBatchCap(summary.updateRows, 3U) : summary.updateRows);
    summary.deleteBatchRows = std::min(options.largeDeleteBatchRows,
                                       options.quick ? quickBatchCap(summary.deleteRows, 3U) : summary.deleteRows);

    insertStringRowsBatchedIntoTable(options, options.largeTable, describe.arrowSchema,
                                     summary.rows, summary.insertBatchRows);

    GatewaySession updateSession(options.websocket);
    std::vector<RowHandle> handles = scanTableHandles(updateSession, options, options.largeTable,
                                                      {"KEYTXT", "NAME"}, summary.rows,
                                                      summary.fetchBatchRows);
    if (handles.size() != summary.rows)
    {
        throw std::runtime_error("large table scan handle count mismatch");
    }
    updateStringRowsBatched(updateSession, options, describe.arrowSchema,
                            std::span<const RowHandle>(handles.data(), summary.updateRows),
                            1U, summary.updateRows, summary.updateBatchRows);
    updateSession.close();

    GatewaySession deleteSession(options.websocket);
    handles = scanTableHandles(deleteSession, options, options.largeTable, {"KEYTXT", "NAME"},
                               summary.rows, summary.fetchBatchRows);
    deleteRowsBatched(deleteSession, options,
                      std::span<const RowHandle>(handles.data() + summary.updateRows, summary.deleteRows),
                      summary.deleteRows, summary.deleteBatchRows);
    const RowHandle staleHandle = handles.at(summary.updateRows);
    deleteSession.close();

    GatewaySession staleDeleteSession(options.websocket);
    const auto staleDeleteOpen = staleDeleteSession.request(sessiongw::MessageType::open_table_delete,
                                                            openTableDeletePayload(options.schema, options.largeTable, 1U),
                                                            sessiongw::MessageType::open_table_operation_result);
    const std::uint64_t staleDeleteOperationId = parseOpenOperationId(staleDeleteOpen);
    const std::string staleError = expectSessionError(
        staleDeleteSession,
        sessiongw::MessageType::delete_rows,
        deleteRowsPayload(staleDeleteOperationId, std::span<const RowHandle>(&staleHandle, 1U)),
        sessiongw::MessageType::affected_rows_result);
    staleDeleteSession.close();
    if (staleError.empty())
    {
        throw std::runtime_error("stale row handle was not rejected");
    }

    handles = scanTableHandles(options, options.largeTable, {"KEYTXT", "NAME"},
                               summary.rows - summary.deleteRows, summary.fetchBatchRows);
    if (handles.size() != summary.rows - summary.deleteRows)
    {
        throw std::runtime_error("large table deleted-row scan mismatch");
    }
    return summary;
}


void cleanupCloseStep(const Options& options)
{
    GatewaySession session(options.websocket);
    const auto open = session.request(sessiongw::MessageType::open_pushed_query,
                                      openPushedQueryPayload("select cast(1 as integer) as id union all select cast(2 as integer) as id order by id"),
                                      sessiongw::MessageType::open_cursor_result);
    (void)parseOpenCursorId(open);
    // Deliberately do not close the cursor; SessionGW Close must clean it up.
    session.close();
}

void concurrencyStep(const Options& options)
{
    const std::uint32_t sessions = options.quick ? std::min<std::uint32_t>(2U, options.concurrency)
                                                 : options.concurrency;
    const std::uint32_t iterations = options.quick ? 1U : options.concurrencyIterations;
    std::atomic<std::uint32_t> failures{0};
    std::mutex messagesMutex;
    std::vector<std::string> messages;
    std::vector<std::thread> workers;
    workers.reserve(sessions);

    for (std::uint32_t worker = 0; worker < sessions; ++worker)
    {
        workers.emplace_back([&, worker]() {
            try
            {
                for (std::uint32_t iteration = 0; iteration < iterations; ++iteration)
                {
                    GatewaySession session(options.websocket);
                    const std::string sql = "select cast(" + std::to_string(worker) +
                                            " as integer) as worker_id, cast(" +
                                            std::to_string(iteration) + " as integer) as iteration_id";
                    const auto open = session.request(sessiongw::MessageType::open_pushed_query,
                                                      openPushedQueryPayload(sql),
                                                      sessiongw::MessageType::open_cursor_result);
                    const std::uint64_t cursorId = parseOpenCursorId(open);
                    const FetchSummary fetch = parseFetchResult(session.request(sessiongw::MessageType::fetch,
                                                                                fetchPayload(cursorId, 10),
                                                                                sessiongw::MessageType::fetch_result));
                    if (fetch.arrowBatchBytes == 0 || !fetch.endOfCursor)
                    {
                        throw std::runtime_error("concurrent fetch assertion failed");
                    }
                    session.request(sessiongw::MessageType::close_cursor,
                                    closeCursorPayload(cursorId),
                                    sessiongw::MessageType::ok);
                    session.close();
                }
            }
            catch (const std::exception& ex)
            {
                failures.fetch_add(1U);
                std::lock_guard lock(messagesMutex);
                messages.push_back("worker " + std::to_string(worker) + ": " + ex.what());
            }
        });
    }

    for (auto& worker : workers)
    {
        worker.join();
    }
    if (failures.load() != 0U)
    {
        std::ostringstream out;
        out << failures.load() << " concurrent worker(s) failed";
        for (const std::string& message : messages)
        {
            out << "\n" << message;
        }
        throw std::runtime_error(out.str());
    }
}

Options parseOptions(const int argc, char** argv)
{
    Options options;
    options.websocket.host = "127.0.0.1";
    options.websocket.port = 8563;
    options.websocket.user.clear();
    options.websocket.password.clear();
    options.websocket.tls_mode = sessiongw::WebSocketTlsMode::tls_verify;

    auto requireValue = [&](const int& index, const std::string_view option) -> std::string {
        if (index + 1 >= argc)
        {
            throw std::runtime_error(std::string(option) + " requires a value");
        }
        return argv[index + 1];
    };

    for (int index = 1; index < argc; ++index)
    {
        const std::string_view arg(argv[index]);
        if (arg == "--host")
        {
            options.websocket.host = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--port")
        {
            options.websocket.port = static_cast<std::uint16_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--user")
        {
            options.websocket.user = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--password")
        {
            options.websocket.password = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--acl-password")
        {
            options.aclPassword = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--schema")
        {
            options.schema = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--table")
        {
            options.table = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--typed-table")
        {
            options.typedTable = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--large-table")
        {
            options.largeTable = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--conflict-table")
        {
            options.conflictTable = requireValue(index, arg);
            ++index;
        }
        else if (arg == "--concurrency")
        {
            options.concurrency = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--concurrency-iterations")
        {
            options.concurrencyIterations = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-rows")
        {
            options.largeRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-insert-batch-rows")
        {
            options.largeInsertBatchRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-fetch-batch-rows")
        {
            options.largeFetchBatchRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-update-rows")
        {
            options.largeUpdateRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-update-batch-rows")
        {
            options.largeUpdateBatchRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-delete-rows")
        {
            options.largeDeleteRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--large-delete-batch-rows")
        {
            options.largeDeleteBatchRows = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--lifetime-opens")
        {
            options.lifetimeOpens = static_cast<std::uint32_t>(std::stoul(requireValue(index, arg)));
            ++index;
        }
        else if (arg == "--skip-tls-verify")
        {
            options.websocket.tls_mode = sessiongw::WebSocketTlsMode::tls_skip_verify_for_test_only;
        }
        else if (arg == "--tls")
        {
            options.websocket.tls_mode = sessiongw::WebSocketTlsMode::tls_verify;
        }
        else if (arg == "--plain")
        {
            options.websocket.tls_mode = sessiongw::WebSocketTlsMode::plain_for_test_only;
        }
        else if (arg == "--ca-file")
        {
            options.websocket.ca_file = requireValue(index, arg);
            options.websocket.tls_mode = sessiongw::WebSocketTlsMode::tls_verify;
            ++index;
        }
        else if (arg == "--quick")
        {
            options.quick = true;
        }
        else if (arg == "--help" || arg == "-h")
        {
            std::cout << "Usage: " << argv[0]
                      << " [--host HOST] [--port PORT] [--user USER] [--password PASSWORD]"
                      << " [--acl-password PASSWORD] [--schema SCHEMA] [--table TABLE]"
                      << " [--typed-table TABLE] [--large-table TABLE]"
                      << " [--conflict-table TABLE] [--concurrency N] [--concurrency-iterations N] [--large-rows N]"
                      << " [--large-insert-batch-rows N] [--large-fetch-batch-rows N]"
                      << " [--large-update-rows N] [--large-update-batch-rows N]"
                      << " [--large-delete-rows N] [--large-delete-batch-rows N] [--lifetime-opens N]"
                      << " [--skip-tls-verify|--tls|--ca-file FILE|--plain] [--quick]\n";
            std::exit(0);
        }
        else
        {
            throw std::runtime_error("unknown option: " + std::string(arg));
        }
    }

    const char* const instrumentation = std::getenv("EXASOL_SESSIONGW_INSTRUMENTATION");
    options.websocket.instrumentation_enabled =
        instrumentation != nullptr && std::string_view(instrumentation) != "0";
    if (options.aclPassword.empty())
    {
        throw std::runtime_error("--acl-password is required for the ACL integration checks");
    }
    if (options.concurrency == 0U || options.concurrencyIterations == 0U || options.largeRows == 0U ||
        options.largeInsertBatchRows == 0U || options.largeFetchBatchRows == 0U ||
        options.largeUpdateBatchRows == 0U || options.largeDeleteBatchRows == 0U)
    {
        throw std::runtime_error("concurrency, large-row, and large-batch values must be positive");
    }
    return options;
}

void runStep(std::vector<StepResult>& results, const std::string& name, const std::function<std::string()>& body)
{
    const auto started = std::chrono::steady_clock::now();
    StepResult result;
    result.name = name;
    try
    {
        result.detail = body();
        result.passed = true;
    }
    catch (const std::exception& ex)
    {
        result.detail = ex.what();
    }
    result.duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started);
    results.push_back(std::move(result));
    const StepResult& stored = results.back();
    std::cout << (stored.passed ? "PASS" : "FAIL") << '\t' << stored.name << '\t'
              << stored.duration.count() << "ms\t" << stored.detail << '\n';
    if (!stored.passed)
    {
        throw std::runtime_error("step failed: " + stored.name);
    }
}

} // namespace

int main(const int argc, char** argv)
{
    try
    {
        const Options options = parseOptions(argc, argv);
        std::vector<StepResult> results;
        std::vector<std::uint8_t> tableSchema;
        std::vector<RowHandle> rowHandles;

        std::cout << "SessionGW SDK integration workload\n";
        std::cout << "target=" << options.websocket.host << ':' << options.websocket.port
                  << " schema=" << options.schema << " table=" << options.table << '\n';

        runStep(results, "connect_hello_ping_close", [&]() {
            pingStep(options);
            return "WebSocket login, enterSessionGateway, Hello, Ping, Close";
        });
        runStep(results, "strict_websocket_message_boundaries", [&]() {
            strictWebSocketMessageBoundaryStep(options);
            return "unsupported upgrade, concatenated, truncated, oversized, and text messages rejected; fresh reconnect succeeded";
        });
        runStep(results, "metadata_describe_version", [&]() {
            const DescribeSummary describe = metadataStep(options);
            tableSchema = describe.arrowSchema;
            return "version=" + describe.version + " arrow_schema_bytes=" + std::to_string(tableSchema.size());
        });
        runStep(results, "metadata_errors_and_ddl_version", [&]() {
            metadataErrorsAndDdlVersionStep(options);
            return "missing, unauthorized, and unsupported types rejected; DDL changed schema/version";
        });
#ifdef SESSIONGW_TYPED_FACADE
        runStep(results, "public_typed_facade", [&]() {
            publicFacadeStep(options);
            return "typed Arrow metadata/query/scan and rollback-only insert/update/delete through sessiongw.hpp";
        });
#endif
        runStep(results, "pushed_query_cursor", [&]() {
            pushedQueryStep(options);
            return "open/fetch/close pushed SELECT cursor";
        });
        runStep(results, "direct_table_operation_privileges", [&]() {
            directTablePrivilegeStep(options, tableSchema);
            return "object/system privileges isolated; SELECT+UPDATE/DELETE data paths executed and rolled back";
        });
        runStep(results, "direct_not_null_constraint_cleanup", [&]() {
            directConstraintEnforcementStep(options);
            return "mixed NULL insert/update batches rejected and earlier accepted prefixes CLEAN-aborted";
        });
        if (options.lifetimeOpens != 0U)
        {
            runStep(results, "long_lived_cursor_operation_ids", [&]() {
                return lifetimeIdExhaustionStep(options, options.lifetimeOpens);
            });
        }
        runStep(results, "table_scan_row_handles", [&]() {
            rowHandles = scanHandlesStep(options, 3U);
            return "handles=" + std::to_string(rowHandles.size());
        });
        runStep(results, "sequential_row_handle_max_bytes_continuation", [&]() {
            sequentialRowHandleByteLimitContinuationStep(options);
            return "overshoot failed without advancing; valid short prefix plus continuation returned all handles once in order";
        });
        runStep(results, "positioned_fetch_continuation", [&]() {
            positionedFetchContinuationStep(options);
            return "size-one positioned fetches honored maxBytes, preserved sequential scan, and rejected stale handles";
        });
        runStep(results, "row_handle_provenance", [&]() {
            rowHandleProvenanceStep(options);
            return "cross-table, deleted, post-commit, post-rollback, and unknown handles rejected";
        });
        runStep(results, "transaction_boundary_state_invalidation", [&]() {
            transactionBoundaryInvalidationStep(options);
            return "commit/rollback invalidated cursors, operations, native state, and row locations";
        });
        runStep(results, "native_insert_autocommit_batches", [&]() {
            insertRowsStep(options, tableSchema, {"k1001", "k1002"}, {"sdk_insert_a", "sdk_insert_b"});
            return "inserted=2";
        });
        runStep(results, "native_insert_explicit_commit", [&]() {
            insertRowsStep(options, tableSchema, {"k1003"}, {"sdk_commit"}, false, true, false);
            return "inserted=1 committed";
        });
        runStep(results, "native_insert_explicit_rollback", [&]() {
            insertRowsStep(options, tableSchema, {"k1999"}, {"sdk_rollback"}, false, false, true);
            return "inserted=1 rolled_back";
        });
        runStep(results, "row_handle_update", [&]() {
            const RowHandle handle = updateRowStep(options, tableSchema, "k1", "sdk_updated_seed_1");
            return "updated handle " + std::to_string(handle.rowNumber);
        });
        runStep(results, "row_handle_delete", [&]() {
            const RowHandle handle = deleteRowStep(options);
            return "deleted handle " + std::to_string(handle.rowNumber);
        });
        runStep(results, "typed_native_matrix_update_delete_scan", [&]() {
            typedMatrixStep(options);
            return "BOOLEAN, DECIMAL(9/18/36), scaled DECIMAL, DOUBLE, DATE, TIMESTAMP, CHAR, VARCHAR, NULLs";
        });
        runStep(results, "negative_dml_lifecycle_errors", [&]() {
            negativeDmlLifecycleStep(options, tableSchema);
            return "unknown operation, oversized InsertRows, InsertRows after CloseOperation, mismatched UpdateRows";
        });
        runStep(results, "transport_abort_cleanup", [&]() {
            transportAbortCleanupStep(options, tableSchema);
            return "dropped transport with open cursor and unclosed insert operation";
        });
        runStep(results, "mixed_sql_sessiongw_writer_conflicts", [&]() {
            const MixedWriterSummary summary = mixedFrontendWriterConflictStep(options);
            return "attempts=" + std::to_string(summary.attempts) +
                   " committed=" + std::to_string(summary.committed) +
                   " transaction_conflicts=" + std::to_string(summary.conflicts);
        });
        runStep(results, "timeout_kill_reconnect", [&]() {
            timeoutKillReconnectStep(options);
            return "query timeout exception, killed SessionGW session exception, fresh reconnect ping";
        });
        runStep(results, "large_table_insert_fetch_update_delete_batched", [&]() {
            const LargeWorkloadSummary summary = largeTableStep(options);
            return "rows=" + std::to_string(summary.rows) +
                   " insert_batch=" + std::to_string(summary.insertBatchRows) +
                   " fetch_batch=" + std::to_string(summary.fetchBatchRows) +
                   " update_rows=" + std::to_string(summary.updateRows) +
                   " update_batch=" + std::to_string(summary.updateBatchRows) +
                   " delete_rows=" + std::to_string(summary.deleteRows) +
                   " delete_batch=" + std::to_string(summary.deleteBatchRows);
        });
        runStep(results, "session_close_cleanup", [&]() {
            cleanupCloseStep(options);
            return "left cursor open, closed SessionGW session";
        });
        runStep(results, "multi_connection_query_stress", [&]() {
            concurrencyStep(options);
            const std::uint32_t sessions = options.quick ? std::min<std::uint32_t>(2U, options.concurrency)
                                                         : options.concurrency;
            const std::uint32_t iterations = options.quick ? 1U : options.concurrencyIterations;
            return "sessions=" + std::to_string(sessions) + " iterations=" + std::to_string(iterations);
        });

        std::cout << "\nCoverage summary:\n"
                  << "  transport: WebSocket API v5 WSS login, enterSessionGateway, SGW1 framing\n"
                  << "  control: Hello, Ping, Close, session-close cleanup\n"
                  << "  metadata: DescribeTable, GetTableVersion\n"
                  << "  read: pushed-query cursor, table scan cursor, row-handle scan\n"
                  << "  authorization: operation-specific object/system SELECT, INSERT, UPDATE, DELETE privileges\n"
                  << "  write: OpenTableInsert, InsertRows native DMP batch, CloseOperation\n"
                  << "  types: BOOLEAN, DECIMAL(9/18/36), scaled DECIMAL, DOUBLE, DATE, TIMESTAMP, CHAR, VARCHAR, NULLs\n"
                  << "  tables: small seed table, typed matrix table, large batch/deleted-row stress table\n"
                  << "  batching: one operation handle with multiple InsertRows/Fetch/UpdateRows/DeleteRows frames\n"
                  << "  negative DML: unknown operation, oversized batch, write-after-close, update cardinality mismatch\n"
                  << "  cleanup: transport abort with open cursor/unclosed insert operation\n"
                  << "  mixed writers: concurrent SQL and SessionGW writes commit or report transaction conflict once\n"
                  << "  timeout/kill: query timeout exception, killed session exception, reconnect ping\n"
                  << "  consistency: scan row handles -> batched update/delete -> multi-fetch visible rows -> SQL verification\n"
                  << "  transactions: SetAutocommit(false), Commit, Rollback\n"
                  << "  dml: OpenTableUpdate/UpdateRows, OpenTableDelete/DeleteRows\n"
                  << "  concurrency: multiple independent SDK WebSocket sessions\n";
        if (options.websocket.instrumentation_enabled)
        {
            if (sdkStatistics.sessiongw_frames_sent == 0U ||
                sdkStatistics.sessiongw_frames_received == 0U ||
                sdkStatistics.transport_bytes_written == 0U ||
                sdkStatistics.transport_bytes_read == 0U ||
                sdkStatistics.websocket_mask_copy_bytes == 0U)
            {
                throw std::runtime_error("SessionGW SDK instrumentation produced empty transport counters");
            }
            std::cout << "SessionGW SDK performance: connect_ns=" << sdkStatistics.connect_nanoseconds
                      << " transport_write_calls=" << sdkStatistics.transport_write_calls
                      << " transport_read_calls=" << sdkStatistics.transport_read_calls
                      << " transport_bytes_written=" << sdkStatistics.transport_bytes_written
                      << " transport_bytes_read=" << sdkStatistics.transport_bytes_read
                      << " transport_write_ns=" << sdkStatistics.transport_write_nanoseconds
                      << " transport_read_ns=" << sdkStatistics.transport_read_nanoseconds
                      << " frames_sent=" << sdkStatistics.sessiongw_frames_sent
                      << " frames_received=" << sdkStatistics.sessiongw_frames_received
                      << " payload_bytes_sent=" << sdkStatistics.sessiongw_payload_bytes_sent
                      << " payload_bytes_received=" << sdkStatistics.sessiongw_payload_bytes_received
                      << " websocket_mask_copy_bytes=" << sdkStatistics.websocket_mask_copy_bytes << '\n';
        }
        std::cout << "SessionGW SDK integration workload PASSED: " << results.size() << " steps\n";
        return 0;
    }
    catch (const std::exception& ex)
    {
        std::cerr << "SessionGW SDK integration workload FAILED: " << ex.what() << '\n';
        return 1;
    }
}
