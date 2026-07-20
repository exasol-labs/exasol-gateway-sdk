#ifndef SESSIONGW_SESSION_HPP
#define SESSIONGW_SESSION_HPP

#include <sessiongw/capabilities.hpp>
#include <sessiongw/native_write_batch.hpp>
#include <sessiongw/websocket.hpp>

#include <arrow/type_fwd.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace sessiongw
{

struct TableName
{
    std::string schema;
    std::string table;
};

struct RowLocation
{
    std::uint64_t row_number = 0;
};

struct TableMetadata
{
    TableName table;
    std::string version;
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::uint8_t> schema_ipc;
    std::optional<std::uint64_t> row_count;
};

struct Cursor
{
    std::uint64_t id = 0;
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::uint8_t> schema_ipc;
};

struct FetchBatch
{
    std::uint64_t cursor_id = 0;
    bool end_of_cursor = false;
    std::shared_ptr<arrow::RecordBatch> rows;
    std::vector<std::uint8_t> rows_ipc;
    std::vector<RowLocation> row_locations;
};

struct NativeFetchBatch
{
    std::uint64_t cursor_id = 0;
    bool end_of_cursor = false;
    std::vector<std::uint8_t> native_rows;
    std::vector<RowLocation> row_locations;
};

struct TableOperation
{
    std::uint64_t id = 0;
    std::shared_ptr<arrow::Schema> accepted_schema;
    std::vector<std::uint8_t> accepted_schema_ipc;
};

class Session final
{
public:
    ~Session();
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) noexcept;
    Session& operator=(Session&&) noexcept;

    static Session connect(const WebSocketOptions& options);

    [[nodiscard]] const Capabilities& capabilities() const;
    [[nodiscard]] const WebSocketStatistics& statistics() const;

    [[nodiscard]] TableMetadata describeTable(const TableName& table);
    [[nodiscard]] std::string getTableVersion(const TableName& table);

    [[nodiscard]] Cursor openPushedQuery(const std::string& sql);
    [[nodiscard]] Cursor openTableScan(const TableName& table,
                                       const std::vector<std::string>& columns,
                                       bool include_row_locations);
    [[nodiscard]] FetchBatch fetch(const Cursor& cursor,
                                   std::uint32_t max_rows = 0,
                                   std::uint32_t max_bytes = 0);
    [[nodiscard]] FetchBatch fetchPositioned(const Cursor& cursor,
                                             std::span<const RowLocation> locations,
                                             std::uint32_t max_bytes = 0);
    // Stable-C-ABI adapter path: retain validated IPC framing without decoding
    // a second Arrow object that the adapter would immediately decode again.
    [[nodiscard]] FetchBatch fetchIpc(const Cursor& cursor,
                                      std::uint32_t max_rows = 0,
                                      std::uint32_t max_bytes = 0);
    [[nodiscard]] FetchBatch fetchPositionedIpc(const Cursor& cursor,
                                                std::span<const RowLocation> locations,
                                                std::uint32_t max_bytes = 0);
    [[nodiscard]] NativeFetchBatch fetchNative(const Cursor& cursor,
                                               std::uint32_t max_rows = 0,
                                               std::uint32_t max_bytes = 0);
    [[nodiscard]] NativeFetchBatch fetchPositionedNative(
        const Cursor& cursor,
        std::span<const RowLocation> locations,
        std::uint32_t max_bytes = 0);
    void closeCursor(Cursor& cursor);

    [[nodiscard]] TableOperation openTableInsert(const TableName& table,
                                                 const std::vector<std::string>& columns,
                                                 std::uint32_t max_rows_per_batch,
                                                 const std::shared_ptr<arrow::Schema>& schema);
    [[nodiscard]] TableOperation openTableUpdate(const TableName& table,
                                                 const std::vector<std::string>& columns,
                                                 std::uint32_t max_rows_per_batch,
                                                 const std::shared_ptr<arrow::Schema>& schema);
    [[nodiscard]] TableOperation openTableDelete(const TableName& table,
                                                 std::uint32_t max_rows_per_batch);
    [[nodiscard]] std::uint64_t insertRows(const TableOperation& operation,
                                           std::span<const std::uint8_t> native_batch);
    [[nodiscard]] std::uint64_t updateRows(const TableOperation& operation,
                                           std::span<const RowLocation> locations,
                                           std::span<const std::uint8_t> native_batch);
    [[nodiscard]] std::uint64_t deleteRows(const TableOperation& operation,
                                           std::span<const RowLocation> locations);
    void closeOperation(TableOperation& operation);

    void setAutocommit(bool enabled);
    void commit();
    void rollback();
    void close();

private:
    class Impl;
    explicit Session(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

} // namespace sessiongw

#endif // SESSIONGW_SESSION_HPP
