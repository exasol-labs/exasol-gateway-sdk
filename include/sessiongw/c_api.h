#ifndef SESSIONGW_C_API_H
#define SESSIONGW_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Stable C ABI. All returned handles and borrowed byte/string views remain
 * owned by the SDK and are valid until their matching destroy call. */
typedef struct sessiongw_c_session sessiongw_c_session;
typedef struct sessiongw_c_metadata sessiongw_c_metadata;
typedef struct sessiongw_c_cursor sessiongw_c_cursor;
typedef struct sessiongw_c_fetch sessiongw_c_fetch;
typedef struct sessiongw_c_native_fetch sessiongw_c_native_fetch;
typedef struct sessiongw_c_operation sessiongw_c_operation;

typedef struct sessiongw_c_options {
    const char* host;
    uint16_t port;
    const char* user;
    const char* password;
    /* "verify" for production; "skip_verify" and "plain" are test-only. */
    const char* tls_mode;
    const char* ca_file;
    int instrumentation_enabled;
} sessiongw_c_options;

typedef struct sessiongw_c_statistics {
    uint64_t requests;
    uint64_t request_bytes;
    uint64_t response_bytes;
    uint64_t network_nanoseconds;
} sessiongw_c_statistics;

/* Additive detailed transport profile. The original statistics structure and
 * function retain their stable size and semantics. */
typedef struct sessiongw_c_transport_profile {
    uint64_t write_calls;
    uint64_t read_calls;
    uint64_t read_iterations;
    uint64_t bytes_written;
    uint64_t bytes_read;
    uint64_t write_nanoseconds;
    uint64_t read_nanoseconds;
    uint64_t websocket_header_read_calls;
    uint64_t websocket_header_read_bytes;
    uint64_t websocket_header_read_nanoseconds;
    uint64_t websocket_payload_read_calls;
    uint64_t websocket_payload_read_bytes;
    uint64_t websocket_payload_read_nanoseconds;
    uint64_t sessiongw_frame_decode_nanoseconds;
} sessiongw_c_transport_profile;

/* Returns zero on success. Failures never cross the C boundary; inspect the
 * thread-local category/message immediately after a nonzero result. */
uint16_t sessiongw_c_last_error_category(void);
const char* sessiongw_c_last_error_message(void);

int sessiongw_c_connect(const sessiongw_c_options* options, sessiongw_c_session** out);
int sessiongw_c_connect_with_client_name(const sessiongw_c_options* options,
                                         const char* client_name,
                                         sessiongw_c_session** out);
int sessiongw_c_execute_sql(const sessiongw_c_options* options, const char* sql);
void sessiongw_c_session_destroy(sessiongw_c_session* session);
int sessiongw_c_close(sessiongw_c_session* session);
int sessiongw_c_statistics_get(const sessiongw_c_session* session, sessiongw_c_statistics* out);
int sessiongw_c_transport_profile_get(const sessiongw_c_session* session,
                                      sessiongw_c_transport_profile* out);

int sessiongw_c_describe_table(sessiongw_c_session* session, const char* schema,
                               const char* table, sessiongw_c_metadata** out);
void sessiongw_c_metadata_destroy(sessiongw_c_metadata* metadata);
const char* sessiongw_c_metadata_schema_name(const sessiongw_c_metadata* metadata);
const char* sessiongw_c_metadata_table_name(const sessiongw_c_metadata* metadata);
const char* sessiongw_c_metadata_version(const sessiongw_c_metadata* metadata);
int sessiongw_c_metadata_has_row_count(const sessiongw_c_metadata* metadata);
uint64_t sessiongw_c_metadata_row_count(const sessiongw_c_metadata* metadata);
const uint8_t* sessiongw_c_metadata_schema_ipc(const sessiongw_c_metadata* metadata, size_t* size);
int sessiongw_c_get_table_version(sessiongw_c_session* session, const char* schema,
                                  const char* table, sessiongw_c_metadata** out);

int sessiongw_c_open_pushed_query(sessiongw_c_session* session, const char* sql,
                                  sessiongw_c_cursor** out);
int sessiongw_c_open_table_scan(sessiongw_c_session* session, const char* schema,
                                const char* table, const char* const* columns,
                                size_t column_count, int include_row_locations,
                                sessiongw_c_cursor** out);
void sessiongw_c_cursor_destroy(sessiongw_c_cursor* cursor);
uint64_t sessiongw_c_cursor_id(const sessiongw_c_cursor* cursor);
const uint8_t* sessiongw_c_cursor_schema_ipc(const sessiongw_c_cursor* cursor, size_t* size);
int sessiongw_c_fetch_rows(sessiongw_c_session* session, const sessiongw_c_cursor* cursor,
                           uint32_t max_rows, uint32_t max_bytes, sessiongw_c_fetch** out);
int sessiongw_c_fetch_positioned(sessiongw_c_session* session, const sessiongw_c_cursor* cursor,
                                 const uint64_t* row_locations, size_t location_count,
                                 uint32_t max_bytes, sessiongw_c_fetch** out);
void sessiongw_c_fetch_destroy(sessiongw_c_fetch* fetch);
uint64_t sessiongw_c_fetch_cursor_id(const sessiongw_c_fetch* fetch);
int sessiongw_c_fetch_end(const sessiongw_c_fetch* fetch);
const uint8_t* sessiongw_c_fetch_rows_ipc(const sessiongw_c_fetch* fetch, size_t* size);
const uint64_t* sessiongw_c_fetch_row_locations(const sessiongw_c_fetch* fetch, size_t* count);
/* Capability-negotiated native read batch v1. Scalar buffers are canonical
 * little-endian. NULL vectors use 0x00=value and 0xff=NULL. String sizes are
 * uint64 big-endian. Every returned pointer is borrowed from fetch. */
int sessiongw_c_fetch_native(sessiongw_c_session* session, const sessiongw_c_cursor* cursor,
                             uint32_t max_rows, uint32_t max_bytes,
                             sessiongw_c_native_fetch** out);
int sessiongw_c_fetch_positioned_native(sessiongw_c_session* session,
                                        const sessiongw_c_cursor* cursor,
                                        const uint64_t* row_locations,
                                        size_t location_count, uint32_t max_bytes,
                                        sessiongw_c_native_fetch** out);
void sessiongw_c_native_fetch_destroy(sessiongw_c_native_fetch* fetch);
uint64_t sessiongw_c_native_fetch_cursor_id(const sessiongw_c_native_fetch* fetch);
int sessiongw_c_native_fetch_end(const sessiongw_c_native_fetch* fetch);
uint32_t sessiongw_c_native_fetch_row_count(const sessiongw_c_native_fetch* fetch);
size_t sessiongw_c_native_fetch_column_count(const sessiongw_c_native_fetch* fetch);
uint8_t sessiongw_c_native_fetch_column_kind(const sessiongw_c_native_fetch* fetch, size_t column);
int32_t sessiongw_c_native_fetch_column_scale(const sessiongw_c_native_fetch* fetch, size_t column);
const uint8_t* sessiongw_c_native_fetch_column_nulls(const sessiongw_c_native_fetch* fetch,
                                                      size_t column, size_t* size);
const uint8_t* sessiongw_c_native_fetch_column_fixed_data(const sessiongw_c_native_fetch* fetch,
                                                          size_t column, size_t* size);
const uint8_t* sessiongw_c_native_fetch_column_sizes(const sessiongw_c_native_fetch* fetch,
                                                     size_t column, size_t* size);
const uint8_t* sessiongw_c_native_fetch_column_variable_data(const sessiongw_c_native_fetch* fetch,
                                                             size_t column, size_t* size);
const uint64_t* sessiongw_c_native_fetch_row_locations(const sessiongw_c_native_fetch* fetch,
                                                       size_t* count);
int sessiongw_c_close_cursor(sessiongw_c_session* session, sessiongw_c_cursor* cursor);

int sessiongw_c_open_table_insert(sessiongw_c_session* session, const char* schema,
                                  const char* table, const char* const* columns,
                                  size_t column_count, uint32_t max_rows_per_batch,
                                  const uint8_t* schema_ipc, size_t schema_size,
                                  sessiongw_c_operation** out);
int sessiongw_c_open_table_update(sessiongw_c_session* session, const char* schema,
                                  const char* table, const char* const* columns,
                                  size_t column_count, uint32_t max_rows_per_batch,
                                  const uint8_t* schema_ipc, size_t schema_size,
                                  sessiongw_c_operation** out);
int sessiongw_c_open_table_delete(sessiongw_c_session* session, const char* schema,
                                  const char* table, uint32_t max_rows_per_batch,
                                  sessiongw_c_operation** out);
void sessiongw_c_operation_destroy(sessiongw_c_operation* operation);
uint64_t sessiongw_c_operation_id(const sessiongw_c_operation* operation);
const uint8_t* sessiongw_c_operation_schema_ipc(const sessiongw_c_operation* operation, size_t* size);
int sessiongw_c_insert_rows(sessiongw_c_session* session, const sessiongw_c_operation* operation,
                            const uint8_t* native_batch, size_t native_size, uint64_t* affected);
int sessiongw_c_update_rows(sessiongw_c_session* session, const sessiongw_c_operation* operation,
                            const uint64_t* row_locations, size_t location_count,
                            const uint8_t* native_batch, size_t native_size, uint64_t* affected);
int sessiongw_c_delete_rows(sessiongw_c_session* session, const sessiongw_c_operation* operation,
                            const uint64_t* row_locations, size_t location_count, uint64_t* affected);
int sessiongw_c_close_operation(sessiongw_c_session* session, sessiongw_c_operation* operation);

int sessiongw_c_set_autocommit(sessiongw_c_session* session, int enabled);
int sessiongw_c_commit(sessiongw_c_session* session);
int sessiongw_c_rollback(sessiongw_c_session* session);

#ifdef __cplusplus
}
#endif
#endif
