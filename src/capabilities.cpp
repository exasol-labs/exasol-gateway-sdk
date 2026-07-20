#include <sessiongw/capabilities.hpp>

#include <utility>

namespace sessiongw
{

const char* toString(const Capability capability) noexcept
{
    switch (capability)
    {
        case Capability::session_gw_v1:
            return "SESSION_GW_V1";
        case Capability::arrow_ipc_batch_v1:
            return "ARROW_IPC_BATCH_V1";
        case Capability::pushed_query_v1:
            return "PUSHED_QUERY_V1";
        case Capability::metadata_v1:
            return "METADATA_V1";
        case Capability::table_scan_v1:
            return "TABLE_SCAN_V1";
        case Capability::table_write_v1:
            return "TABLE_WRITE_V1";
        case Capability::transaction_control_v1:
            return "TRANSACTION_CONTROL_V1";
        case Capability::native_table_write_batch_v1:
            return "NATIVE_TABLE_WRITE_BATCH_V1";
        case Capability::table_update_v1:
            return "TABLE_UPDATE_V1";
        case Capability::table_delete_v1:
            return "TABLE_DELETE_V1";
        case Capability::outcome_recovery_v1:
            return "OUTCOME_RECOVERY_V1";
        case Capability::native_table_write_batch_v2:
            return "NATIVE_TABLE_WRITE_BATCH_V2";
        case Capability::native_table_read_batch_v1:
            return "NATIVE_TABLE_READ_BATCH_V1";
    }
    return "UNKNOWN";
}

Capabilities::Capabilities(std::vector<Capability> capabilities)
{
    for (const Capability capability : capabilities)
    {
        add(capability);
    }
}

void Capabilities::add(const Capability capability)
{
    capabilities_.insert(capability);
}

bool Capabilities::supports(const Capability capability) const
{
    return capabilities_.contains(capability);
}

std::vector<Capability> Capabilities::values() const
{
    return {capabilities_.begin(), capabilities_.end()};
}

Capabilities defaultClientCapabilities()
{
    return Capabilities({Capability::session_gw_v1,
                         Capability::arrow_ipc_batch_v1,
                         Capability::pushed_query_v1,
                         Capability::metadata_v1,
                         Capability::table_scan_v1,
                         Capability::table_write_v1,
                         Capability::native_table_write_batch_v2,
                         Capability::native_table_read_batch_v1,
                         Capability::table_update_v1,
                         Capability::table_delete_v1,
                         Capability::transaction_control_v1});
}

} // namespace sessiongw
