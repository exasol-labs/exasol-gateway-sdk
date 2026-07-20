#ifndef SESSIONGW_CAPABILITIES_HPP
#define SESSIONGW_CAPABILITIES_HPP

#include <cstdint>
#include <set>
#include <string>
#include <vector>

namespace sessiongw
{

enum class Capability : std::uint16_t
{
    session_gw_v1 = 1,
    arrow_ipc_batch_v1 = 2,
    pushed_query_v1 = 3,
    metadata_v1 = 4,
    table_scan_v1 = 5,
    table_write_v1 = 6,
    transaction_control_v1 = 7,
    native_table_write_batch_v1 = 8,
    table_update_v1 = 9,
    table_delete_v1 = 10,
    outcome_recovery_v1 = 11,
    native_table_write_batch_v2 = 12,
    native_table_read_batch_v1 = 13
};

const char* toString(Capability capability) noexcept;

class Capabilities
{
public:
    Capabilities() = default;
    explicit Capabilities(std::vector<Capability> capabilities);

    void add(Capability capability);
    [[nodiscard]] bool supports(Capability capability) const;
    [[nodiscard]] std::vector<Capability> values() const;

private:
    std::set<Capability> capabilities_;
};

Capabilities defaultClientCapabilities();

} // namespace sessiongw

#endif // SESSIONGW_CAPABILITIES_HPP
