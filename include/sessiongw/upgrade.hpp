#ifndef SESSIONGW_UPGRADE_HPP
#define SESSIONGW_UPGRADE_HPP

#include <cstdint>

namespace sessiongw
{

// Stable after-login command byte in the existing Exasol SQL protocol command
// space. The normal SQL protocol version is not bumped; after this command is
// acknowledged, SessionGW performs its own SGW1 version/capability negotiation.
inline constexpr std::uint8_t enter_session_gateway_command = 127;

} // namespace sessiongw

#endif // SESSIONGW_UPGRADE_HPP
