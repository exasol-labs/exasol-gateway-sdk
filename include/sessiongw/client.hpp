#ifndef SESSIONGW_CLIENT_HPP
#define SESSIONGW_CLIENT_HPP

#include <sessiongw/capabilities.hpp>

#include <cstdint>
#include <string>

namespace sessiongw
{

enum class TlsMode : std::uint16_t
{
    require = 1,
    disable_for_test_only = 2
};

struct ConnectionOptions
{
    std::string host = "127.0.0.1";
    std::uint16_t port = 8563;
    std::string user;
    std::string password;
    TlsMode tls_mode = TlsMode::require;
};

class Client
{
public:
    Client() = default;

    static Client createOfflineForTesting();

    [[nodiscard]] Capabilities clientCapabilities() const;
};

} // namespace sessiongw

#endif // SESSIONGW_CLIENT_HPP
