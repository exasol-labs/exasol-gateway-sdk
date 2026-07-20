#include <sessiongw/client.hpp>

namespace sessiongw
{

Client Client::createOfflineForTesting()
{
    return Client();
}

Capabilities Client::clientCapabilities() const
{
    return defaultClientCapabilities();
}

} // namespace sessiongw
