#include <sessiongw/sessiongw.hpp>

int main()
{
    const sessiongw::WebSocketOptions options;
    return options.user.empty() && options.password.empty() ? 0 : 1;
}
