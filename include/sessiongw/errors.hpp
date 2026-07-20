#ifndef SESSIONGW_ERRORS_HPP
#define SESSIONGW_ERRORS_HPP

#include <cstdint>
#include <stdexcept>
#include <string>

namespace sessiongw
{

enum class ErrorCategory : std::uint16_t
{
    protocol_error = 1,
    authentication_failed = 2,
    not_authorized = 3,
    object_not_found = 4,
    unsupported_type = 5,
    unsupported_operation = 6,
    transaction_conflict = 7,
    constraint_violation = 8,
    resource_limit = 9,
    cursor_not_found = 10,
    internal_error = 11,
    transport_error = 12,
    // Client-local result when a completion request may have been applied but
    // no authoritative response was received. Not a server wire error value.
    outcome_unknown = 13
};

const char* toString(ErrorCategory category) noexcept;

class Error : public std::runtime_error
{
public:
    Error(ErrorCategory category, std::string message);

    [[nodiscard]] ErrorCategory category() const noexcept;

private:
    ErrorCategory category_;
};

} // namespace sessiongw

#endif // SESSIONGW_ERRORS_HPP
