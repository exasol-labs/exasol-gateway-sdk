#include <sessiongw/errors.hpp>

#include <utility>

namespace sessiongw
{

const char* toString(const ErrorCategory category) noexcept
{
    switch (category)
    {
        case ErrorCategory::protocol_error:
            return "PROTOCOL_ERROR";
        case ErrorCategory::authentication_failed:
            return "AUTHENTICATION_FAILED";
        case ErrorCategory::not_authorized:
            return "NOT_AUTHORIZED";
        case ErrorCategory::object_not_found:
            return "OBJECT_NOT_FOUND";
        case ErrorCategory::unsupported_type:
            return "UNSUPPORTED_TYPE";
        case ErrorCategory::unsupported_operation:
            return "UNSUPPORTED_OPERATION";
        case ErrorCategory::transaction_conflict:
            return "TRANSACTION_CONFLICT";
        case ErrorCategory::constraint_violation:
            return "CONSTRAINT_VIOLATION";
        case ErrorCategory::resource_limit:
            return "RESOURCE_LIMIT";
        case ErrorCategory::cursor_not_found:
            return "CURSOR_NOT_FOUND";
        case ErrorCategory::internal_error:
            return "INTERNAL_ERROR";
        case ErrorCategory::transport_error:
            return "TRANSPORT_ERROR";
        case ErrorCategory::outcome_unknown:
            return "OUTCOME_UNKNOWN";
    }
    return "UNKNOWN";
}

Error::Error(const ErrorCategory category, std::string message)
    : std::runtime_error(std::move(message))
    , category_(category)
{
}

ErrorCategory Error::category() const noexcept
{
    return category_;
}

} // namespace sessiongw
