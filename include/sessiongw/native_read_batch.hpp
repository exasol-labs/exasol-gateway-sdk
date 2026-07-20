#ifndef SESSIONGW_NATIVE_READ_BATCH_HPP
#define SESSIONGW_NATIVE_READ_BATCH_HPP

#include <arrow/type_fwd.h>

#include <cstdint>
#include <memory>
#include <span>

namespace sessiongw
{

/// Validates stable native table read batch v1 against the cursor's Arrow
/// schema. Throws sessiongw::Error(PROTOCOL_ERROR/UNSUPPORTED_TYPE) on failure.
void validateNativeReadBatch(std::span<const std::uint8_t> bytes,
                             const std::shared_ptr<arrow::Schema>& schema);

} // namespace sessiongw

#endif // SESSIONGW_NATIVE_READ_BATCH_HPP
