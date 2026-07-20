#include <sessiongw/native_write_batch.hpp>

#include <sessiongw/errors.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <type_traits>

namespace sessiongw
{
namespace
{

void writeNativeU64(std::uint8_t* const output, const std::uint64_t value)
{
    for (std::size_t index = 0; index < sizeof(value); ++index)
    {
        const unsigned shift = static_cast<unsigned>(56U - index * 8U);
        output[index] = static_cast<std::uint8_t>((value >> shift) & 0xffU);
    }
}

template <typename Input, typename Native, typename Convert>
void appendTypedFixedColumn(NativeWriteBatchBuilder& builder,
                            const std::span<const std::optional<Input>> values,
                            Convert&& convert)
{
    static_assert(std::is_trivially_copyable_v<Native>);
    std::vector<std::uint8_t> nulls;
    std::vector<std::uint8_t> fixed;
    nulls.reserve(values.size());
    fixed.reserve(values.size() * sizeof(Native));
    for (const std::optional<Input>& value : values)
    {
        nulls.push_back(value.has_value() ? native_write_not_null : native_write_null);
        const Native nativeValue = value.has_value() ? convert(*value) : Native{};
        appendNativeWriteBytes(fixed, &nativeValue, sizeof(nativeValue));
    }
    builder.appendFixedColumn(nulls, fixed);
}

template <typename T>
void appendIdentityFixedColumn(NativeWriteBatchBuilder& builder,
                               const std::span<const std::optional<T>> values)
{
    appendTypedFixedColumn<T, T>(builder, values, [](const T value) { return value; });
}

} // namespace

NativeDate NativeDate::fromYmd(const std::uint32_t year,
                               const std::uint32_t month,
                               const std::uint32_t day)
{
    const std::chrono::year_month_day date{std::chrono::year(static_cast<int>(year)),
                                           std::chrono::month(month),
                                           std::chrono::day(day)};
    if (year > 9999U || !date.ok())
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW native date is invalid");
    }
    return NativeDate{(year << 16U) | (month << 8U) | day};
}

NativeDecimal128 NativeDecimal128::fromInt64(const std::int64_t value) noexcept
{
    return NativeDecimal128{static_cast<std::uint64_t>(value), value < 0 ? -1 : 0};
}

NativeTimestamp NativeTimestamp::fromComponents(const std::uint32_t year,
                                                 const std::uint32_t month,
                                                 const std::uint32_t day,
                                                 const std::uint32_t hour,
                                                 const std::uint32_t minute,
                                                 const std::uint32_t second,
                                                 const std::uint32_t nanosecond)
{
    if (hour >= 24U || minute >= 60U || second >= 60U || nanosecond >= 1000000000U)
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW native timestamp is invalid");
    }
    return NativeTimestamp{nanosecond,
                           hour * 3600U + minute * 60U + second,
                           NativeDate::fromYmd(year, month, day),
                           0U};
}

void appendNativeWriteU32(std::vector<std::uint8_t>& output, const std::uint32_t value)
{
    output.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xffU));
    output.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xffU));
    output.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
    output.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

void appendNativeWriteU64(std::vector<std::uint8_t>& output, const std::uint64_t value)
{
    const std::size_t offset = output.size();
    if (offset > std::numeric_limits<std::size_t>::max() - sizeof(value))
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW native size word overflows");
    }
    output.resize(offset + sizeof(value));
    writeNativeU64(output.data() + offset, value);
}

void appendNativeWriteBytes(std::vector<std::uint8_t>& output,
                            const void* const data,
                            const std::size_t size)
{
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    output.insert(output.end(), bytes, bytes + size);
}

void alignNativeWriteBatch(std::vector<std::uint8_t>& output)
{
    const std::size_t alignedSize =
        (output.size() + native_write_batch_alignment - 1U) & ~(native_write_batch_alignment - 1U);
    output.resize(alignedSize, 0U);
}

NativeWriteBatchBuilder::NativeWriteBatchBuilder(std::vector<std::uint8_t>& output)
    : output_(output)
{
}

void NativeWriteBatchBuilder::begin(const std::uint32_t row_count,
                                    const std::uint32_t column_count,
                                    const bool clear_output)
{
    if (clear_output)
    {
        output_.clear();
    }
    batchOffset_ = output_.size();
    rowCount_ = row_count;
    columnCount_ = column_count;
    appendedColumns_ = 0;
    appendNativeWriteU32(output_, native_write_batch_magic);
    appendNativeWriteU32(output_, native_write_batch_version);
    appendNativeWriteU32(output_, row_count);
    appendNativeWriteU32(output_, column_count);
    appendNativeWriteU32(output_, static_cast<std::uint32_t>(native_write_batch_alignment));
    appendNativeWriteU32(output_, native_write_batch_size_width);
    appendNativeWriteU32(output_, native_write_batch_scalar_endianness);
    appendNativeWriteU32(output_, native_write_batch_scalar_abi);
    started_ = true;
}

void NativeWriteBatchBuilder::appendFixedColumn(const std::span<const std::uint8_t> null_vector,
                                                const std::span<const std::uint8_t> fixed_data)
{
    ensureStarted();
    appendNullVector(null_vector);
    alignOutput();
    output_.insert(output_.end(), fixed_data.begin(), fixed_data.end());
    ++appendedColumns_;
}

void NativeWriteBatchBuilder::appendVariableColumn(const std::span<const std::uint8_t> null_vector,
                                                   const std::span<const std::size_t> sizes,
                                                   const std::span<const std::uint8_t> variable_data)
{
    ensureStarted();
    if (sizes.size() != rowCount_)
    {
        throw Error(ErrorCategory::protocol_error,
                    "SessionGW native variable-size count does not match row count");
    }
    if (variable_data.size() > std::numeric_limits<std::uint32_t>::max())
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW native variable column data is too large");
    }
    std::size_t describedDataBytes = 0;
    for (const std::size_t size : sizes)
    {
        if (size > variable_data.size() - describedDataBytes)
        {
            throw Error(ErrorCategory::protocol_error,
                        "SessionGW native variable sizes exceed the data extent");
        }
        describedDataBytes += size;
    }
    if (describedDataBytes != variable_data.size())
    {
        throw Error(ErrorCategory::protocol_error,
                    "SessionGW native variable sizes do not cover the data extent");
    }
    appendNullVector(null_vector);
    alignOutput();
    if (sizes.size() > (std::numeric_limits<std::size_t>::max() - output_.size()) /
                           sizeof(std::uint64_t))
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW native size vector overflows");
    }
    const std::size_t sizesOffset = output_.size();
    output_.resize(sizesOffset + sizes.size() * sizeof(std::uint64_t));
    for (std::size_t row = 0; row < sizes.size(); ++row)
    {
        writeNativeU64(output_.data() + sizesOffset + row * sizeof(std::uint64_t),
                       static_cast<std::uint64_t>(sizes[row]));
    }
    appendNativeWriteU32(output_, static_cast<std::uint32_t>(variable_data.size()));
    alignOutput();
    output_.insert(output_.end(), variable_data.begin(), variable_data.end());
    ++appendedColumns_;
}

void NativeWriteBatchBuilder::appendBooleanColumn(
    const std::span<const std::optional<bool>> values)
{
    appendTypedFixedColumn<bool, std::uint8_t>(*this, values, [](const bool value) {
        return static_cast<std::uint8_t>(value ? 0xffU : 0x00U);
    });
}

void NativeWriteBatchBuilder::appendDecimal32Column(
    const std::span<const std::optional<std::int32_t>> values)
{
    appendIdentityFixedColumn(*this, values);
}

void NativeWriteBatchBuilder::appendDecimal64Column(
    const std::span<const std::optional<std::int64_t>> values)
{
    appendIdentityFixedColumn(*this, values);
}

void NativeWriteBatchBuilder::appendDecimal128Column(
    const std::span<const std::optional<NativeDecimal128>> values)
{
    std::vector<std::uint8_t> nulls;
    std::vector<std::uint8_t> fixed;
    nulls.reserve(values.size());
    fixed.reserve(values.size() * 16U);
    for (const std::optional<NativeDecimal128>& value : values)
    {
        nulls.push_back(value.has_value() ? native_write_not_null : native_write_null);
        const NativeDecimal128 nativeValue = value.value_or(NativeDecimal128{});
        const std::uint64_t high = static_cast<std::uint64_t>(nativeValue.high);
        if constexpr (std::endian::native == std::endian::little)
        {
            appendNativeWriteBytes(fixed, &nativeValue.low, sizeof(nativeValue.low));
            appendNativeWriteBytes(fixed, &high, sizeof(high));
        }
        else
        {
            appendNativeWriteBytes(fixed, &high, sizeof(high));
            appendNativeWriteBytes(fixed, &nativeValue.low, sizeof(nativeValue.low));
        }
    }
    appendFixedColumn(nulls, fixed);
}

void NativeWriteBatchBuilder::appendDoubleColumn(
    const std::span<const std::optional<double>> values)
{
    appendIdentityFixedColumn(*this, values);
}

void NativeWriteBatchBuilder::appendDateColumn(
    const std::span<const std::optional<NativeDate>> values)
{
    appendTypedFixedColumn<NativeDate, std::uint32_t>(*this, values, [](const NativeDate value) {
        return value.value;
    });
}

void NativeWriteBatchBuilder::appendTimestampColumn(
    const std::span<const std::optional<NativeTimestamp>> values)
{
    appendIdentityFixedColumn(*this, values);
}

void NativeWriteBatchBuilder::appendStringColumn(
    const std::span<const std::optional<std::string_view>> values)
{
    std::vector<std::uint8_t> nulls;
    std::vector<std::size_t> sizes;
    std::vector<std::uint8_t> variable;
    nulls.reserve(values.size());
    sizes.reserve(values.size());
    for (const std::optional<std::string_view>& value : values)
    {
        nulls.push_back(value.has_value() ? native_write_not_null : native_write_null);
        const std::string_view text = value.value_or(std::string_view{});
        sizes.push_back(text.size());
        variable.insert(variable.end(), text.begin(), text.end());
    }
    appendVariableColumn(nulls, sizes, variable);
}

void NativeWriteBatchBuilder::finish()
{
    ensureStarted();
    if (appendedColumns_ != columnCount_)
    {
        throw Error(ErrorCategory::protocol_error,
                    "SessionGW native column count does not match batch header");
    }
    alignOutput();
    started_ = false;
}

std::span<const std::uint8_t> NativeWriteBatchBuilder::bytes() const noexcept
{
    return std::span<const std::uint8_t>(output_.data() + static_cast<std::ptrdiff_t>(batchOffset_),
                                         batchSize());
}

std::size_t NativeWriteBatchBuilder::batchOffset() const noexcept
{
    return batchOffset_;
}

std::size_t NativeWriteBatchBuilder::batchSize() const noexcept
{
    return output_.size() - batchOffset_;
}

void NativeWriteBatchBuilder::ensureStarted() const
{
    if (!started_)
    {
        throw Error(ErrorCategory::protocol_error, "SessionGW native write batch builder is not open");
    }
}

void NativeWriteBatchBuilder::appendNullVector(const std::span<const std::uint8_t> null_vector)
{
    if (appendedColumns_ >= columnCount_ || null_vector.size() != rowCount_)
    {
        throw Error(ErrorCategory::protocol_error,
                    "SessionGW native null-vector size or column count is invalid");
    }
    if (!std::all_of(null_vector.begin(), null_vector.end(), [](const std::uint8_t value) {
            return value == native_write_not_null || value == native_write_null;
        }))
    {
        throw Error(ErrorCategory::protocol_error,
                    "SessionGW native write batch null vector must contain only 0x00 or 0xff");
    }
    alignOutput();
    output_.insert(output_.end(), null_vector.begin(), null_vector.end());
}

void NativeWriteBatchBuilder::alignOutput()
{
    const std::size_t relativeSize = output_.size() - batchOffset_;
    if (relativeSize > std::numeric_limits<std::size_t>::max() -
                           (native_write_batch_alignment - 1U))
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW native batch alignment overflows");
    }
    const std::size_t alignedRelativeSize =
        (relativeSize + native_write_batch_alignment - 1U) & ~(native_write_batch_alignment - 1U);
    if (batchOffset_ > std::numeric_limits<std::size_t>::max() - alignedRelativeSize)
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW native batch size overflows");
    }
    output_.resize(batchOffset_ + alignedRelativeSize, 0U);
}

} // namespace sessiongw
