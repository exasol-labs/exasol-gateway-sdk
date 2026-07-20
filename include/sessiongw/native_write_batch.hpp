#ifndef SESSIONGW_NATIVE_WRITE_BATCH_HPP
#define SESSIONGW_NATIVE_WRITE_BATCH_HPP

#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace sessiongw
{

inline constexpr std::uint32_t native_write_batch_magic = 0x53475742U; // "SGWB"
inline constexpr std::uint32_t native_write_batch_version = 2U;
inline constexpr std::size_t native_write_batch_alignment = 16U;
inline constexpr std::uint32_t native_write_batch_size_width = 8U;
inline constexpr std::uint32_t native_write_batch_scalar_abi = 1U;
inline constexpr std::uint32_t native_write_batch_little_endian = 1U;
inline constexpr std::uint32_t native_write_batch_big_endian = 2U;
inline constexpr std::uint32_t native_write_batch_scalar_endianness =
    std::endian::native == std::endian::little ? native_write_batch_little_endian
                                               : native_write_batch_big_endian;
inline constexpr std::uint8_t native_write_not_null = 0x00U;
inline constexpr std::uint8_t native_write_null = 0xffU;

struct NativeDate final
{
    std::uint32_t value = 0;

    [[nodiscard]] static NativeDate fromYmd(std::uint32_t year,
                                            std::uint32_t month,
                                            std::uint32_t day);
};

struct NativeDecimal128 final
{
    std::uint64_t low = 0;
    std::int64_t high = 0;

    [[nodiscard]] static NativeDecimal128 fromInt64(std::int64_t value) noexcept;
};

struct NativeTimestamp final
{    std::uint32_t nanosecond = 0;
    std::uint32_t seconds_since_midnight = 0;
    NativeDate date;
    std::uint32_t padding = 0;

    [[nodiscard]] static NativeTimestamp fromComponents(std::uint32_t year,
                                                        std::uint32_t month,
                                                        std::uint32_t day,
                                                        std::uint32_t hour,
                                                        std::uint32_t minute,
                                                        std::uint32_t second,
                                                        std::uint32_t nanosecond);
};

class NativeWriteBatchBuilder final
{
public:
    explicit NativeWriteBatchBuilder(std::vector<std::uint8_t>& output);

    void begin(std::uint32_t row_count, std::uint32_t column_count, bool clear_output = true);
    void appendFixedColumn(std::span<const std::uint8_t> null_vector,
                           std::span<const std::uint8_t> fixed_data);
    void appendVariableColumn(std::span<const std::uint8_t> null_vector,
                              std::span<const std::size_t> sizes,
                              std::span<const std::uint8_t> variable_data);

    // Typed adapter-facing helpers. Decimal values are unscaled signed integers;
    // callers select the width that matches the Arrow schema supplied when the
    // table operation is opened.
    void appendBooleanColumn(std::span<const std::optional<bool>> values);
    void appendDecimal32Column(std::span<const std::optional<std::int32_t>> values);
    void appendDecimal64Column(std::span<const std::optional<std::int64_t>> values);
    void appendDecimal128Column(std::span<const std::optional<NativeDecimal128>> values);
    void appendDoubleColumn(std::span<const std::optional<double>> values);
    void appendDateColumn(std::span<const std::optional<NativeDate>> values);
    void appendTimestampColumn(std::span<const std::optional<NativeTimestamp>> values);
    void appendStringColumn(std::span<const std::optional<std::string_view>> values);
    void finish();

    [[nodiscard]] std::span<const std::uint8_t> bytes() const noexcept;
    [[nodiscard]] std::size_t batchOffset() const noexcept;
    [[nodiscard]] std::size_t batchSize() const noexcept;

private:
    void ensureStarted() const;
    void appendNullVector(std::span<const std::uint8_t> null_vector);
    void alignOutput();

    std::vector<std::uint8_t>& output_;
    std::size_t batchOffset_ = 0;
    std::uint32_t rowCount_ = 0;
    std::uint32_t columnCount_ = 0;
    std::uint32_t appendedColumns_ = 0;
    bool started_ = false;
};

void appendNativeWriteU32(std::vector<std::uint8_t>& output, std::uint32_t value);
void appendNativeWriteU64(std::vector<std::uint8_t>& output, std::uint64_t value);
void appendNativeWriteBytes(std::vector<std::uint8_t>& output, const void* data, std::size_t size);
void alignNativeWriteBatch(std::vector<std::uint8_t>& output);

} // namespace sessiongw

#endif // SESSIONGW_NATIVE_WRITE_BATCH_HPP
