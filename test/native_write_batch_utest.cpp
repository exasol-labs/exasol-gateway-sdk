#include <sessiongw/native_write_batch.hpp>

#include <sessiongw/errors.hpp>

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace
{

std::uint32_t readU32(const std::vector<std::uint8_t>& bytes, const std::size_t offset)
{
    return (static_cast<std::uint32_t>(bytes[offset]) << 24U) |
           (static_cast<std::uint32_t>(bytes[offset + 1U]) << 16U) |
           (static_cast<std::uint32_t>(bytes[offset + 2U]) << 8U) |
           static_cast<std::uint32_t>(bytes[offset + 3U]);
}

std::uint64_t readU64(const std::vector<std::uint8_t>& bytes, const std::size_t offset)
{
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < 8U; ++index)
    {
        value = (value << 8U) | bytes[offset + index];
    }
    return value;
}

void alignOffset(std::size_t& offset)
{
    offset = (offset + sessiongw::native_write_batch_alignment - 1U) &
             ~(sessiongw::native_write_batch_alignment - 1U);
}

} // namespace

TEST(SessionGatewayNativeWriteBatch, buildsFixedAndVariableColumnsInCallerOwnedBuffer)
{
    std::vector<std::uint8_t> buffer;
    buffer.reserve(256);
    sessiongw::NativeWriteBatchBuilder builder(buffer);

    builder.begin(2, 2);
    const std::array<std::uint8_t, 2> nulls{sessiongw::native_write_not_null,
                                            sessiongw::native_write_not_null};
    const std::array<std::int32_t, 2> ids{1, 2};
    builder.appendFixedColumn(nulls,
                              std::span<const std::uint8_t>(
                                  reinterpret_cast<const std::uint8_t*>(ids.data()),
                                  sizeof(ids)));

    const std::array<std::size_t, 2> sizes{5, 3};
    const std::array<std::uint8_t, 8> names{'A', 'l', 'i', 'c', 'e', 'B', 'o', 'b'};
    builder.appendVariableColumn(nulls, sizes, names);
    builder.finish();

    EXPECT_EQ(builder.batchOffset(), 0);
    EXPECT_EQ(builder.batchSize(), buffer.size());
    EXPECT_EQ(readU32(buffer, 0), sessiongw::native_write_batch_magic);
    EXPECT_EQ(readU32(buffer, 4), sessiongw::native_write_batch_version);
    EXPECT_EQ(readU32(buffer, 8), 2);
    EXPECT_EQ(readU32(buffer, 12), 2);
    EXPECT_EQ(readU32(buffer, 16), sessiongw::native_write_batch_alignment);
    EXPECT_EQ(readU32(buffer, 20), sessiongw::native_write_batch_size_width);
    EXPECT_EQ(readU32(buffer, 24), sessiongw::native_write_batch_scalar_endianness);
    EXPECT_EQ(readU32(buffer, 28), sessiongw::native_write_batch_scalar_abi);

    std::size_t offset = 32;
    alignOffset(offset);
    ASSERT_LE(offset + nulls.size(), buffer.size());
    EXPECT_TRUE(std::equal(nulls.begin(), nulls.end(), buffer.begin() + static_cast<std::ptrdiff_t>(offset)));
    offset += nulls.size();
    alignOffset(offset);
    ASSERT_LE(offset + sizeof(ids), buffer.size());
    EXPECT_EQ(std::memcmp(buffer.data() + offset, ids.data(), sizeof(ids)), 0);
    offset += sizeof(ids);

    alignOffset(offset);
    ASSERT_LE(offset + nulls.size(), buffer.size());
    EXPECT_TRUE(std::equal(nulls.begin(), nulls.end(), buffer.begin() + static_cast<std::ptrdiff_t>(offset)));
    offset += nulls.size();
    alignOffset(offset);
    ASSERT_LE(offset + sizes.size() * sizeof(std::uint64_t), buffer.size());
    EXPECT_EQ(readU64(buffer, offset), sizes[0]);
    offset += sizeof(std::uint64_t);
    EXPECT_EQ(readU64(buffer, offset), sizes[1]);
    offset += sizeof(std::uint64_t);
    EXPECT_EQ(readU32(buffer, offset), names.size());
    offset += sizeof(std::uint32_t);
    alignOffset(offset);
    ASSERT_LE(offset + names.size(), buffer.size());
    EXPECT_TRUE(std::equal(names.begin(), names.end(), buffer.begin() + static_cast<std::ptrdiff_t>(offset)));
    offset += names.size();
    alignOffset(offset);
    EXPECT_EQ(offset, buffer.size());
}

TEST(SessionGatewayNativeWriteBatch, appendsBatchIntoExistingPayload)
{
    std::vector<std::uint8_t> buffer{1, 2, 3};
    sessiongw::NativeWriteBatchBuilder builder(buffer);
    builder.begin(1, 1, false);
    const std::array<std::uint8_t, 1> nulls{sessiongw::native_write_not_null};
    const std::array<std::int64_t, 1> values{42};
    builder.appendFixedColumn(nulls,
                              std::span<const std::uint8_t>(
                                  reinterpret_cast<const std::uint8_t*>(values.data()),
                                  sizeof(values)));
    builder.finish();

    EXPECT_EQ(builder.batchOffset(), 3);
    EXPECT_EQ(readU32(buffer, 3), sessiongw::native_write_batch_magic);
    // Alignment is relative to the batch start, not the caller-owned buffer.
    EXPECT_EQ(builder.batchSize() % sessiongw::native_write_batch_alignment, 0U);
}

TEST(SessionGatewayNativeWriteBatch, buildsAllPublicTypedColumns)
{
    std::vector<std::uint8_t> buffer;
    sessiongw::NativeWriteBatchBuilder builder(buffer);
    builder.begin(2, 8);
    const std::array<std::optional<bool>, 2> booleans{false, true};
    const std::array<std::optional<std::int32_t>, 2> decimals32{-1, 1};
    const std::array<std::optional<std::int64_t>, 2> decimals64{-2, 2};
    const std::array<std::optional<sessiongw::NativeDecimal128>, 2> decimals128{
        sessiongw::NativeDecimal128::fromInt64(-3), sessiongw::NativeDecimal128::fromInt64(3)};
    const std::array<std::optional<double>, 2> doubles{-4.5, 4.5};
    const std::array<std::optional<sessiongw::NativeDate>, 2> dates{
        sessiongw::NativeDate::fromYmd(2000, 2, 29), std::nullopt};
    const std::array<std::optional<sessiongw::NativeTimestamp>, 2> timestamps{
        sessiongw::NativeTimestamp::fromComponents(2026, 7, 17, 12, 34, 56, 123456789),
        std::nullopt};
    const std::array<std::optional<std::string_view>, 2> strings{"typed", std::nullopt};
    builder.appendBooleanColumn(booleans);
    builder.appendDecimal32Column(decimals32);
    builder.appendDecimal64Column(decimals64);
    builder.appendDecimal128Column(decimals128);
    builder.appendDoubleColumn(doubles);
    builder.appendDateColumn(dates);
    builder.appendTimestampColumn(timestamps);
    builder.appendStringColumn(strings);
    EXPECT_NO_THROW(builder.finish());
    EXPECT_EQ(readU32(buffer, 8), 2U);
    EXPECT_EQ(readU32(buffer, 12), 8U);
}

TEST(SessionGatewayNativeWriteBatch, rejectsInvalidPublicDateAndTimestamp)
{
    EXPECT_THROW((void)sessiongw::NativeDate::fromYmd(2025, 2, 29), sessiongw::Error);
    EXPECT_THROW((void)sessiongw::NativeTimestamp::fromComponents(2026, 1, 1, 24, 0, 0, 0),
                 sessiongw::Error);
    EXPECT_THROW((void)sessiongw::NativeTimestamp::fromComponents(2026, 1, 1, 0, 0, 0, 1000000000U),
                 sessiongw::Error);
}

TEST(SessionGatewayNativeWriteBatch, rejectsInvalidNullVectorByte)
{
    std::vector<std::uint8_t> buffer;
    sessiongw::NativeWriteBatchBuilder builder(buffer);
    builder.begin(1, 1);
    const std::array<std::uint8_t, 1> invalidNulls{1};
    const std::array<std::int32_t, 1> values{1};
    EXPECT_THROW(builder.appendFixedColumn(
                     invalidNulls,
                     std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(values.data()),
                                                   sizeof(values))),
                 sessiongw::Error);
}

TEST(SessionGatewayNativeWriteBatch, rejectsVariableSizesThatDoNotExactlyCoverData)
{
    const std::array<std::uint8_t, 1> nulls{sessiongw::native_write_not_null};
    const std::array<std::uint8_t, 3> data{'a', 'b', 'c'};
    for (const std::size_t describedSize : {2U, 4U})
    {
        std::vector<std::uint8_t> buffer;
        sessiongw::NativeWriteBatchBuilder builder(buffer);
        builder.begin(1, 1);
        const std::array<std::size_t, 1> sizes{describedSize};
        EXPECT_THROW(builder.appendVariableColumn(nulls, sizes, data), sessiongw::Error);
    }
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
