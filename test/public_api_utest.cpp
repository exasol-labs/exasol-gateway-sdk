#include <sessiongw/sessiongw.hpp>

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <array>
#include <type_traits>
#include <vector>

namespace
{
void appendU32(std::vector<std::uint8_t>& out, const std::uint32_t value)
{
    out.push_back(static_cast<std::uint8_t>(value >> 24U));
    out.push_back(static_cast<std::uint8_t>(value >> 16U));
    out.push_back(static_cast<std::uint8_t>(value >> 8U));
    out.push_back(static_cast<std::uint8_t>(value));
}

std::vector<std::uint8_t> oneInt64NativeReadBatch()
{
    std::vector<std::uint8_t> bytes;
    appendU32(bytes, 0x53475242U);
    appendU32(bytes, 1U);
    appendU32(bytes, 1U);
    appendU32(bytes, 1U);
    appendU32(bytes, 1U);
    appendU32(bytes, 1U);
    bytes.insert(bytes.end(), {2U, 0U, 0U, 0U});
    appendU32(bytes, 0U);
    appendU32(bytes, 2U);
    appendU32(bytes, 1U);
    bytes.push_back(0U);
    appendU32(bytes, 8U);
    bytes.insert(bytes.end(), {42U, 0U, 0U, 0U, 0U, 0U, 0U, 0U});
    return bytes;
}
} // namespace

TEST(SessionGatewayPublicApi, requiresExplicitCredentialsAndVerifiesTlsByDefault)
{
    const sessiongw::WebSocketOptions options;
    EXPECT_TRUE(options.user.empty());
    EXPECT_TRUE(options.password.empty());
    EXPECT_EQ(options.tls_mode, sessiongw::WebSocketTlsMode::tls_verify);
    try
    {
        (void) sessiongw::WebSocketConnection::connectAndLogin(options);
        FAIL() << "missing credentials unexpectedly reached the network";
    }
    catch (const sessiongw::Error& error)
    {
        EXPECT_EQ(error.category(), sessiongw::ErrorCategory::authentication_failed);
    }
}

TEST(SessionGatewayPublicApi, exposesTypedArrowAndNativeWriteBoundary)
{
    static_assert(!std::is_copy_constructible_v<sessiongw::Session>);
    static_assert(std::is_move_constructible_v<sessiongw::Session>);

    sessiongw::TableMetadata metadata;
    metadata.table = {"S", "T"};
    metadata.schema = arrow::schema({arrow::field("ID", arrow::int64(), false)});
    ASSERT_EQ(metadata.schema->field(0)->name(), "ID");

    std::vector<std::uint8_t> bytes;
    sessiongw::NativeWriteBatchBuilder builder(bytes);
    builder.begin(1, 1);
    const std::array<std::uint8_t, 1> nulls{sessiongw::native_write_not_null};
    const std::array<std::int64_t, 1> values{42};
    builder.appendFixedColumn(
        nulls,
        std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(values.data()),
                                      sizeof(values)));
    builder.finish();
    EXPECT_FALSE(bytes.empty());
}

TEST(SessionGatewayPublicApi, validatesNativeReadFramingAndRejectsMalformedViews)
{
    const auto schema = arrow::schema({arrow::field("ID", arrow::int64(), false)});
    const std::vector<std::uint8_t> valid = oneInt64NativeReadBatch();
    EXPECT_NO_THROW(sessiongw::validateNativeReadBatch(valid, schema));

    std::vector<std::uint8_t> invalidNull = valid;
    invalidNull[40U] = 1U;
    EXPECT_THROW(sessiongw::validateNativeReadBatch(invalidNull, schema), sessiongw::Error);

    std::vector<std::uint8_t> truncated = valid;
    truncated.pop_back();
    EXPECT_THROW(sessiongw::validateNativeReadBatch(truncated, schema), sessiongw::Error);

    std::vector<std::uint8_t> trailing = valid;
    trailing.push_back(0U);
    EXPECT_THROW(sessiongw::validateNativeReadBatch(trailing, schema), sessiongw::Error);

    const auto wrongSchema = arrow::schema({arrow::field("ID", arrow::utf8(), false)});
    EXPECT_THROW(sessiongw::validateNativeReadBatch(valid, wrongSchema), sessiongw::Error);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
