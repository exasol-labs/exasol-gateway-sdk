#include <sessiongw/c_api.h>

#include <gtest/gtest.h>

#include <string>

TEST(SessionGatewayCApi, containsFailuresAtTheAbiBoundary)
{
    EXPECT_NE(sessiongw_c_connect(nullptr, nullptr), 0);
    EXPECT_NE(sessiongw_c_connect_with_client_name(nullptr, nullptr, nullptr), 0);
    EXPECT_EQ(sessiongw_c_last_error_category(), 1U);
    EXPECT_FALSE(std::string(sessiongw_c_last_error_message()).empty());
}

TEST(SessionGatewayCApi, validatesSqlArgumentsWithoutThrowing)
{
    sessiongw_c_options options{};
    EXPECT_NE(sessiongw_c_execute_sql(&options, nullptr), 0);
    EXPECT_EQ(sessiongw_c_last_error_category(), 1U);
}

TEST(SessionGatewayCApi, nullAccessorsAreDeterministic)
{
    std::size_t size = 99U;
    EXPECT_EQ(sessiongw_c_cursor_id(nullptr), 0U);
    EXPECT_EQ(sessiongw_c_cursor_schema_ipc(nullptr, &size), nullptr);
    EXPECT_EQ(size, 0U);
    EXPECT_EQ(sessiongw_c_fetch_end(nullptr), 0);
    EXPECT_EQ(sessiongw_c_metadata_has_row_count(nullptr), 0);
    EXPECT_EQ(sessiongw_c_metadata_row_count(nullptr), 0U);
    EXPECT_EQ(sessiongw_c_native_fetch_cursor_id(nullptr), 0U);
    EXPECT_EQ(sessiongw_c_native_fetch_end(nullptr), 0);
    EXPECT_EQ(sessiongw_c_native_fetch_row_count(nullptr), 0U);
    EXPECT_EQ(sessiongw_c_native_fetch_column_count(nullptr), 0U);
    EXPECT_EQ(sessiongw_c_native_fetch_column_nulls(nullptr, 0U, &size), nullptr);
    EXPECT_EQ(size, 0U);
    EXPECT_EQ(sessiongw_c_operation_id(nullptr), 0U);
    sessiongw_c_transport_profile profile{};
    EXPECT_NE(sessiongw_c_transport_profile_get(nullptr, &profile), 0);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
