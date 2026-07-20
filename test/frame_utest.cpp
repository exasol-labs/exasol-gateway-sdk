#include <sessiongw/capabilities.hpp>
#include <sessiongw/client.hpp>
#include <sessiongw/errors.hpp>
#include <sessiongw/frame.hpp>
#include <sessiongw/upgrade.hpp>
#include <sessiongw/websocket.hpp>

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <sys/socket.h>
#include <type_traits>
#include <unistd.h>
#include <vector>

namespace
{

std::array<int, 2> socketPair()
{
    std::array<int, 2> sockets{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.data()) != 0)
    {
        throw std::runtime_error("socketpair failed");
    }
    return sockets;
}

void writeAll(const int fd, const std::span<const std::uint8_t> bytes)
{
    std::size_t offset = 0;
    while (offset < bytes.size())
    {
        const ssize_t rc = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
        if (rc <= 0)
        {
            throw std::runtime_error("socket write failed");
        }
        offset += static_cast<std::size_t>(rc);
    }
}

std::vector<std::uint8_t> readExact(const int fd, const std::size_t size)
{
    std::vector<std::uint8_t> bytes(size);
    std::size_t offset = 0;
    while (offset < size)
    {
        const ssize_t rc = ::recv(fd, bytes.data() + offset, size - offset, 0);
        if (rc <= 0)
        {
            throw std::runtime_error("socket read failed");
        }
        offset += static_cast<std::size_t>(rc);
    }
    return bytes;
}

std::vector<std::uint8_t> serverFrame(const std::uint8_t firstByte,
                                      const std::span<const std::uint8_t> payload)
{
    if (payload.size() > 125U)
    {
        throw std::runtime_error("test helper only supports short frames");
    }
    std::vector<std::uint8_t> frame{firstByte, static_cast<std::uint8_t>(payload.size())};
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

} // namespace

TEST(SessionGatewayApi, protocolFacingEnumsHaveStableUnderlyingValues)
{
    static_assert(std::is_same_v<std::underlying_type_t<sessiongw::MessageType>, std::uint16_t>);
    static_assert(std::is_same_v<std::underlying_type_t<sessiongw::Capability>, std::uint16_t>);
    static_assert(std::is_same_v<std::underlying_type_t<sessiongw::ErrorCategory>, std::uint16_t>);
    static_assert(std::is_same_v<std::underlying_type_t<sessiongw::TlsMode>, std::uint16_t>);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::ping), 3);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::describe_table), 8);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::get_table_version_result), 11);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::open_pushed_query), 12);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::close_cursor), 16);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::open_table_scan), 17);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::open_table_insert), 18);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::rollback), 25);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::open_table_update), 26);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::delete_rows), 29);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::fetch_native), 31);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::fetch_native_result), 32);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::MessageType::fetch_positioned_rows_native), 33);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::arrow_ipc_batch_v1), 2);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::table_write_v1), 6);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::native_table_write_batch_v1), 8);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::table_delete_v1), 10);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::outcome_recovery_v1), 11);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::native_table_write_batch_v2), 12);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::Capability::native_table_read_batch_v1), 13);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::ErrorCategory::object_not_found), 4);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::ErrorCategory::outcome_unknown), 13);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::TlsMode::disable_for_test_only), 2);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::WebSocketTlsMode::plain_for_test_only), 1);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::WebSocketTlsMode::tls_verify), 2);
    EXPECT_EQ(static_cast<std::uint16_t>(sessiongw::WebSocketTlsMode::tls_skip_verify_for_test_only), 3);
    EXPECT_EQ(sessiongw::enter_session_gateway_command, 127);
}

TEST(SessionGatewayWebSocket, defaultsToVerifiedTls)
{
    const sessiongw::WebSocketOptions options;
    EXPECT_EQ(options.tls_mode, sessiongw::WebSocketTlsMode::tls_verify);
    EXPECT_TRUE(options.ca_file.empty());
}

TEST(SessionGatewayFrame, encodesAndDecodesHeaderAndPayload)
{
    sessiongw::Frame frame;
    frame.header.message_type = sessiongw::MessageType::ping;
    frame.header.flags = 7;
    frame.header.request_id = 42;
    frame.payload = {1, 2, 3, 4};

    const std::vector<std::uint8_t> encoded = sessiongw::encodeFrame(frame);
    const sessiongw::Frame decoded = sessiongw::decodeFrame(encoded);

    EXPECT_EQ(decoded.header.magic, sessiongw::frame_magic);
    EXPECT_EQ(decoded.header.version, sessiongw::protocol_version_v1);
    EXPECT_EQ(decoded.header.message_type, sessiongw::MessageType::ping);
    EXPECT_EQ(decoded.header.flags, 7);
    EXPECT_EQ(decoded.header.request_id, 42);
    EXPECT_EQ(decoded.header.payload_length, 4);
    EXPECT_EQ(decoded.payload, frame.payload);
}

TEST(SessionGatewayFrame, rejectsInvalidMagic)
{
    sessiongw::Frame frame;
    std::vector<std::uint8_t> encoded = sessiongw::encodeFrame(frame);
    encoded[0] = 0;

    EXPECT_THROW({ (void)sessiongw::decodeFrame(encoded); }, sessiongw::Error);
}

TEST(SessionGatewayFrame, rejectsMismatchingPayloadLength)
{
    sessiongw::Frame frame;
    frame.payload = {1, 2, 3};
    std::vector<std::uint8_t> encoded = sessiongw::encodeFrame(frame);
    encoded.pop_back();

    EXPECT_THROW({ (void)sessiongw::decodeFrame(encoded); }, sessiongw::Error);
}

TEST(SessionGatewayWebSocket, rejectsMismatchedSessionGatewayResponseId)
{
    const auto sockets = socketPair();
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
    sessiongw::Frame response;
    response.header.message_type = sessiongw::MessageType::ok;
    response.header.request_id = 42;
    const std::vector<std::uint8_t> encoded = sessiongw::encodeFrame(response);
    writeAll(sockets[1], serverFrame(0x82U, encoded));

    try
    {
        (void)connection.receiveFrame(41);
        FAIL() << "expected request-id mismatch";
    }
    catch (const sessiongw::Error& error)
    {
        EXPECT_EQ(error.category(), sessiongw::ErrorCategory::protocol_error);
    }
    ::close(sockets[1]);
}

TEST(SessionGatewayWebSocket, reassemblesFragmentsAcrossPingAndSendsMaskedPong)
{
    const auto sockets = socketPair();
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
    sessiongw::Frame response;
    response.header.message_type = sessiongw::MessageType::ok;
    response.header.request_id = 9;
    response.payload = {1, 2, 3, 4};
    const std::vector<std::uint8_t> encoded = sessiongw::encodeFrame(response);
    const std::size_t split = 10;
    writeAll(sockets[1], serverFrame(0x02U, std::span(encoded).first(split)));
    writeAll(sockets[1], serverFrame(0x89U, std::vector<std::uint8_t>{7, 8}));
    writeAll(sockets[1], serverFrame(0x80U, std::span(encoded).subspan(split)));

    const sessiongw::Frame received = connection.receiveFrame(9);
    EXPECT_EQ(received.payload, response.payload);
    const std::vector<std::uint8_t> pong = readExact(sockets[1], 8);
    EXPECT_EQ(pong[0], 0x8aU);
    EXPECT_EQ(pong[1] & 0x80U, 0x80U);
    EXPECT_EQ(pong[1] & 0x7fU, 2U);
    ::close(sockets[1]);
}

TEST(SessionGatewayWebSocket, rejectsMaskedServerFramesAndInvalidRsvBits)
{
    for (const std::array<std::uint8_t, 2> header :
         {std::array<std::uint8_t, 2>{0x82U, 0x80U},
          std::array<std::uint8_t, 2>{0xc2U, 0x00U}})
    {
        const auto sockets = socketPair();
        sessiongw::WebSocketConnection connection =
            sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
        writeAll(sockets[1], header);
        EXPECT_THROW((void)connection.receiveFrame(), sessiongw::Error);
        ::close(sockets[1]);
    }
}

TEST(SessionGatewayWebSocket, rejectsUnexpectedContinuationAndFragmentedControl)
{
    for (const std::vector<std::uint8_t>& frame :
         {serverFrame(0x80U, std::vector<std::uint8_t>{}),
          serverFrame(0x09U, std::vector<std::uint8_t>{1})})
    {
        const auto sockets = socketPair();
        sessiongw::WebSocketConnection connection =
            sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
        writeAll(sockets[1], frame);
        EXPECT_THROW((void)connection.receiveFrame(), sessiongw::Error);
        ::close(sockets[1]);
    }
}

TEST(SessionGatewayWebSocket, peerCloseClosesTransportDeterministically)
{
    const auto sockets = socketPair();
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
    writeAll(sockets[1], serverFrame(0x88U, std::vector<std::uint8_t>{}));
    try
    {
        (void)connection.receiveFrame();
        FAIL() << "expected peer close";
    }
    catch (const sessiongw::Error& error)
    {
        EXPECT_EQ(error.category(), sessiongw::ErrorCategory::transport_error);
    }
    EXPECT_THROW(connection.sendRawBinaryMessageForTest(std::span<const std::uint8_t>{}),
                 sessiongw::Error);
    ::close(sockets[1]);
}

TEST(SessionGatewayWebSocket, rejectsTextCarrierForSessionGatewayFrame)
{
    const auto sockets = socketPair();
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
    sessiongw::Frame response;
    const std::vector<std::uint8_t> encoded = sessiongw::encodeFrame(response);
    writeAll(sockets[1], serverFrame(0x81U, encoded));
    EXPECT_THROW((void)connection.receiveFrame(), sessiongw::Error);
    ::close(sockets[1]);
}

TEST(SessionGatewayWebSocket, disconnectedPlainWriteReturnsTransportErrorWithoutSigpipe)
{
    const auto sockets = socketPair();
    sessiongw::WebSocketConnection connection =
        sessiongw::WebSocketConnection::fromConnectedSocketForTest(sockets[0]);
    ::close(sockets[1]);
    try
    {
        connection.sendRawBinaryMessageForTest(std::vector<std::uint8_t>{1, 2, 3});
        FAIL() << "expected disconnected write failure";
    }
    catch (const sessiongw::Error& error)
    {
        EXPECT_EQ(error.category(), sessiongw::ErrorCategory::transport_error);
    }
}

TEST(SessionGatewayCapabilities, defaultClientCapabilitiesAdvertiseAllSupportedOperations)
{
    const sessiongw::Capabilities capabilities = sessiongw::defaultClientCapabilities();

    EXPECT_EQ(capabilities.values().size(), 11U);
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::session_gw_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::arrow_ipc_batch_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::pushed_query_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::metadata_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::table_scan_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::table_write_v1));
    EXPECT_FALSE(capabilities.supports(sessiongw::Capability::native_table_write_batch_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::native_table_write_batch_v2));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::native_table_read_batch_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::table_update_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::table_delete_v1));
    EXPECT_TRUE(capabilities.supports(sessiongw::Capability::transaction_control_v1));
    EXPECT_FALSE(capabilities.supports(sessiongw::Capability::outcome_recovery_v1));
}

TEST(SessionGatewayErrors, exposesStableCategoryNames)
{
    EXPECT_STREQ(sessiongw::toString(sessiongw::ErrorCategory::object_not_found),
                 "OBJECT_NOT_FOUND");
    EXPECT_STREQ(sessiongw::toString(sessiongw::ErrorCategory::outcome_unknown),
                 "OUTCOME_UNKNOWN");
    const sessiongw::Error error(sessiongw::ErrorCategory::protocol_error, "bad frame");
    EXPECT_EQ(error.category(), sessiongw::ErrorCategory::protocol_error);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
