#ifndef SESSIONGW_WEBSOCKET_HPP
#define SESSIONGW_WEBSOCKET_HPP

#include <sessiongw/frame.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace sessiongw
{

enum class WebSocketTlsMode : std::uint16_t
{
    plain_for_test_only = 1,
    tls_verify = 2,
    tls_skip_verify_for_test_only = 3
};

struct WebSocketOptions
{
    std::string host = "127.0.0.1";
    std::uint16_t port = 8563;
    std::string user;
    std::string password;
    std::string client_name = "sessiongw_sdk";
    WebSocketTlsMode tls_mode = WebSocketTlsMode::tls_verify;
    std::string ca_file;
    bool instrumentation_enabled = false;
};

struct WebSocketStatistics
{
    std::uint64_t connect_nanoseconds = 0;
    std::uint64_t transport_write_calls = 0;
    std::uint64_t transport_read_calls = 0;
    std::uint64_t transport_read_iterations = 0;
    std::uint64_t transport_bytes_written = 0;
    std::uint64_t transport_bytes_read = 0;
    std::uint64_t transport_write_nanoseconds = 0;
    std::uint64_t transport_read_nanoseconds = 0;
    std::uint64_t sessiongw_frames_sent = 0;
    std::uint64_t sessiongw_frames_received = 0;
    std::uint64_t sessiongw_payload_bytes_sent = 0;
    std::uint64_t sessiongw_payload_bytes_received = 0;
    std::uint64_t websocket_mask_copy_bytes = 0;
    std::uint64_t websocket_header_read_calls = 0;
    std::uint64_t websocket_header_read_bytes = 0;
    std::uint64_t websocket_header_read_nanoseconds = 0;
    std::uint64_t websocket_payload_read_calls = 0;
    std::uint64_t websocket_payload_read_bytes = 0;
    std::uint64_t websocket_payload_read_nanoseconds = 0;
    std::uint64_t sessiongw_frame_decode_nanoseconds = 0;
};

class WebSocketConnection final
{
public:
    WebSocketConnection();
    ~WebSocketConnection();

    WebSocketConnection(const WebSocketConnection&) = delete;
    WebSocketConnection& operator=(const WebSocketConnection&) = delete;
    WebSocketConnection(WebSocketConnection&& other) noexcept;
    WebSocketConnection& operator=(WebSocketConnection&& other) noexcept;

    static WebSocketConnection connectAndLogin(const WebSocketOptions& options);
    // Takes ownership of an already connected plain socket. Unit tests only.
    static WebSocketConnection fromConnectedSocketForTest(int fd);

    [[nodiscard]] std::string sendJsonCommand(std::string_view json);
    void enterSessionGateway();
    void sendFrame(const Frame& frame);
    void sendFrame(MessageType message_type,
                   std::uint64_t request_id,
                   std::span<const std::uint8_t> payload = {});
    [[nodiscard]] Frame receiveFrame();
    [[nodiscard]] Frame receiveFrame(std::uint64_t expected_request_id);

    // Integration-test hooks for malformed carrier messages; production clients use sendFrame().
    void sendRawBinaryMessageForTest(std::span<const std::uint8_t> payload);
    void sendRawTextMessageForTest(std::string_view payload);

    [[nodiscard]] const WebSocketStatistics& statistics() const noexcept;
    void close();

private:
    explicit WebSocketConnection(int fd);

    void enableTls(const WebSocketOptions& options);
    void transportWrite(std::span<const std::uint8_t> payload);
    void transportRead(std::span<std::uint8_t> payload);

    void sendText(std::string_view text);
    [[nodiscard]] std::string receiveText();
    void sendBinary(std::span<const std::uint8_t> payload);
    void sendControl(std::uint8_t opcode, std::span<const std::uint8_t> payload);
    [[nodiscard]] std::vector<std::uint8_t> receiveBinary(std::uint8_t expected_opcode = 0);

    int fd_ = -1;
    std::vector<std::uint8_t> webSocketFrameBuffer_;
    void* sslContext_ = nullptr;
    void* ssl_ = nullptr;
    bool instrumentationEnabled_ = false;
    WebSocketStatistics statistics_;
};

[[nodiscard]] std::string encryptPasswordForWebSocketLogin(const std::string& publicKeyPem,
                                                           const std::string& password);

} // namespace sessiongw

#endif // SESSIONGW_WEBSOCKET_HPP
