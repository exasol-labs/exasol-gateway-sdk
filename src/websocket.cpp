#include <sessiongw/websocket.hpp>

#include <sessiongw/errors.hpp>
#include <sessiongw/upgrade.hpp>

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <netdb.h>
#include <netinet/tcp.h>
#include <random>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace sessiongw
{
namespace
{

constexpr std::string_view websocket_guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
constexpr std::uint64_t max_websocket_message_size = 16U * 1024U * 1024U;

void checkOpenSsl(const int rc, const char* message)
{
    if (rc <= 0)
    {
        throw Error(ErrorCategory::transport_error, message);
    }
}

std::string base64Encode(const std::span<const std::uint8_t> data)
{
    BIO* bio = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());
    if (bio == nullptr || b64 == nullptr)
    {
        BIO_free_all(b64);
        BIO_free_all(bio);
        throw Error(ErrorCategory::transport_error, "OpenSSL BIO allocation failed");
    }
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    BIO_push(b64, bio);
    checkOpenSsl(BIO_write(b64, data.data(), static_cast<int>(data.size())), "OpenSSL base64 write failed");
    checkOpenSsl(BIO_flush(b64), "OpenSSL base64 flush failed");
    BUF_MEM* mem = nullptr;
    BIO_get_mem_ptr(b64, &mem);
    std::string result(mem->data, mem->length);
    BIO_free_all(b64);
    return result;
}

std::string websocketAcceptForKey(const std::string& key)
{
    const std::string input = key + std::string(websocket_guid);
    std::array<std::uint8_t, SHA_DIGEST_LENGTH> digest{};
    SHA1(reinterpret_cast<const unsigned char*>(input.data()), input.size(), digest.data());
    return base64Encode(digest);
}

std::string randomWebSocketKey()
{
    std::array<std::uint8_t, 16> bytes{};
    std::random_device rd;
    for (auto& byte : bytes)
    {
        byte = static_cast<std::uint8_t>(rd());
    }
    return base64Encode(bytes);
}

std::string jsonEscape(const std::string& value)
{
    std::string out;
    out.reserve(value.size() + 8);
    for (const char ch : value)
    {
        switch (ch)
        {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += ch;
                break;
        }
    }
    return out;
}

std::string jsonStringValue(const std::string& json, const std::string& key)
{
    const std::string quotedKey = "\"" + key + "\"";
    std::size_t pos = json.find(quotedKey);
    if (pos == std::string::npos)
    {
        throw Error(ErrorCategory::protocol_error, "WebSocket JSON response does not contain key: " + key);
    }
    pos = json.find(':', pos + quotedKey.size());
    if (pos == std::string::npos)
    {
        throw Error(ErrorCategory::protocol_error, "WebSocket JSON response has malformed key: " + key);
    }
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos)
    {
        throw Error(ErrorCategory::protocol_error, "WebSocket JSON response value is not a string: " + key);
    }
    ++pos;

    std::string value;
    bool escaped = false;
    for (; pos < json.size(); ++pos)
    {
        const char ch = json[pos];
        if (escaped)
        {
            switch (ch)
            {
                case 'n':
                    value += '\n';
                    break;
                case 'r':
                    value += '\r';
                    break;
                case 't':
                    value += '\t';
                    break;
                default:
                    value += ch;
                    break;
            }
            escaped = false;
            continue;
        }
        if (ch == '\\')
        {
            escaped = true;
            continue;
        }
        if (ch == '"')
        {
            return value;
        }
        value += ch;
    }
    throw Error(ErrorCategory::protocol_error, "WebSocket JSON response has unterminated string: " + key);
}

void requireStatusOk(const std::string& json)
{
    const std::string status = jsonStringValue(json, "status");
    if (status != "ok")
    {
        throw Error(ErrorCategory::protocol_error, "WebSocket command returned non-ok status: " + json);
    }
}

int connectTcp(const std::string& host, const std::uint16_t port)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* addresses = nullptr;
    const std::string portString = std::to_string(port);
    const int gai = getaddrinfo(host.c_str(), portString.c_str(), &hints, &addresses);
    if (gai != 0)
    {
        throw Error(ErrorCategory::transport_error, gai_strerror(gai));
    }

    int fd = -1;
    for (addrinfo* address = addresses; address != nullptr; address = address->ai_next)
    {
        fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (fd < 0)
        {
            continue;
        }
        if (::connect(fd, address->ai_addr, address->ai_addrlen) == 0)
        {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(addresses);

    if (fd < 0)
    {
        throw Error(ErrorCategory::transport_error, "Could not connect TCP socket");
    }
    return fd;
}

std::string openSslErrorString(const char* context)
{
    const unsigned long error = ERR_get_error();
    if (error == 0)
    {
        return context;
    }
    std::array<char, 256> buffer{};
    ERR_error_string_n(error, buffer.data(), buffer.size());
    return std::string(context) + ": " + buffer.data();
}

SSL_CTX* asSslContext(void* ptr)
{
    return static_cast<SSL_CTX*>(ptr);
}

SSL* asSsl(void* ptr)
{
    return static_cast<SSL*>(ptr);
}

int noSigpipeBioCreate(BIO* bio)
{
    BIO_set_init(bio, 1);
    BIO_set_data(bio, nullptr);
    return 1;
}

int noSigpipeBioDestroy(BIO* bio)
{
    if (bio != nullptr)
    {
        BIO_set_init(bio, 0);
        BIO_set_data(bio, nullptr);
    }
    return 1;
}

int noSigpipeBioWrite(BIO* bio, const char* data, const std::size_t size,
                      std::size_t* written)
{
    BIO_clear_retry_flags(bio);
    const int fd = static_cast<int>(reinterpret_cast<std::intptr_t>(BIO_get_data(bio)));
    const ssize_t rc = ::send(fd, data, size, MSG_NOSIGNAL);
    if (rc > 0)
    {
        *written = static_cast<std::size_t>(rc);
        return 1;
    }
    *written = 0;
    if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
    {
        BIO_set_retry_write(bio);
    }
    return 0;
}

int noSigpipeBioRead(BIO* bio, char* data, const std::size_t size,
                     std::size_t* read)
{
    BIO_clear_retry_flags(bio);
    const int fd = static_cast<int>(reinterpret_cast<std::intptr_t>(BIO_get_data(bio)));
    const ssize_t rc = ::recv(fd, data, size, 0);
    if (rc > 0)
    {
        *read = static_cast<std::size_t>(rc);
        return 1;
    }
    *read = 0;
    if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
    {
        BIO_set_retry_read(bio);
    }
    return 0;
}

long noSigpipeBioControl(BIO* bio, const int command, long, void* pointer)
{
    const int fd = static_cast<int>(reinterpret_cast<std::intptr_t>(BIO_get_data(bio)));
    switch (command)
    {
        case BIO_CTRL_FLUSH:
        case BIO_CTRL_DUP:
            return 1;
        case BIO_C_GET_FD:
            if (pointer != nullptr)
            {
                *static_cast<int*>(pointer) = fd;
            }
            return fd;
        case BIO_CTRL_EOF:
        case BIO_CTRL_PENDING:
        case BIO_CTRL_WPENDING:
            return 0;
        default:
            return 0;
    }
}

BIO_METHOD* noSigpipeSocketBioMethod()
{
    static BIO_METHOD* method = [] {
        BIO_METHOD* created = BIO_meth_new(BIO_TYPE_SOURCE_SINK, "SessionGW MSG_NOSIGNAL socket");
        if (created == nullptr ||
            BIO_meth_set_create(created, noSigpipeBioCreate) != 1 ||
            BIO_meth_set_destroy(created, noSigpipeBioDestroy) != 1 ||
            BIO_meth_set_write_ex(created, noSigpipeBioWrite) != 1 ||
            BIO_meth_set_read_ex(created, noSigpipeBioRead) != 1 ||
            BIO_meth_set_ctrl(created, noSigpipeBioControl) != 1)
        {
            BIO_meth_free(created);
            return static_cast<BIO_METHOD*>(nullptr);
        }
        return created;
    }();
    return method;
}

std::uint64_t readBigEndian(std::span<const std::uint8_t> bytes)
{
    std::uint64_t result = 0;
    for (const std::uint8_t byte : bytes)
    {
        result = (result << 8U) | static_cast<std::uint64_t>(byte);
    }
    return result;
}

void appendBigEndian(std::vector<std::uint8_t>& out, std::uint64_t value, const std::size_t size)
{
    for (std::size_t index = 0; index < size; ++index)
    {
        const unsigned shift = static_cast<unsigned>((size - index - 1U) * 8U);
        out.push_back(static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

void appendMask(std::vector<std::uint8_t>& out, std::array<std::uint8_t, 4>& mask)
{
    std::random_device rd;
    for (auto& byte : mask)
    {
        byte = static_cast<std::uint8_t>(rd());
        out.push_back(byte);
    }
}

void appendMaskedByte(std::vector<std::uint8_t>& out,
                      const std::array<std::uint8_t, 4>& mask,
                      const std::size_t index,
                      const std::uint8_t value)
{
    out.push_back(value ^ mask[index % mask.size()]);
}

void appendMaskedBigEndian(std::vector<std::uint8_t>& out,
                           const std::array<std::uint8_t, 4>& mask,
                           std::size_t& index,
                           const std::uint64_t value,
                           const std::size_t size)
{
    for (std::size_t byteIndex = 0; byteIndex < size; ++byteIndex)
    {
        const unsigned shift = static_cast<unsigned>((size - byteIndex - 1U) * 8U);
        appendMaskedByte(out, mask, index++, static_cast<std::uint8_t>((value >> shift) & 0xffU));
    }
}

void appendMaskedPayloadWithMask(std::vector<std::uint8_t>& out,
                                 const std::array<std::uint8_t, 4>& mask,
                                 std::size_t& index,
                                 const std::span<const std::uint8_t> payload)
{
    for (const std::uint8_t byte : payload)
    {
        appendMaskedByte(out, mask, index++, byte);
    }
}

void appendMaskedPayload(std::vector<std::uint8_t>& out, std::span<const std::uint8_t> payload)
{
    std::array<std::uint8_t, 4> mask{};
    appendMask(out, mask);
    std::size_t index = 0;
    appendMaskedPayloadWithMask(out, mask, index, payload);
}

void appendMaskedSessionGatewayFrame(std::vector<std::uint8_t>& out,
                                     const std::array<std::uint8_t, 4>& mask,
                                     std::size_t& index,
                                     const MessageType messageType,
                                     const std::uint64_t requestId,
                                     const std::span<const std::uint8_t> payload)
{
    if (payload.size() > std::numeric_limits<std::uint32_t>::max())
    {
        throw Error(ErrorCategory::resource_limit, "SessionGW frame payload is too large");
    }
    appendMaskedBigEndian(out, mask, index, frame_magic, 4);
    appendMaskedBigEndian(out, mask, index, protocol_version_v1, 2);
    appendMaskedBigEndian(out, mask, index, static_cast<std::uint16_t>(messageType), 2);
    appendMaskedBigEndian(out, mask, index, 0, 2); // flags
    appendMaskedBigEndian(out, mask, index, 0, 2); // reserved
    appendMaskedBigEndian(out, mask, index, requestId, 8);
    appendMaskedBigEndian(out, mask, index, static_cast<std::uint32_t>(payload.size()), 4);
    appendMaskedPayloadWithMask(out, mask, index, payload);
}

} // namespace

WebSocketConnection::WebSocketConnection() = default;

WebSocketConnection::WebSocketConnection(const int fd) : fd_(fd)
{
}

WebSocketConnection::~WebSocketConnection()
{
    close();
}

WebSocketConnection::WebSocketConnection(WebSocketConnection&& other) noexcept
    : fd_(other.fd_),
      webSocketFrameBuffer_(std::move(other.webSocketFrameBuffer_)),
      sslContext_(other.sslContext_),
      ssl_(other.ssl_),
      instrumentationEnabled_(other.instrumentationEnabled_),
      statistics_(other.statistics_)
{
    other.fd_ = -1;
    other.sslContext_ = nullptr;
    other.ssl_ = nullptr;
}

WebSocketConnection& WebSocketConnection::operator=(WebSocketConnection&& other) noexcept
{
    if (this != &other)
    {
        close();
        fd_ = other.fd_;
        webSocketFrameBuffer_ = std::move(other.webSocketFrameBuffer_);
        sslContext_ = other.sslContext_;
        ssl_ = other.ssl_;
        instrumentationEnabled_ = other.instrumentationEnabled_;
        statistics_ = other.statistics_;
        other.fd_ = -1;
        other.sslContext_ = nullptr;
        other.ssl_ = nullptr;
    }
    return *this;
}

WebSocketConnection WebSocketConnection::fromConnectedSocketForTest(const int fd)
{
    return WebSocketConnection(fd);
}

WebSocketConnection WebSocketConnection::connectAndLogin(const WebSocketOptions& options)
{
    if (options.user.empty() || options.password.empty())
    {
        throw Error(ErrorCategory::authentication_failed,
                    "Exasol Gateway user and password must be configured explicitly");
    }
    const auto connectStarted = options.instrumentation_enabled ? std::chrono::steady_clock::now()
                                                                  : std::chrono::steady_clock::time_point{};
    WebSocketConnection connection(connectTcp(options.host, options.port));
    const int noDelay = 1;
    if (::setsockopt(connection.fd_, IPPROTO_TCP, TCP_NODELAY, &noDelay, sizeof(noDelay)) != 0)
    {
        throw Error(ErrorCategory::transport_error,
                    std::string("Failed to enable TCP_NODELAY: ") + std::strerror(errno));
    }
    connection.instrumentationEnabled_ = options.instrumentation_enabled;
    if (options.tls_mode != WebSocketTlsMode::plain_for_test_only)
    {
        connection.enableTls(options);
    }

    const std::string key = randomWebSocketKey();
    std::ostringstream request;
    request << "GET / HTTP/1.1\r\n"
            << "Host: " << options.host << ':' << options.port << "\r\n"
            << "Upgrade: websocket\r\n"
            << "Connection: Upgrade\r\n"
            << "Sec-WebSocket-Key: " << key << "\r\n"
            << "Sec-WebSocket-Version: 13\r\n\r\n";
    const std::string requestText = request.str();
    connection.transportWrite(std::span<const std::uint8_t>(
        reinterpret_cast<const std::uint8_t*>(requestText.data()), requestText.size()));

    std::string response;
    std::array<char, 1> ch{};
    while (response.find("\r\n\r\n") == std::string::npos)
    {
        connection.transportRead(std::span<std::uint8_t>(
            reinterpret_cast<std::uint8_t*>(ch.data()), ch.size()));
        response.append(ch.data(), ch.size());
        if (response.size() > 16U * 1024U)
        {
            throw Error(ErrorCategory::protocol_error, "WebSocket HTTP upgrade response is too large");
        }
    }
    if (response.find("101") == std::string::npos || response.find(websocketAcceptForKey(key)) == std::string::npos)
    {
        throw Error(ErrorCategory::protocol_error, "Invalid WebSocket upgrade response: " + response);
    }

    connection.sendText(R"({"command":"login","protocolVersion":5})");
    const std::string keyResponse = connection.receiveText();
    requireStatusOk(keyResponse);
    const std::string publicKeyPem = jsonStringValue(keyResponse, "publicKeyPem");
    const std::string encryptedPassword = encryptPasswordForWebSocketLogin(publicKeyPem, options.password);

    std::ostringstream login;
    login << "{\"username\":\"" << jsonEscape(options.user)
          << "\",\"password\":\"" << encryptedPassword
          << "\",\"useCompression\":false,\"clientName\":\""
          << jsonEscape(options.client_name) << "\"}";
    connection.sendText(login.str());
    requireStatusOk(connection.receiveText());
    if (connection.instrumentationEnabled_)
    {
        connection.statistics_.connect_nanoseconds = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - connectStarted).count());
    }
    return connection;
}

std::string WebSocketConnection::sendJsonCommand(const std::string_view json)
{
    sendText(json);
    return receiveText();
}

void WebSocketConnection::enterSessionGateway()
{
    const std::string response = sendJsonCommand(R"({"command":"enterSessionGateway","protocolVersion":1})");
    requireStatusOk(response);
}

void WebSocketConnection::sendFrame(const Frame& frame)
{
    sendFrame(frame.header.message_type, frame.header.request_id, frame.payload);
}

void WebSocketConnection::sendFrame(const MessageType message_type,
                                    const std::uint64_t request_id,
                                    const std::span<const std::uint8_t> payload)
{
    const std::size_t sessionGatewayFrameSize = frame_header_size + payload.size();
    webSocketFrameBuffer_.clear();
    webSocketFrameBuffer_.push_back(0x82U);
    if (sessionGatewayFrameSize <= 125U)
    {
        webSocketFrameBuffer_.push_back(static_cast<std::uint8_t>(0x80U | sessionGatewayFrameSize));
    }
    else if (sessionGatewayFrameSize <= 0xffffU)
    {
        webSocketFrameBuffer_.push_back(0x80U | 126U);
        appendBigEndian(webSocketFrameBuffer_, sessionGatewayFrameSize, 2);
    }
    else
    {
        webSocketFrameBuffer_.push_back(0x80U | 127U);
        appendBigEndian(webSocketFrameBuffer_, sessionGatewayFrameSize, 8);
    }

    std::array<std::uint8_t, 4> mask{};
    appendMask(webSocketFrameBuffer_, mask);
    std::size_t maskIndex = 0;
    appendMaskedSessionGatewayFrame(webSocketFrameBuffer_, mask, maskIndex, message_type, request_id, payload);
    if (instrumentationEnabled_)
    {
        ++statistics_.sessiongw_frames_sent;
        statistics_.sessiongw_payload_bytes_sent += payload.size();
        statistics_.websocket_mask_copy_bytes += sessionGatewayFrameSize;
    }
    transportWrite(webSocketFrameBuffer_);
}

Frame WebSocketConnection::receiveFrame()
{
    try
    {
        std::vector<std::uint8_t> bytes = receiveBinary(0x02U);
        const auto decodeStarted = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                            : std::chrono::steady_clock::time_point{};
        Frame frame = decodeFrame(bytes);
        if (instrumentationEnabled_)
        {
            statistics_.sessiongw_frame_decode_nanoseconds += static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - decodeStarted).count());
            ++statistics_.sessiongw_frames_received;
            statistics_.sessiongw_payload_bytes_received += frame.payload.size();
        }
        return frame;
    }
    catch (...)
    {
        close();
        throw;
    }
}

Frame WebSocketConnection::receiveFrame(const std::uint64_t expected_request_id)
{
    Frame frame = receiveFrame();
    if (frame.header.request_id != expected_request_id)
    {
        std::ostringstream message;
        message << "SessionGW response request id " << frame.header.request_id
                << " does not match request " << expected_request_id;
        close();
        throw Error(ErrorCategory::protocol_error, message.str());
    }
    return frame;
}

void WebSocketConnection::sendRawBinaryMessageForTest(const std::span<const std::uint8_t> payload)
{
    sendBinary(payload);
}

void WebSocketConnection::sendRawTextMessageForTest(const std::string_view payload)
{
    sendText(payload);
}

const WebSocketStatistics& WebSocketConnection::statistics() const noexcept
{
    return statistics_;
}

void WebSocketConnection::close()
{
    if (ssl_ != nullptr)
    {
        SSL_shutdown(asSsl(ssl_));
        SSL_free(asSsl(ssl_));
        ssl_ = nullptr;
    }
    if (sslContext_ != nullptr)
    {
        SSL_CTX_free(asSslContext(sslContext_));
        sslContext_ = nullptr;
    }
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

void WebSocketConnection::enableTls(const WebSocketOptions& options)
{
    SSL_CTX* context = SSL_CTX_new(TLS_client_method());
    if (context == nullptr)
    {
        throw Error(ErrorCategory::transport_error, openSslErrorString("OpenSSL TLS context allocation failed"));
    }
    sslContext_ = context;

    if (options.tls_mode == WebSocketTlsMode::tls_verify)
    {
        SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
        if (options.ca_file.empty())
        {
            if (SSL_CTX_set_default_verify_paths(context) != 1)
            {
                throw Error(ErrorCategory::transport_error,
                            openSslErrorString("OpenSSL could not load default CA paths"));
            }
        }
        else if (SSL_CTX_load_verify_locations(context, options.ca_file.c_str(), nullptr) != 1)
        {
            throw Error(ErrorCategory::transport_error,
                        openSslErrorString("OpenSSL could not load CA file"));
        }
    }
    else
    {
        SSL_CTX_set_verify(context, SSL_VERIFY_NONE, nullptr);
    }

    SSL* ssl = SSL_new(context);
    if (ssl == nullptr)
    {
        throw Error(ErrorCategory::transport_error, openSslErrorString("OpenSSL TLS allocation failed"));
    }
    ssl_ = ssl;
    BIO_METHOD* bioMethod = noSigpipeSocketBioMethod();
    BIO* socketBio = bioMethod == nullptr ? nullptr : BIO_new(bioMethod);
    if (socketBio == nullptr)
    {
        throw Error(ErrorCategory::transport_error,
                    openSslErrorString("OpenSSL TLS socket BIO setup failed"));
    }
    BIO_set_data(socketBio, reinterpret_cast<void*>(static_cast<std::intptr_t>(fd_)));
    BIO_set_init(socketBio, 1);
    // SSL owns this shared read/write BIO after SSL_set_bio(). The BIO itself
    // does not own fd_; WebSocketConnection closes the socket exactly once.
    SSL_set_bio(ssl, socketBio, socketBio);
    SSL_set_tlsext_host_name(ssl, options.host.c_str());
    if (options.tls_mode == WebSocketTlsMode::tls_verify && SSL_set1_host(ssl, options.host.c_str()) != 1)
    {
        throw Error(ErrorCategory::transport_error, openSslErrorString("OpenSSL TLS host verification setup failed"));
    }
    if (SSL_connect(ssl) != 1)
    {
        throw Error(ErrorCategory::transport_error, openSslErrorString("OpenSSL TLS handshake failed"));
    }
    if (options.tls_mode == WebSocketTlsMode::tls_verify && SSL_get_verify_result(ssl) != X509_V_OK)
    {
        throw Error(ErrorCategory::transport_error, "OpenSSL TLS certificate verification failed");
    }
}

void WebSocketConnection::transportWrite(const std::span<const std::uint8_t> payload)
{
    const auto started = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                  : std::chrono::steady_clock::time_point{};
    if (fd_ < 0)
    {
        throw Error(ErrorCategory::transport_error, "WebSocket transport is closed");
    }
    std::size_t written = 0;
    while (written < payload.size())
    {
        int rc = 0;
        if (ssl_ != nullptr)
        {
            rc = SSL_write(asSsl(ssl_), payload.data() + written,
                           static_cast<int>(payload.size() - written));
        }
        else
        {
            rc = static_cast<int>(::send(fd_, payload.data() + written,
                                         payload.size() - written, MSG_NOSIGNAL));
        }
        if (rc <= 0)
        {
            throw Error(ErrorCategory::transport_error,
                        ssl_ != nullptr ? openSslErrorString("OpenSSL TLS write failed") : std::strerror(errno));
        }
        written += static_cast<std::size_t>(rc);
    }
    if (instrumentationEnabled_)
    {
        ++statistics_.transport_write_calls;
        statistics_.transport_bytes_written += payload.size();
        statistics_.transport_write_nanoseconds += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started).count());
    }
}

void WebSocketConnection::transportRead(const std::span<std::uint8_t> payload)
{
    const auto started = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                  : std::chrono::steady_clock::time_point{};
    std::size_t read = 0;
    while (read < payload.size())
    {
        int rc = 0;
        if (ssl_ != nullptr)
        {
            rc = SSL_read(asSsl(ssl_), payload.data() + read,
                          static_cast<int>(payload.size() - read));
        }
        else
        {
            rc = static_cast<int>(::recv(fd_, payload.data() + read, payload.size() - read, 0));
        }
        if (rc <= 0)
        {
            throw Error(ErrorCategory::transport_error,
                        ssl_ != nullptr ? openSslErrorString("OpenSSL TLS read failed") : "Socket closed while reading");
        }
        if (instrumentationEnabled_)
            ++statistics_.transport_read_iterations;
        read += static_cast<std::size_t>(rc);
    }
    if (instrumentationEnabled_)
    {
        ++statistics_.transport_read_calls;
        statistics_.transport_bytes_read += payload.size();
        statistics_.transport_read_nanoseconds += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started).count());
    }
}

void WebSocketConnection::sendText(const std::string_view text)
{
    const auto payload = std::span<const std::uint8_t>(
        reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
    webSocketFrameBuffer_.clear();
    webSocketFrameBuffer_.push_back(0x81U);
    if (payload.size() <= 125U)
    {
        webSocketFrameBuffer_.push_back(static_cast<std::uint8_t>(0x80U | payload.size()));
    }
    else if (payload.size() <= 0xffffU)
    {
        webSocketFrameBuffer_.push_back(0x80U | 126U);
        appendBigEndian(webSocketFrameBuffer_, payload.size(), 2);
    }
    else
    {
        webSocketFrameBuffer_.push_back(0x80U | 127U);
        appendBigEndian(webSocketFrameBuffer_, payload.size(), 8);
    }
    appendMaskedPayload(webSocketFrameBuffer_, payload);
    transportWrite(webSocketFrameBuffer_);
}

void WebSocketConnection::sendBinary(const std::span<const std::uint8_t> payload)
{
    webSocketFrameBuffer_.clear();
    webSocketFrameBuffer_.push_back(0x82U);
    if (payload.size() <= 125U)
    {
        webSocketFrameBuffer_.push_back(static_cast<std::uint8_t>(0x80U | payload.size()));
    }
    else if (payload.size() <= 0xffffU)
    {
        webSocketFrameBuffer_.push_back(0x80U | 126U);
        appendBigEndian(webSocketFrameBuffer_, payload.size(), 2);
    }
    else
    {
        webSocketFrameBuffer_.push_back(0x80U | 127U);
        appendBigEndian(webSocketFrameBuffer_, payload.size(), 8);
    }
    appendMaskedPayload(webSocketFrameBuffer_, payload);
    transportWrite(webSocketFrameBuffer_);
}

void WebSocketConnection::sendControl(const std::uint8_t opcode,
                                      const std::span<const std::uint8_t> payload)
{
    if (payload.size() > 125U)
    {
        throw Error(ErrorCategory::protocol_error, "WebSocket control payload is too large");
    }
    webSocketFrameBuffer_.clear();
    webSocketFrameBuffer_.push_back(static_cast<std::uint8_t>(0x80U | opcode));
    webSocketFrameBuffer_.push_back(static_cast<std::uint8_t>(0x80U | payload.size()));
    appendMaskedPayload(webSocketFrameBuffer_, payload);
    transportWrite(webSocketFrameBuffer_);
}

std::string WebSocketConnection::receiveText()
{
    try
    {
        std::vector<std::uint8_t> bytes = receiveBinary(0x01U);
        return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    catch (...)
    {
        close();
        throw;
    }
}

std::vector<std::uint8_t> WebSocketConnection::receiveBinary(const std::uint8_t expected_opcode)
{
    std::vector<std::uint8_t> message;
    bool fragmented = false;

    for (;;)
    {
        std::array<std::uint8_t, 2> header{};
        const auto headerStarted = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                            : std::chrono::steady_clock::time_point{};
        transportRead(header);
        if (instrumentationEnabled_)
        {
            ++statistics_.websocket_header_read_calls;
            statistics_.websocket_header_read_bytes += header.size();
            statistics_.websocket_header_read_nanoseconds += static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - headerStarted).count());
        }
        const bool fin = (header[0] & 0x80U) != 0;
        const std::uint8_t rsv = header[0] & 0x70U;
        const std::uint8_t opcode = header[0] & 0x0fU;
        const bool masked = (header[1] & 0x80U) != 0;
        if (rsv != 0)
        {
            throw Error(ErrorCategory::protocol_error, "WebSocket RSV bits are not negotiated");
        }
        if (masked)
        {
            throw Error(ErrorCategory::protocol_error, "WebSocket server frame must not be masked");
        }

        std::uint64_t payloadLength = header[1] & 0x7fU;
        if (payloadLength == 126U)
        {
            std::array<std::uint8_t, 2> extended{};
            const auto extendedStarted = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                                  : std::chrono::steady_clock::time_point{};
            transportRead(extended);
            if (instrumentationEnabled_)
            {
                ++statistics_.websocket_header_read_calls;
                statistics_.websocket_header_read_bytes += extended.size();
                statistics_.websocket_header_read_nanoseconds += static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - extendedStarted).count());
            }
            payloadLength = readBigEndian(extended);
            if (payloadLength < 126U)
            {
                throw Error(ErrorCategory::protocol_error, "WebSocket payload uses non-minimal length encoding");
            }
        }
        else if (payloadLength == 127U)
        {
            std::array<std::uint8_t, 8> extended{};
            const auto extendedStarted = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                                  : std::chrono::steady_clock::time_point{};
            transportRead(extended);
            if (instrumentationEnabled_)
            {
                ++statistics_.websocket_header_read_calls;
                statistics_.websocket_header_read_bytes += extended.size();
                statistics_.websocket_header_read_nanoseconds += static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - extendedStarted).count());
            }
            if ((extended[0] & 0x80U) != 0)
            {
                throw Error(ErrorCategory::protocol_error, "WebSocket payload length has its high bit set");
            }
            payloadLength = readBigEndian(extended);
            if (payloadLength <= 0xffffU)
            {
                throw Error(ErrorCategory::protocol_error, "WebSocket payload uses non-minimal length encoding");
            }
        }

        const bool control = opcode >= 0x8U;
        if (control && (!fin || payloadLength > 125U))
        {
            throw Error(ErrorCategory::protocol_error, "Invalid fragmented or oversized WebSocket control frame");
        }
        if (payloadLength > max_websocket_message_size ||
            message.size() > max_websocket_message_size - payloadLength)
        {
            throw Error(ErrorCategory::resource_limit, "WebSocket message is too large");
        }

        std::vector<std::uint8_t> payload(static_cast<std::size_t>(payloadLength));
        if (!payload.empty())
        {
            const auto payloadStarted = instrumentationEnabled_ ? std::chrono::steady_clock::now()
                                                                 : std::chrono::steady_clock::time_point{};
            transportRead(payload);
            if (instrumentationEnabled_)
            {
                ++statistics_.websocket_payload_read_calls;
                statistics_.websocket_payload_read_bytes += payload.size();
                statistics_.websocket_payload_read_nanoseconds += static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - payloadStarted).count());
            }
        }

        if (opcode == 0x9U)
        {
            sendControl(0x0aU, payload);
            continue;
        }
        if (opcode == 0x0aU)
        {
            continue;
        }
        if (opcode == 0x8U)
        {
            try
            {
                sendControl(0x08U, payload);
            }
            catch (...)
            {
            }
            close();
            throw Error(ErrorCategory::transport_error, "WebSocket close received");
        }
        if (control || (opcode >= 0x3U && opcode <= 0x7U))
        {
            throw Error(ErrorCategory::protocol_error, "Unsupported WebSocket opcode");
        }

        if (opcode == 0x0U)
        {
            if (!fragmented)
            {
                throw Error(ErrorCategory::protocol_error, "Unexpected WebSocket continuation frame");
            }
            message.insert(message.end(), payload.begin(), payload.end());
            if (fin)
            {
                return message;
            }
            continue;
        }

        if (fragmented)
        {
            throw Error(ErrorCategory::protocol_error,
                        "New WebSocket data frame before fragmented message completed");
        }
        if (expected_opcode != 0 && opcode != expected_opcode)
        {
            throw Error(ErrorCategory::protocol_error, "Unexpected WebSocket data frame type");
        }
        if (fin)
        {
            return payload;
        }
        fragmented = true;
        message = std::move(payload);
    }
}

std::string encryptPasswordForWebSocketLogin(const std::string& publicKeyPem, const std::string& password)
{
    BIO* bio = BIO_new_mem_buf(publicKeyPem.data(), static_cast<int>(publicKeyPem.size()));
    if (bio == nullptr)
    {
        throw Error(ErrorCategory::transport_error, "OpenSSL BIO allocation failed");
    }
    EVP_PKEY* key = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (key == nullptr)
    {
        throw Error(ErrorCategory::transport_error, "Could not parse WebSocket public key PEM");
    }

    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new(key, nullptr);
    if (ctx == nullptr)
    {
        EVP_PKEY_free(key);
        throw Error(ErrorCategory::transport_error, "OpenSSL EVP_PKEY_CTX allocation failed");
    }

    checkOpenSsl(EVP_PKEY_encrypt_init(ctx), "OpenSSL RSA encrypt init failed");
    checkOpenSsl(EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_PADDING), "OpenSSL RSA padding setup failed");

    std::size_t encryptedSize = 0;
    checkOpenSsl(EVP_PKEY_encrypt(ctx,
                                  nullptr,
                                  &encryptedSize,
                                  reinterpret_cast<const unsigned char*>(password.data()),
                                  password.size()),
                 "OpenSSL RSA encrypted-size calculation failed");

    std::vector<std::uint8_t> encrypted(encryptedSize);
    checkOpenSsl(EVP_PKEY_encrypt(ctx,
                                  encrypted.data(),
                                  &encryptedSize,
                                  reinterpret_cast<const unsigned char*>(password.data()),
                                  password.size()),
                 "OpenSSL RSA encrypt failed");
    encrypted.resize(encryptedSize);

    EVP_PKEY_CTX_free(ctx);
    EVP_PKEY_free(key);
    return base64Encode(encrypted);
}

} // namespace sessiongw
