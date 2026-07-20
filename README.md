# Exasol Gateway SDK

Open-source C++ and stable C client SDK for the Exasol Gateway SessionGateway v1
protocol.

> **Status:** `0.x` preview. The `sessiongw_c_*` C ABI is the compatibility
> boundary used by native database adapters. Capability negotiation must be used
> instead of assuming server features.

## Requirements

- CMake 3.20+
- C++23 compiler (GCC 13+ or Clang 17+)
- OpenSSL 3
- Apache Arrow 19 C++ development package
- GoogleTest when `BUILD_TESTING=ON`

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
cmake --install build --prefix "$PWD/install"
```

## Consume

```cmake
find_package(ExasolGatewaySdk CONFIG REQUIRED)
target_link_libraries(my_adapter PRIVATE ExasolGateway::Sdk)
```

C consumers include `<sessiongw/c_api.h>` and may link the additive
`ExasolGateway::C` target. Existing `find_package(SessionGatewaySdk)` and
`SessionGateway::Sdk` consumers remain supported.

Credentials must be configured explicitly. TLS certificate verification is the
default. The `plain` and `skip_verify` C options and corresponding C++ enum
values are for isolated tests only and must not be used in production.

See:

- [Naming and compatibility](docs/branding.md)
- [SDK architecture](docs/sdk.md)
- [SessionGateway protocol v1](docs/protocol_v1.md)
- [Security policy](SECURITY.md)

## License

MIT; see [LICENSE](LICENSE). Third-party dependencies retain their own licenses;
see [NOTICE](NOTICE).
