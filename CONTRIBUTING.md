# Contributing

1. Build and run all tests from a clean CMake build directory.
2. Preserve SGW1 framing, `sessiongw` headers/namespaces, and the `sessiongw_c_*`
   ABI unless a separately reviewed compatibility plan exists.
3. Add malformed-input and lifecycle tests for protocol changes.
4. Keep credentials, proprietary Exasol artifacts, and server-private headers
   out of this repository.
5. Use capability negotiation for additive protocol behavior.

By contributing, you agree that your contribution is licensed under the MIT
License in this repository.
