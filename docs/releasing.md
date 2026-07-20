# Releasing

1. Ensure `main` CI is green and update `CHANGELOG.md` plus the CMake project
   version.
2. Create a reviewed, signed, v-prefixed semantic-version tag, initially in the
   `0.x` preview line.
3. Push the tag. The release workflow validates that the tag already exists,
   creates a reproducible source archive, SHA-256 checksum, minimal SPDX SBOM,
   and GitHub release.
4. Run the protected internal Exasol compatibility workflow against the tag
   before promoting it for production adapter use.

The C ABI symbol allowlist is checked on every public build. Removing or changing
an existing `sessiongw_c_*` symbol requires a separately reviewed major-version
compatibility decision. The C++ API may evolve during `0.x`, but capability and
wire compatibility remain mandatory.
