# Exasol Gateway Naming and Compatibility

## Product Name

The user-facing product and display name is **Exasol Gateway**. The preferred
code-style identifier for new additive APIs and build targets is
`ExasolGateway`.

MariaDB continues to expose the storage engine as:

```sql
ENGINE=EXASOL
```

## Compatibility Names

`SessionGateway` and `sessiongw` remain the technical compatibility names for
version 1. They are intentionally retained wherever renaming would break wire,
source, binary, package, configuration, or operational compatibility.

| Surface | Stable compatibility name |
|---|---|
| Protocol magic and framing | `SGW1` |
| Protocol capability and message specifications | SessionGateway v1 terminology |
| C++ namespace and installed headers | `sessiongw`, `<sessiongw/...>` |
| Stable C ABI | `sessiongw_c_*` |
| Existing CMake package | `SessionGatewaySdk` |
| Existing CMake target | `SessionGateway::Sdk` |
| Library and module files | `libSessionGatewaySdk`, existing SessionGateway modules |
| Environment variables | `EXASOL_SESSIONGW_*` |
| MariaDB plugin/module | `exasol_gw`, `ha_exasol_gw.so` |
| MariaDB SQL engine name | `EXASOL` |
| Existing instrumentation labels and protocol diagnostics | SessionGateway/SessionGW where operational compatibility matters |

These names do not imply a second product. They identify the stable technical
protocol and SDK surface used by Exasol Gateway.

## Additive Product-Facing Names

New CMake consumers should prefer:

```cmake
find_package(SessionGatewaySdk CONFIG REQUIRED)
target_link_libraries(my_adapter PRIVATE ExasolGateway::Sdk)
```

`ExasolGateway::Sdk` is an additive alias of the same SDK library. The original
`SessionGateway::Sdk` target remains fully supported and links to the identical
ABI.

No file, shared-library, protocol, plugin, header, namespace, environment
variable, or engine rename is part of this branding change. Any future removal
or migration of a compatibility name requires a separately reviewed migration
and deprecation decision.
