cmake_minimum_required(VERSION 3.20)
project(ExasolGatewaySdk VERSION 0.1.0 LANGUAGES C CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

find_package(OpenSSL REQUIRED)
find_package(Arrow CONFIG REQUIRED)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)
add_library(SessionGatewaySdk
    src/c_api.cpp
    src/capabilities.cpp
    src/client.cpp
    src/errors.cpp
    src/frame.cpp
    src/native_write_batch.cpp
    src/session.cpp
    src/websocket.cpp)

# ExasolGateway is the product-facing name. SessionGateway names remain stable
# protocol, source, binary, and package compatibility surfaces.
add_library(ExasolGateway::Sdk ALIAS SessionGatewaySdk)
add_library(SessionGateway::Sdk ALIAS SessionGatewaySdk)
add_library(ExasolGateway::C ALIAS SessionGatewaySdk)

set_target_properties(SessionGatewaySdk PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 0
    EXPORT_NAME Sdk)
target_compile_features(SessionGatewaySdk PUBLIC cxx_std_23)
target_include_directories(SessionGatewaySdk
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(SessionGatewaySdk
    PUBLIC Arrow::arrow_shared
    PRIVATE OpenSSL::SSL OpenSSL::Crypto)

install(TARGETS SessionGatewaySdk
    EXPORT SessionGatewaySdkTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/sessiongw DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES LICENSE NOTICE DESTINATION ${CMAKE_INSTALL_DOCDIR})
install(DIRECTORY docs/ DESTINATION ${CMAKE_INSTALL_DOCDIR})

install(EXPORT SessionGatewaySdkTargets
    FILE SessionGatewaySdkTargets.cmake
    NAMESPACE SessionGateway::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SessionGatewaySdk)

configure_package_config_file(
    cmake/SessionGatewaySdkConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/SessionGatewaySdkConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SessionGatewaySdk)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/SessionGatewaySdkConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/SessionGatewaySdkConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/SessionGatewaySdkConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/SessionGatewaySdk)

# Additive product-named package. It exports the same library and compatibility
# target, so existing SessionGatewaySdk consumers do not need to migrate.
install(EXPORT SessionGatewaySdkTargets
    FILE SessionGatewaySdkTargets.cmake
    NAMESPACE SessionGateway::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ExasolGatewaySdk)
configure_package_config_file(
    cmake/SessionGatewaySdkConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/ExasolGatewaySdkConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ExasolGatewaySdk)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/ExasolGatewaySdkConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/ExasolGatewaySdkConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/ExasolGatewaySdkConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ExasolGatewaySdk)

option(EXASOL_GATEWAY_BUILD_EXAMPLES "Build public SDK examples" ON)
if(EXASOL_GATEWAY_BUILD_EXAMPLES)
    add_executable(sessiongw_public_example examples/public_consumer.cpp)
    target_link_libraries(sessiongw_public_example PRIVATE ExasolGateway::Sdk)
endif()

option(EXASOL_GATEWAY_BUILD_TOOLS "Build protocol diagnostic tools" OFF)
if(EXASOL_GATEWAY_BUILD_TOOLS)
    add_executable(sessiongw_sim_client tools/sessiongw_sim_client.cpp)
    target_link_libraries(sessiongw_sim_client PRIVATE ExasolGateway::Sdk)
endif()

if(BUILD_TESTING)
    find_package(GTest CONFIG REQUIRED)
    foreach(test_name IN ITEMS c_api frame native_write_batch public_api)
        add_executable(sessiongw_${test_name}_utest test/${test_name}_utest.cpp)
        target_link_libraries(sessiongw_${test_name}_utest
            PRIVATE ExasolGateway::Sdk GTest::gtest)
        add_test(NAME sessiongw_${test_name} COMMAND sessiongw_${test_name}_utest)
    endforeach()

    option(EXASOL_GATEWAY_BUILD_INTEGRATION_TEST
        "Build the integration workload that requires a live Exasol server" OFF)
    if(EXASOL_GATEWAY_BUILD_INTEGRATION_TEST)
        add_executable(sessiongw_sdk_integration_test
            test/integration/sessiongw_sdk_integration_test.cpp)
        target_link_libraries(sessiongw_sdk_integration_test
            PRIVATE ExasolGateway::Sdk GTest::gtest)
    endif()
endif()
