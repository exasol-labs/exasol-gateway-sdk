cmake_minimum_required(VERSION 3.20)
project(ExasolGatewayInstalledConsumers LANGUAGES C CXX)

option(USE_SESSIONGATEWAY_COMPAT_PACKAGE "Use the compatibility package name" OFF)
if(USE_SESSIONGATEWAY_COMPAT_PACKAGE)
    find_package(SessionGatewaySdk CONFIG REQUIRED)
else()
    find_package(ExasolGatewaySdk CONFIG REQUIRED)
endif()
if(NOT TARGET SessionGateway::Sdk OR NOT TARGET ExasolGateway::Sdk OR
   NOT TARGET ExasolGateway::C)
    message(FATAL_ERROR "Exasol Gateway product or compatibility targets are missing")
endif()

add_executable(exasol_gateway_c_consumer consumer.c)
target_compile_features(exasol_gateway_c_consumer PRIVATE c_std_11)
target_link_libraries(exasol_gateway_c_consumer PRIVATE ExasolGateway::C)

add_executable(exasol_gateway_cpp_consumer consumer.cpp)
target_compile_features(exasol_gateway_cpp_consumer PRIVATE cxx_std_23)
target_link_libraries(exasol_gateway_cpp_consumer PRIVATE ExasolGateway::Sdk)
