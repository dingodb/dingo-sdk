# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# if compile_commands.json is needed, please enable CMAKE_EXPORT_COMPILE_COMMANDS, of use `bear --append -- make` to do make, it's more recommended to use bear.

cmake_minimum_required(VERSION 3.24 FATAL_ERROR)

# Keep the version below in sync with the one in client.h
project(dingosdk VERSION 1.10.0 LANGUAGES C CXX)

option(SDK_ENABLE_GRPC "Build sdk with grpc instead brpc" OFF)
option(BUILD_BENCHMARK "Build benchmark" ON)
option(BUILD_INTEGRATION_TESTS "Build integration test" ON)
option(BUILD_UNIT_TESTS "Build unit test" ON)
option(BUILD_SDK_EXAMPLE "Build sdk example" ON)
option(BUILD_PYTHON_SDK "Build python sdk" OFF)
option(DINGOSDK_INSTALL "Intall dingosdk header and libary" ON)

set(CMAKE_CXX_STANDARD 17)
message("CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message("CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

message("SYSTEM: ${CMAKE_HOST_SYSTEM_NAME} ${CMAKE_HOST_SYSTEM_PROCESSOR}")

set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

# third-party install path
if(NOT THIRD_PARTY_INSTALL_PATH)
  set(THIRD_PARTY_INSTALL_PATH "$ENV{HOME}/.local/dingo-eureka")
endif()
message("THIRD_PARTY_INSTALL_PATH:${THIRD_PARTY_INSTALL_PATH}")

set(CMAKE_PREFIX_PATH
  ${THIRD_PARTY_INSTALL_PATH}
  ${CMAKE_PREFIX_PATH}
  )
message("CMAKE_PREFIX_PATH:${CMAKE_PREFIX_PATH}")

# link first lookup from THIRD_PARTY_INSTALL_PATH
link_directories(BEFORE ${THIRD_PARTY_INSTALL_PATH}/lib)

# openssl
if(NOT OPENSSL_ROOT_DIR) 
  set(OPENSSL_ROOT_DIR ${THIRD_PARTY_INSTALL_PATH})
endif()
find_package(OpenSSL REQUIRED)
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}, include dir: ${OPENSSL_INCLUDE_DIR}")

# find third-party
set(gflags_DIR ${THIRD_PARTY_INSTALL_PATH}/lib/cmake/gflags)
find_package(gflags REQUIRED)
message("Using gflags ${gflags_VERSION}, include_dir:${gflags_INCLUDE_DIR}")

find_package(fmt REQUIRED)
message("Using fmt ${fmt_VERSION}")

set(ZLIB_USE_STATIC_LIBS "ON")
find_package(ZLIB REQUIRED)
message("Using zlib ${ZLIB_VERSION}, include_dir:${ZLIB_INCLUDE_DIR}")

find_package (glog REQUIRED)
# used for glog 0.7.0
add_compile_definitions(GLOG_USE_GLOG_EXPORT)
message("Using glog ${glog_VERSION}")

find_package(GTest CONFIG REQUIRED)
message("Using GTest ${GTest_VERSION}")

find_package(RapidJSON REQUIRED)
message("Using RapidJSON ${RapidJSON_VERSION}, include_dir:${RapidJSON_INCLUDE_DIR}")

# # https://github.com/llvm/llvm-project/blob/main/cmake/Modules/FindGRPC.cmake
# This setup requires gRPC to be built from sources using CMake and installed
# to ${GRPC_INSTALL_PATH} via -DCMAKE_INSTALL_PREFIX=${GRPC_INSTALL_PATH}.
# Libraries will be linked according to gRPC build policy which generates
# static libraries when BUILD_SHARED_LIBS is Off and dynamic libraries when
# it's On (NOTE: This is a variable passed to gRPC CMake build invocation,
# LLVM's BUILD_SHARED_LIBS has no effect).
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message("Using protobuf ${Protobuf_VERSION}, include_dir:${Protobuf_INCLUDE_DIRS}" )
# include_directories(${Protobuf_INCLUDE_DIRS})

find_package(gRPC CONFIG REQUIRED)
message("Using gRPC ${gRPC_VERSION}")

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
message("Using grpc_cpp_plugin ${GRPC_CPP_PLUGIN}")

find_program(PROTOC protoc REQUIRED)
message("Using protoc ${PROTOC}")

# gRPC CMake CONFIG gives the libraries slightly odd names, make them match
# the conventional system-installed names.
set_target_properties(protobuf::libprotobuf PROPERTIES IMPORTED_GLOBAL TRUE)
add_library(grpc_protobuf ALIAS protobuf::libprotobuf)
  
set_target_properties(gRPC::grpc++ PROPERTIES IMPORTED_GLOBAL TRUE)
add_library(grpc++ ALIAS gRPC::grpc++)
if (ENABLE_GRPC_REFLECTION)
    set_target_properties(gRPC::grpc++_reflection PROPERTIES IMPORTED_GLOBAL TRUE)
    add_library(grpc++_reflection ALIAS gRPC::grpc++_reflection)
endif()

if(SDK_ENABLE_GRPC)
    add_definitions(-DUSE_GRPC=1)
endif()

set(PROTO_OUTPUT_DIR ${CMAKE_BINARY_DIR}/proto)
add_custom_target(create_proto_output_dir ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory ${PROTO_OUTPUT_DIR}
)
message("PROTO_OUTPUT_DIR:${PROTO_OUTPUT_DIR}")

set(PROTO_OBJS_SRC "")

set(PROTOS_DIR ${PROJECT_SOURCE_DIR}/store-proto/proto)
message("PROTOS_DIR:${PROTOS_DIR}")
file(GLOB_RECURSE MSG_PROTOS ${PROTOS_DIR}/*.proto)

if(SDK_ENABLE_GRPC)
    # grpc proto file
    set(GRPC_PROTO_DIR ${CMAKE_BINARY_DIR}/proto_tmp)
    add_custom_target(create_grpc_proto_dir ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory ${GRPC_PROTO_DIR}
    )
    message("GRPC_PROTO_DIR:${GRPC_PROTO_DIR}")

    set(GRPC_PROTOS "")

    set(GRPC_PROTO_SRCS "")
    set(GRPC_PROTO_HDRS "")
    set(GRPC_PROTO_GRPC_SRCS "")
    set(GRPC_PROTO_GRPC_HDRS "")
    # First loop to copy proto files

    foreach(msg ${MSG_PROTOS})
        get_filename_component(FIL_WE ${msg} NAME_WE)
        set(TMP_PROTO ${GRPC_PROTO_DIR}/${FIL_WE}.proto)
        message("proto:${msg}, grpc_proto: ${TMP_PROTO}")

        add_custom_command(
            OUTPUT ${TMP_PROTO}
            COMMAND ${CMAKE_COMMAND} -E copy ${msg} ${TMP_PROTO}
            COMMAND sed -i "/cc_generic_services/c\\option cc_generic_services = false;" ${TMP_PROTO}
            COMMENT "Copying ${msg} to ${TMP_PROTO} and replacing cc_generic_services option"
            VERBATIM
        )

        list(APPEND GRPC_PROTOS ${TMP_PROTO})
    endforeach()

    foreach(grpc_proto ${GRPC_PROTOS})
        get_filename_component(FIL_WE ${grpc_proto} NAME_WE)

        set(FILE_PREFIX_NAME "${PROTO_OUTPUT_DIR}/${FIL_WE}")
        list(APPEND GRPC_PROTO_SRCS "${FILE_PREFIX_NAME}.pb.cc")
        list(APPEND GRPC_PROTO_HDRS "${FILE_PREFIX_NAME}.pb.h")
        list(APPEND GRPC_PROTO_GRPC_SRCS "${FILE_PREFIX_NAME}.grpc.pb.cc")
        list(APPEND GRPC_PROTO_GRPC_HDRS "${FILE_PREFIX_NAME}.grpc.pb.h")

        add_custom_command(
          OUTPUT "${FILE_PREFIX_NAME}.pb.cc"
                 "${FILE_PREFIX_NAME}.pb.h"
                 "${FILE_PREFIX_NAME}.grpc.pb.cc"
                 "${FILE_PREFIX_NAME}.grpc.pb.h"
          COMMAND  ${PROTOC}
          ARGS --grpc_out "${PROTO_OUTPUT_DIR}"
            --cpp_out "${PROTO_OUTPUT_DIR}"
            -I "${GRPC_PROTO_DIR}"
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            "${grpc_proto}"
          DEPENDS ${GRPC_PROTOS}
          COMMENT "Running GRpc protocol buffer compiler on ${grpc_proto}"
          VERBATIM
        )
    endforeach()

    set_source_files_properties(
      ${GRPC_PROTO_SRCS} 
      ${GRPC_PROTO_HDRS} 
      ${GRPC_PROTO_GRPC_SRCS} 
      ${GRPC_PROTO_GRPC_HDRS} 
      PROPERTIES GENERATED TRUE)

    set(PROTO_OBJS_SRC
          ${GRPC_PROTO_SRCS} 
          ${GRPC_PROTO_HDRS} 
          ${GRPC_PROTO_GRPC_SRCS} 
          ${GRPC_PROTO_GRPC_HDRS} 
    )

else()
    set(PROTO_SRCS "")
    set(PROTO_HDRS "")

    foreach(msg ${MSG_PROTOS})
            get_filename_component(FIL_WE ${msg} NAME_WE)

            message(STATUS "proto file: ${msg}")

            set(FILE_PREFIX_NAME "${PROTO_OUTPUT_DIR}/${FIL_WE}")
            list(APPEND PROTO_SRCS "${FILE_PREFIX_NAME}.pb.cc")
            list(APPEND PROTO_HDRS "${FILE_PREFIX_NAME}.pb.h")

            add_custom_command(
              OUTPUT "${FILE_PREFIX_NAME}.pb.cc"
                 "${FILE_PREFIX_NAME}.pb.h"
              COMMAND  ${PROTOC}
              ARGS --cpp_out  ${PROTO_OUTPUT_DIR}
                -I ${PROTOS_DIR}
                ${msg}
              DEPENDS ${msg}
              COMMENT "Running C++ protocol buffer compiler on ${msg}"
              VERBATIM
            )
    endforeach()

    set_source_files_properties(
      ${PROTO_SRCS} 
      ${PROTO_HDRS} 
      PROPERTIES GENERATED TRUE)

    set(PROTO_OBJS_SRC
        ${PROTO_SRCS} 
        ${PROTO_HDRS} 
    )
endif(SDK_ENABLE_GRPC)


add_library(PROTO_OBJS OBJECT 
      ${PROTO_OBJS_SRC} 
)
set_target_properties(PROTO_OBJS
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
)

# include dir
include_directories(${THIRD_PARTY_INSTALL_PATH}/include)
include_directories(${PROJECT_SOURCE_DIR}/serial/src)
include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${CMAKE_BINARY_DIR})

# serial source file
file(GLOB_RECURSE DINGO_SERIAL_SOURCES ${PROJECT_SOURCE_DIR}/serial/src/*.cc)
file(GLOB_RECURSE DINGO_SERIAL_SCHEMA_SOURCES ${PROJECT_SOURCE_DIR}/serial/src/schema*.cc)
add_library(SERRIAL_OJBS OBJECT
  ${DINGO_SERIAL_SOURCES}
  ${DINGO_SERIAL_SCHEMA_SOURCES}
)
set_target_properties(SERRIAL_OJBS
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
)

add_subdirectory(src/sdk)

if(BUILD_SDK_EXAMPLE)
  message(STATUS "Build sdk example")
  add_subdirectory(src/example)
endif()

if(BUILD_BENCHMARK)
  message(STATUS "Build benchmark")
  add_subdirectory(src/benchmark)
endif()

if(BUILD_PYTHON_SDK)
  message(STATUS "Build python sdk")
  add_subdirectory(python)
endif()

if(BUILD_INTEGRATION_TESTS)
  message(STATUS "Build integration test")
  add_subdirectory(test/integration_test)
endif()

if(BUILD_UNIT_TESTS)
  message(STATUS "Build unit test")
  add_subdirectory(test/unit_test/sdk)
endif()

set(DINGOSDK_PUBLIC_INCLUDE_DIR "include/dingosdk")
if(DINGOSDK_INSTALL)
  # include(GNUInstallDirs) 

  install(TARGETS sdk 
    EXPORT dingosdkTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )

  install(
    FILES 
    "${DINGOSDK_PUBLIC_INCLUDE_DIR}/client.h"
    "${DINGOSDK_PUBLIC_INCLUDE_DIR}/document.h"
    "${DINGOSDK_PUBLIC_INCLUDE_DIR}/vector.h"
    "${DINGOSDK_PUBLIC_INCLUDE_DIR}/slice.h"
    "${DINGOSDK_PUBLIC_INCLUDE_DIR}/status.h"
    "${DINGOSDK_PUBLIC_INCLUDE_DIR}/types.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/dingosdk"
  )

  include(CMakePackageConfigHelpers)
  configure_package_config_file(
    "cmake/${PROJECT_NAME}Config.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
  )
  install(
    EXPORT dingosdkTargets
    NAMESPACE dingo::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  install(
    FILES
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
endif(DINGOSDK_INSTALL)
  