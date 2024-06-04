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
project(dingo-sdk C CXX)

option(SDK_ENABLE_GRPC "Build sdk with grpc instead brpc" ON)

set(CMAKE_CXX_STANDARD 17)
message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

message(STATUS "SYSTEM: ${CMAKE_HOST_SYSTEM_NAME} ${CMAKE_HOST_SYSTEM_PROCESSOR}")

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)

# third-party install path
set(THIRD_PARTY_INSTALL_PATH ${PROJECT_SOURCE_DIR}/third-party/installed)
set(CMAKE_PREFIX_PATH ${THIRD_PARTY_INSTALL_PATH})
message(STATUS "THIRD_PARTY_INSTALL_PATH:${THIRD_PARTY_INSTALL_PATH}, CMAKE_PREFIX_PATH:{CMAKE_PREFIX_PATH}")

# find third-party
set(gflags_DIR ${THIRD_PARTY_INSTALL_PATH}/lib/cmake/gflags)
find_package(gflags REQUIRED)
message(STATUS "Using gflags ${gflags_VERSION}, include_dir:${gflags_INCLUDE_DIR}")

find_package(fmt REQUIRED)
message(STATUS "Using fmt ${fmt_VERSION}")

set(ZLIB_USE_STATIC_LIBS "ON")
find_package(ZLIB REQUIRED)
message(STATUS "Using zlib ${ZLIB_VERSION}, include_dir:${ZLIB_INCLUDE_DIR}")

find_package (glog REQUIRED)
add_compile_definitions(GLOG_USE_GLOG_EXPORT)
message(STATUS "Using glog ${glog_VERSION}")

# # https://github.com/llvm/llvm-project/blob/main/cmake/Modules/FindGRPC.cmake
# This setup requires gRPC to be built from sources using CMake and installed
# to ${GRPC_INSTALL_PATH} via -DCMAKE_INSTALL_PREFIX=${GRPC_INSTALL_PATH}.
# Libraries will be linked according to gRPC build policy which generates
# static libraries when BUILD_SHARED_LIBS is Off and dynamic libraries when
# it's On (NOTE: This is a variable passed to gRPC CMake build invocation,
# LLVM's BUILD_SHARED_LIBS has no effect).
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}, include_dir:${Protobuf_INCLUDE_DIRS}" )
# include_directories(${Protobuf_INCLUDE_DIRS})

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
message(STATUS "Using grpc_cpp_plugin ${GRPC_CPP_PLUGIN}")

find_program(PROTOC protoc REQUIRED)
message(STATUS "Using protoc ${PROTOC}")

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

  

# grpc proto file
set(GRPC_PROTO_DIR ${CMAKE_BINARY_DIR}/proto_tmp)
add_custom_target(create_grpc_proto_dir ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GRPC_PROTO_DIR}
)

set(GRPC_PROTO_OUTPUT_DIR ${CMAKE_BINARY_DIR}/proto_grpc)
add_custom_target(create_grpc_proto_output_dir ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GRPC_PROTO_OUTPUT_DIR}
)
message(STATUS "grpc proto_tmp:${GRPC_PROTO_DIR}, proto_grpc:${GRPC_PROTO_OUTPUT_DIR}")

set(GRPC_PROTOS "")

set(GRPC_PROTO_SRCS "")
set(GRPC_PROTO_HDRS "")
set(GRPC_PROTO_GRPC_SRCS "")
set(GRPC_PROTO_GRPC_HDRS "")
set(PROTOS_DIR ${PROJECT_SOURCE_DIR}/dingo-store-proto/proto)
message(STATUS "PROTOS_DIR:${PROTOS_DIR}")
file(GLOB_RECURSE MSG_PROTOS ${PROTOS_DIR}/*.proto)
# First loop to copy proto files
foreach(msg ${MSG_PROTOS})
    get_filename_component(FIL_WE ${msg} NAME_WE)
    set(TMP_PROTO ${GRPC_PROTO_DIR}/${FIL_WE}.proto)
    message(STATUS "proto:${msg}, grpc_proto: ${TMP_PROTO}")

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

    set(FILE_PREFIX_NAME "${GRPC_PROTO_OUTPUT_DIR}/${FIL_WE}")
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
      ARGS --grpc_out "${GRPC_PROTO_OUTPUT_DIR}"
        --cpp_out "${GRPC_PROTO_OUTPUT_DIR}"
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

add_library(GPRC_PROTO_OBJS OBJECT 
      ${GRPC_PROTO_SRCS} 
      ${GRPC_PROTO_HDRS} 
      ${GRPC_PROTO_GRPC_SRCS} 
      ${GRPC_PROTO_GRPC_HDRS} 
)

set_target_properties(GPRC_PROTO_OBJS
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON)

# include dir
include_directories(${THIRD_PARTY_INSTALL_PATH}/include)
include_directories(${PROJECT_SOURCE_DIR}/serial/src)
include_directories(${PROJECT_SOURCE_DIR}/src)
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

if(SDK_ENABLE_GRPC)
    add_definitions(-DUSE_GRPC=1)
endif()

add_subdirectory(src/sdk)
add_subdirectory(src/example)
add_subdirectory(python)

add_subdirectory(test/unit_test/sdk)
