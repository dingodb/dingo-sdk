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

cmake_minimum_required(VERSION 3.23.1 FATAL_ERROR)
project(dingo-sdk-third-party C CXX)

SET(THIRD_PARTY_INSTALL_PATH ${CMAKE_CURRENT_SOURCE_DIR}/installed)

include(ExternalProject)

ExternalProject_Add(gflags
    PREFIX gflags
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gflags
    CMAKE_ARGS
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_TESTING=OFF
        -DINSTALL_HEADERS=ON
    CMAKE_CACHE_ARGS 
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

ExternalProject_Add(glog
    PREFIX glog
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/glog
    CMAKE_ARGS
        -DBUILD_TESTING=OFF
        -DBUILD_SHARED_LIBS=OFF
        -DWITH_GFLAGS=ON
    CMAKE_CACHE_ARGS 
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -Dgflags_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/gflags
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
    DEPENDS gflags
)

ExternalProject_Add(gtest
    PREFIX gtest
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/googletest
    CMAKE_CACHE_ARGS 
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

ExternalProject_Add(fmt
    PREFIX fmt
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/fmt
    CMAKE_ARGS 
        -DFMT_TEST=OFF
    CMAKE_CACHE_ARGS 
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

ExternalProject_Add(nlohmann-json
    PREFIX nlohmann-json
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/nlohmann-json
    CMAKE_ARGS 
        -DJSON_BuildTests=OFF
        -DJSON_MultipleHeaders=ON
    CMAKE_CACHE_ARGS 
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)


SET(GRPC_SOURCES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/grpc)

# Builds absl project from the git submodule.
ExternalProject_Add(absl
  PREFIX absl
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/abseil-cpp"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

# Builds absl project from the git submodule.
ExternalProject_Add(utf8_range
  PREFIX utf8_range
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/utf8_range"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -Dutf8_range_ENABLE_TESTS:BOOL=OFF
        -Dabsl_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/absl
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
  DEPENDS absl
)

# Builds c-ares project from the git submodule.
ExternalProject_Add(c-ares
  PREFIX c-ares
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/cares/cares"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCARES_SHARED:BOOL=OFF
        -DCARES_STATIC:BOOL=ON
        -DCARES_STATIC_PIC:BOOL=ON
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

# Builds protobuf project from the git submodule.
ExternalProject_Add(protobuf
  PREFIX protobuf
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/protobuf"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -Dprotobuf_BUILD_TESTS:BOOL=OFF
        -Dprotobuf_WITH_ZLIB:BOOL=OFF
        -Dprotobuf_ABSL_PROVIDER:STRING=package
        -Dabsl_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/absl
        -Dutf8_range_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/utf8_range
        -Dprotobuf_MSVC_STATIC_RUNTIME:BOOL=OFF
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
  DEPENDS absl utf8_range
)

# Builds re2 project from the git submodule.
ExternalProject_Add(re2
  PREFIX re2
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/re2"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

# Builds zlib project from the git submodule.
ExternalProject_Add(zlib
  PREFIX zlib
  SOURCE_DIR "${GRPC_SOURCES_DIR}/third_party/zlib"
  CMAKE_ARGS
      -DBUILD_SHARED_LIBS=OFF
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
)

# the location where protobuf-config.cmake will be installed varies by platform
if (WIN32)
  set(_FINDPACKAGE_PROTOBUF_CONFIG_DIR "${THIRD_PARTY_INSTALL_PATH}/cmake")
else()
  set(_FINDPACKAGE_PROTOBUF_CONFIG_DIR "${THIRD_PARTY_INSTALL_PATH}/lib/cmake/protobuf")
endif()

# if OPENSSL_ROOT_DIR is set, propagate that hint path to the external projects with OpenSSL dependency.
set(_CMAKE_ARGS_OPENSSL_ROOT_DIR "")
if (OPENSSL_ROOT_DIR)
  set(_CMAKE_ARGS_OPENSSL_ROOT_DIR "-DOPENSSL_ROOT_DIR:PATH=${OPENSSL_ROOT_DIR}")
endif()

# Builds gRPC based on locally checked-out sources and set arguments so that all the dependencies
# are correctly located.
ExternalProject_Add(grpc
  PREFIX grpc
  SOURCE_DIR "${GRPC_SOURCES_DIR}"
  CMAKE_CACHE_ARGS
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DgRPC_INSTALL:BOOL=ON
        -DgRPC_BUILD_TESTS:BOOL=OFF
        -DgRPC_BUILD_MSVC_MP_COUNT:STRING=-1
        -Dutf8_range_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/utf8_range
        -DgRPC_PROTOBUF_PROVIDER:STRING=package
        -DProtobuf_DIR:PATH=${_FINDPACKAGE_PROTOBUF_CONFIG_DIR}
        -DgRPC_RE2_PROVIDER:STRING=package
        -Dre2_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/re2
        -DgRPC_ZLIB_PROVIDER:STRING=package
        -DZLIB_ROOT:STRING=${THIRD_PARTY_INSTALL_PATH}
        -DgRPC_ABSL_PROVIDER:STRING=package
        -Dabsl_DIR:STRING=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/absl
        -DgRPC_CARES_PROVIDER:STRING=package
        -Dc-ares_DIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib/cmake/c-ares
        -DgRPC_SSL_PROVIDER:STRING=package
        ${_CMAKE_ARGS_OPENSSL_ROOT_DIR}
        -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_INSTALL_PATH}
        -DCMAKE_INSTALL_LIBDIR:PATH=${THIRD_PARTY_INSTALL_PATH}/lib
  DEPENDS c-ares protobuf re2 zlib absl
)