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
project(pysdk)

set(CMAKE_CXX_STANDARD 17)
message("CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")

find_package(Python 3.9 COMPONENTS Interpreter Development.Module REQUIRED)

if(NOT THIRD_PARTY_INSTALL_PATH)
  set(THIRD_PARTY_INSTALL_PATH "$ENV{HOME}/.local/dingo-eureka")
endif()
list(PREPEND CMAKE_PREFIX_PATH ${THIRD_PARTY_INSTALL_PATH})

find_package(nanobind CONFIG REQUIRED)
message(STATUS "Found nanobind v${nanobind_VERSION}")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

nanobind_add_module(dingosdk NB_STATIC
  src/wrapper.cc
  src/status_bindings.cc
  src/client_bindings.cc
  src/vector_bindings.cc
  src/types_bindings.cc
  src/document_bindings.cc
  src/document_index_bindings.cc
  src/vector_index_bindings.cc
)

target_link_libraries(dingosdk
  PRIVATE
  sdk
)

set_target_properties(dingosdk PROPERTIES
                                INTERPROCEDURAL_OPTIMIZATION ON
                                VISIBILITY_INLINES_HIDDEN ON)

install(TARGETS dingosdk DESTINATION .)

configure_file(examples/pysdk_rawkv_example.py pysdk_rawkv_example.py COPYONLY)
configure_file(examples/pysdk_vector_example.py pysdk_vector_example.py COPYONLY)

