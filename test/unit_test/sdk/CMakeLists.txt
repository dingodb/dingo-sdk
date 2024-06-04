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

find_package(GTest CONFIG REQUIRED)
message(STATUS "Using GTest ${GTest_VERSION}")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/)

file(GLOB SDK_UNIT_TEST_RAWKV_SRCS "rawkv/*.cc")
file(GLOB SDK_UNIT_TEST_TRANSACTION_SRCS "transaction/*.cc")
file(GLOB SDK_UNIT_TEST_VECTOR_SRCS "vector/*.cc")

set(SDK_UNIT_TEST_SRCS
  test_meta_cache.cc
  test_region.cc
  test_store_rpc_controller.cc
  test_thread_pool_actuator.cc
  test_auto_increment_manager.cc
  utils/test_coding.cc
  expression/test_langchain_expr_encoder.cc
  ${SDK_UNIT_TEST_RAWKV_SRCS}
  ${SDK_UNIT_TEST_TRANSACTION_SRCS}
  ${SDK_UNIT_TEST_VECTOR_SRCS}
)

# add_library(SDK_UNIT_TEST_OBJS
#             OBJECT
#             ${SDK_UNIT_TEST_SRCS}
# )

# add_dependencies(SDK_UNIT_TEST_OBJS sdk)

add_executable(sdk_unit_test
  main.cc
  ${SDK_UNIT_TEST_SRCS}
)

target_link_libraries(sdk_unit_test
  sdk
  GTest::gtest
  GTest::gmock
)