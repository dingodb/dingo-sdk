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

add_executable(sdk_rawkv_example
    sdk_rawkv_example.cc)
target_link_libraries(sdk_rawkv_example
    sdk
)

add_executable(sdk_transaction_example
    sdk_transaction_example.cc)
target_link_libraries(sdk_transaction_example
    sdk
)

add_executable(sdk_transaction_gc_example
    sdk_transaction_gc_example.cc)
target_link_libraries(sdk_transaction_gc_example
    sdk
)

add_executable(sdk_transaction_gc_op_all_and_lock_example
    sdk_transaction_gc_op_all_and_lock_example.cc)
target_link_libraries(sdk_transaction_gc_op_all_and_lock_example
    sdk
)

add_executable(sdk_vector_example
    sdk_vector_example.cc)
target_link_libraries(sdk_vector_example
    sdk
)

add_executable(sdk_document_example
    sdk_document_example.cc)
target_link_libraries(sdk_document_example
    sdk
)

add_executable(txn_scan_test
    txn_scan_test.cc)
target_link_libraries(txn_scan_test
    sdk
)
