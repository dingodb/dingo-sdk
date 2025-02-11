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

set(SDK_SRCS
  admin_tool.cc
  auto_increment_manager.cc
  client_stub.cc
  client.cc
  coordinator.cc
  version.cc
  meta_cache.cc
  meta_member_info.cc
  region.cc
  slice.cc
  status.cc
  rawkv/raw_kv_task.cc
  rawkv/raw_kv_get_task.cc
  rawkv/raw_kv_batch_get_task.cc
  rawkv/raw_kv_put_task.cc
  rawkv/raw_kv_batch_put_task.cc
  rawkv/raw_kv_put_if_absent_task.cc
  rawkv/raw_kv_batch_put_if_absent_task.cc
  rawkv/raw_kv_delete_task.cc
  rawkv/raw_kv_batch_delete_task.cc
  rawkv/raw_kv_compare_and_set_task.cc
  rawkv/raw_kv_batch_compare_and_set_task.cc
  rawkv/raw_kv_delete_range_task.cc
  rawkv/raw_kv_scan_task.cc
  rawkv/raw_kv_region_scanner_impl.cc
  rpc/coordinator_rpc_controller.cc
  rpc/store_rpc_controller.cc
  transaction/txn_buffer.cc
  transaction/txn_impl.cc
  transaction/txn_lock_resolver.cc
  transaction/txn_region_scanner_impl.cc
  vector/vector_client.cc
  vector/vector_index_cache.cc
  vector/vector_index_creator.cc
  vector/vector_index.cc
  vector/vector_param.cc
  vector/vector_task.cc
  vector/vector_add_task.cc
  vector/vector_batch_query_task.cc
  vector/vector_count_task.cc
  vector/vector_delete_task.cc
  vector/vector_get_border_task.cc
  vector/vector_get_index_metrics_task.cc
  vector/vector_scan_query_task.cc
  vector/vector_search_task.cc
  vector/vector_upsert_task.cc
  vector/vector_get_auto_increment_id_task.cc
  vector/vector_update_auto_increment_task.cc
  vector/diskann/vector_diskann_count_memory_task.cc
  vector/diskann/vector_diskann_reset_by_index_task.cc
  vector/diskann/vector_diskann_reset_by_region_task.cc
  vector/diskann/vector_diskann_status_by_index_task.cc
  vector/diskann/vector_diskann_status_by_region_task.cc
  vector/diskann/vector_diskann_build_by_index_task.cc
  vector/diskann/vector_diskann_build_by_region_task.cc
  vector/diskann/vector_diskann_load_by_index_task.cc
  vector/diskann/vector_diskann_load_by_region_task.cc
  vector/diskann/vector_diskann_import_task.cc
  vector/diskann/vector_diskann_dump.cc
  document/document_client.cc
  document/document_index_creator.cc
  document/document_param.cc
  document/document_index_cache.cc
  document/document_index.cc
  document/document_task.cc
  document/document_add_task.cc
  document/document_batch_query_task.cc
  document/document_count_task.cc
  document/document_delete_task.cc
  document/document_get_border_task.cc
  document/document_get_index_metrics_task.cc
  document/document_scan_query_task.cc
  document/document_search_task.cc
  document/document_search_all_task.cc
  document/document_update_task.cc
  document/document_get_auto_increment_id_task.cc
  document/document_update_auto_increment_task.cc
  utils/thread_pool_actuator.cc
  utils/thread_pool_impl.cc
  common/param_config.cc
  expression/coding.cc
  expression/langchain_expr_encoder.cc
  expression/langchain_expr_factory.cc
  expression/langchain_expr.cc
)

if(SDK_ENABLE_GRPC)
    message(STATUS "Build sdk with grpc")

    list(APPEND SDK_SRCS
        rpc/grpc/grpc_rpc_client.cc
        rpc/grpc/coordinator_rpc.cc
        rpc/grpc/index_service_rpc.cc
        rpc/grpc/store_rpc.cc
        rpc/grpc/document_service_rpc.cc
        rpc/grpc/version_rpc.cc
    )

    add_library(sdk
        ${SDK_SRCS}
        $<TARGET_OBJECTS:PROTO_OBJS>
        $<TARGET_OBJECTS:SERRIAL_OJBS>
    )

    set_target_properties(sdk
      PROPERTIES
      VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR}
      POSITION_INDEPENDENT_CODE ON
    )

    target_link_libraries(sdk
      PRIVATE
        grpc++ 
        grpc_protobuf
        fmt::fmt
        gflags
        glog::glog
        ZLIB::ZLIB
        dl 
        Threads::Threads
    )

else()
    message(STATUS "Build sdk with brpc")

    find_package(Snappy)
    message("Using Snappy ${Snappy_VERSION}")

    list(APPEND SDK_SRCS
        rpc/brpc/brpc_rpc_client.cc
        rpc/brpc/coordinator_rpc.cc
        rpc/brpc/index_service_rpc.cc
        rpc/brpc/store_rpc.cc
        rpc/brpc/document_service_rpc.cc
        rpc/brpc/version_rpc.cc
    )

    add_library(sdk
        ${SDK_SRCS}
        $<TARGET_OBJECTS:PROTO_OBJS>
        $<TARGET_OBJECTS:SERRIAL_OJBS>
    )

    set_target_properties(sdk
      PROPERTIES
      VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR}
      POSITION_INDEPENDENT_CODE ON
    )

   target_link_libraries(sdk
      PRIVATE
        brpc
        gflags
        leveldb
        Snappy::snappy
        ZLIB::ZLIB
        fmt::fmt
        glog::glog
        protobuf::libprotobuf
        OpenSSL::SSL
        dl 
        Threads::Threads
   )
endif()

