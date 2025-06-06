// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_SDK_METRIC_H_
#define DINGODB_SDK_METRIC_H_

#include <cstdint>
#include <iostream>
#include <sstream>

namespace dingodb {

namespace sdk {
struct StoreOwnMetics {
  int64_t store_id{0};                 // store id
  int64_t system_total_capacity{0};    // total capacity of this store
  int64_t system_free_capacity{0};     // free capacity of this store
  float system_capacity_usage{0};      // capacity usage of this store
  int64_t system_cpu_usage{0};         // cpu usage of this store process
  int64_t system_total_memory{0};      // total memory of the host this store process running on
  int64_t system_free_memory{0};       // total free memory of the host this store process running on
  int64_t system_shared_memory{0};     // shared memory of the host this store process running on
  int64_t system_buffer_memory{0};     // buffer memory of the host this store process running on
  int64_t system_cached_memory{0};     // cache memory of the host this store process running on
  int64_t system_available_memory{0};  // available memory of the host this store process running on
  int64_t system_total_swap{0};        // total swap of the host this store process running on
  int64_t system_free_swap{0};         // total free swap of the host this store process running on
  int64_t process_used_memory{0};      // total used memory of this store process

  std::string ToString() const {
    std::ostringstream oss;
    oss << "StoreMetics: {";
    oss << "store_id: " << store_id << ", ";
    oss << "system_total_capacity" << system_total_capacity << ", ";
    oss << "system_free_capacity: " << system_free_capacity << ", ";
    oss << "system_cpu_usage: " << system_cpu_usage << ", ";
    oss << "system_total_memory: " << system_total_memory << ", ";
    oss << "system_free_memory: " << system_free_memory << ", ";
    oss << "system_shared_memory: " << system_shared_memory << ", ";
    oss << "system_available_memory: " << system_available_memory << ", ";
    oss << "system_buffer_memory: " << system_buffer_memory << ", ";
    oss << "system_cached_memory: " << system_cached_memory << ", ";
    oss << "system_total_swap: " << system_total_swap << ", ";
    oss << "system_free_swap: " << system_free_swap << ", ";
    oss << "process_used_memory: " << process_used_memory << ", ";
    oss << "}";
    return oss.str();
  }
};

enum StoreType : uint8_t { kNodeNone, kNodeStore, kNodeIndex, kNodeDocument };

enum RegionType : uint8_t { kRegionNone, kRegionStore, kRegionIndex, kRegionDocument };

enum StoreState : uint8_t { kStoreNew, kStoreNormal, kStoreOffline };
enum StoreInState : uint8_t { kStoreIn, kStoreOut };

// tansfer pb::common::Region To RegionPB
struct RegionPB {
  int64_t id{0};                        // region id
  int64_t epoch{0};                     // region epoch
  RegionType region_type{kRegionNone};  // region type
  int64_t leader_store_id{0};           // leader store id

  // todo : add more region info
};

// tansfer pb::common::Store To StorePB
struct StorePB {
  int64_t id{0};                     // store id
  int64_t epoch{0};                  // store epoch
  StoreType store_type{kNodeNone};   // store type
  int32_t leader_num_weight{0};      // leader num weight
  StoreState state{kStoreNew};       // store state
  StoreInState in_state{kStoreOut};  // store in state
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_METRIC_H_