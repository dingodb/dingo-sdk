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

#ifndef DINGODB_SDK_COORDINATOR_H_
#define DINGODB_SDK_COORDINATOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "dingosdk/status.h"

namespace dingodb {
namespace sdk {

class ClientStub;

struct TableIncrement {
  int64_t table_id;
  int64_t start_id;
};

struct Location {
  std::string host;
  int32_t port;
};

struct MDS {
  enum class State {
    kInit = 0,
    kNormal = 1,
    kAbnormal = 2,
  };

  int64_t id;
  Location location;

  State state;

  uint64_t register_time_ms;
  uint64_t last_online_time_ms;
};

class Coordinator {
 public:
  Coordinator(const ClientStub& stub) : stub_(stub) {}

  Status ScanRegions(const std::string& start_key, const std::string& end_key, std::vector<int64_t>& region_ids);

  // Auto Increment
  Status CreateAutoIncrement(int64_t table_id, int64_t start_id);
  Status DeleteAutoIncrement(int64_t table_id);
  Status UpdateAutoIncrement(int64_t table_id, int64_t start_id);
  Status GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id);
  Status GetAutoIncrement(int64_t table_id, int64_t& start_id);
  Status GetAutoIncrements(std::vector<TableIncrement>& table_increments);

  // MDS
  Status MDSHeartbeat(const MDS& mds);
  Status GetMDSList(std::vector<MDS>& mdses);

 private:
  const ClientStub& stub_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_COORDINATOR_H_