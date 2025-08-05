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

#ifndef DINGODB_SDK_REGION_H_
#define DINGODB_SDK_REGION_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "dingosdk/metric.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/utils/net_util.h"
#include "sdk/utils/rw_lock.h"
#include "sdk/common/common.h"

namespace dingodb {
namespace sdk {

class MetaCache;

enum RaftRole : uint8_t { kLeader, kFollower };

struct Replica {
  EndPoint end_point;
  RaftRole role;
};

class Region {
 public:
  Region(const Region&) = delete;
  const Region& operator=(const Region&) = delete;

  explicit Region(int64_t id, pb::common::Range range, pb::common::RegionEpoch epoch, pb::common::RegionType type,
                  std::vector<Replica> replicas);

  ~Region() = default;

  int64_t RegionId() const { return region_id_; }

  const Range& GetRange() const { return range_; }

  const RegionEpoch& GetEpoch() const { return epoch_; }

  RegionType GetRegionType() const { return region_type_; }

  std::vector<Replica> Replicas();

  std::vector<EndPoint> ReplicaEndPoint();

  void MarkLeader(const EndPoint& end_point);

  void MarkFollower(const EndPoint& end_point);

  Status GetLeader(EndPoint& leader);

  bool IsStale() { return stale_.load(std::memory_order_relaxed); }

  std::string ReplicasAsString();

  std::string DescribeEpoch() const { return epoch_.ToString(); }

  std::string ToString();

  void TEST_MarkStale() {  // NOLINT
    MarkStale();
  }

  void TEST_UnMarkStale() {  // NOLINT
    UnMarkStale();
  }

 private:
  friend class MetaCache;

  void MarkStale() { stale_.store(true, std::memory_order_relaxed); }

  void UnMarkStale() { stale_.store(false, std::memory_order_relaxed); }

  std::string ReplicasAsStringUnlocked() const;

  const int64_t region_id_;
  Range range_;
  RegionEpoch epoch_;
  RegionType region_type_;

  RWLock rw_lock_;
  EndPoint leader_addr_;
  std::vector<Replica> replicas_;

  std::atomic<bool> stale_;
};

inline std::ostream& operator<<(std::ostream& os, Region& region) { return os << region.ToString(); }

static std::string RaftRoleName(const RaftRole& role) {
  switch (role) {
    case kLeader:
      return "Leader";
    case kFollower:
      return "Follower";
    default:
      CHECK(false) << "role is illeagal";
  }
}

using RegionPtr = std::shared_ptr<Region>;

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_REGION_H_