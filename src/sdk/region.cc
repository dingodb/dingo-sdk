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

#include "sdk/region.h"

#include "common/logging.h"
#include "sdk/common/helper.h"

namespace dingodb {
namespace sdk {

Region::Region(int64_t id, pb::common::Range range, pb::common::RegionEpoch epoch, pb::common::RegionType type,
               std::vector<Replica> replicas)
    : region_id_(id),
      range_(std::move(range)),
      epoch_(std::move(epoch)),
      region_type_(type),
      replicas_(std::move(replicas)),
      stale_(true) {
  for (auto& r : replicas_) {
    if (r.role == kLeader) {
      leader_addr_ = r.end_point;
      break;
    }
  }
}

std::vector<Replica> Region::Replicas() {
  ReadLockGuard guard(rw_lock_);
  return replicas_;
}

std::vector<EndPoint> Region::ReplicaEndPoint() {
  ReadLockGuard guard(rw_lock_);

  std::vector<EndPoint> end_points;
  end_points.reserve(replicas_.size());
  for (const auto& r : replicas_) {
    end_points.push_back(r.end_point);
  }

  return end_points;
}

void Region::MarkLeader(const EndPoint& end_point) {
  WriteLockGuard guard(rw_lock_);

  for (auto& r : replicas_) {
    if (r.end_point == end_point) {
      r.role = kLeader;
    } else {
      r.role = kFollower;
    }
  }

  leader_addr_ = end_point;

  DINGO_LOG(INFO) << "region:" << region_id_ << " replicas:" << ReplicasAsStringUnlocked();
}

void Region::MarkFollower(const EndPoint& end_point) {
  WriteLockGuard guard(rw_lock_);

  for (auto& r : replicas_) {
    if (r.end_point == end_point) {
      r.role = kFollower;
    }
  }

  if (leader_addr_ == end_point) {
    leader_addr_.ReSet();
  }

  DINGO_LOG(INFO) << "region:" << region_id_ << " mark replica:" << end_point.ToString()
                  << " follower, current replicas:" << ReplicasAsStringUnlocked();
}

Status Region::GetLeader(EndPoint& leader) {
  ReadLockGuard guard(rw_lock_);

  if (leader_addr_.IsValid()) {
    leader = leader_addr_;
    return Status::OK();
  }

  std::string msg = fmt::format("region:{} not found leader", region_id_);
  DINGO_LOG(WARNING) << msg << " replicas:" << ReplicasAsStringUnlocked();
  return Status::NotFound(msg);
}

std::string Region::ReplicasAsString() {
  ReadLockGuard guard(rw_lock_);

  return ReplicasAsStringUnlocked();
}

std::string Region::ReplicasAsStringUnlocked() const {
  std::string replicas_str;
  for (const auto& r : replicas_) {
    if (!replicas_str.empty()) {
      replicas_str.append(", ");
    }

    std::string msg = fmt::format("({},{})", r.end_point.ToString(), RaftRoleName(r.role));
    replicas_str.append(msg);
  }
  return replicas_str;
}

std::string Region::ToString() {
  ReadLockGuard guard(rw_lock_);

  // region_id, start_key-end_key, version, config_version, type, replicas
  return fmt::format("({}, [{}-{}], [{},{}], {}, {})", region_id_, StringToHex(range_.start_key()),
                     StringToHex(range_.end_key()), epoch_.version(), epoch_.conf_version(),
                     RegionType_Name(region_type_), ReplicasAsStringUnlocked());
}

}  // namespace sdk
}  // namespace dingodb