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

#include "sdk/meta_cache.h"

#include <fmt/format.h>

#include <string_view>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/region.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {

using pb::coordinator::ScanRegionInfo;

Status MetaCache::LookupRegionByKey(std::string_view key, std::shared_ptr<Region>& region) {
  DINGO_LOG(DEBUG) << fmt::format("LookupRegionByKey key:{}", StringToHex(key));
  CHECK(!key.empty()) << "key should not empty";
  Status s;
  {
    ReadLockGuard guard(rw_lock_);

    s = FastLookUpRegionByKeyUnlocked(key, region);
    if (s.IsOK()) {
      return s;
    }
  }

  s = SlowLookUpRegionByKey(key, region);
  return s;
}

Status MetaCache::LookupRegionByRegionId(int64_t region_id, std::shared_ptr<Region>& region) {
  DINGO_LOG(DEBUG) << fmt::format("LookupRegionByRegionId region_id:{}", region_id);
  CHECK_GT(region_id, 0) << "region_id should bigger than 0";
  Status s;
  {
    ReadLockGuard guard(rw_lock_);
    s = FastLookUpRegionByRegionIdUnlocked(region_id, region);
    if (s.IsOK()) {
      return s;
    }
  }

  s = SlowLookUpRegionByRegionId(region_id, region);
  return s;
}

Status MetaCache::LookupRegionBetweenRange(std::string_view start_key, std::string_view end_key,
                                           std::shared_ptr<Region>& region) {
  DINGO_LOG(DEBUG) << fmt::format("LookupRegionBetweenRange range: [{}, {}]", StringToHex(start_key),
                                  StringToHex(end_key));
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  Status s;
  {
    ReadLockGuard guard(rw_lock_);

    s = FastLookUpRegionByKeyUnlocked(start_key, region);
    if (s.IsOK()) {
      DINGO_LOG_IF(WARNING, start_key < region->GetRange().start_key) << fmt::format(
          "start_key is less than region start_key, range: [{}, {}], region_range: [{}, {}]", StringToHex(start_key),
          StringToHex(end_key), StringToHex(region->GetRange().start_key), StringToHex(region->GetRange().end_key));

      CHECK(end_key > region->GetRange().start_key)
          << fmt::format("end_key should greater than region start_key, range: [{}, {}], region_range: [{}, {}]",
                         StringToHex(start_key), StringToHex(end_key), StringToHex(region->GetRange().start_key),
                         StringToHex(region->GetRange().end_key));
      return s;
    }
  }

  std::vector<std::shared_ptr<Region>> regions;
  s = ScanRegionsBetweenRange(start_key, end_key, kPrefetchRegionCount, regions);
  if (s.IsOK() && !regions.empty()) {
    region = std::move(regions.front());

    DINGO_LOG_IF(WARNING, start_key < region->GetRange().start_key) << fmt::format(
        "start_key is less than region start_key, range: [{}, {}], region_range: [{}, {}]", StringToHex(start_key),
        StringToHex(end_key), StringToHex(region->GetRange().start_key), StringToHex(region->GetRange().end_key));

    CHECK(end_key > region->GetRange().start_key) << fmt::format(
        "end_key should greater than region start_key, range: [{}, {}], region_range: [{}, {}]", StringToHex(start_key),
        StringToHex(end_key), StringToHex(region->GetRange().start_key), StringToHex(region->GetRange().end_key));
  }

  return s;
}

Status MetaCache::LookupRegionBetweenRangeNoPrefetch(std::string_view start_key, std::string_view end_key,
                                                     std::shared_ptr<Region>& region) {
  DINGO_LOG(DEBUG) << fmt::format("LookupRegionBetweenRangeNoPrefetch range: [{}, {}]", StringToHex(start_key),
                                  StringToHex(end_key));
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  Status s;
  {
    ReadLockGuard guard(rw_lock_);

    s = FastLookUpRegionByKeyUnlocked(start_key, region);
    if (s.IsOK()) {
      DINGO_LOG_IF(WARNING, start_key < region->GetRange().start_key) << fmt::format(
          "start_key is less than region start_key, range: [{}, {}], region_range: [{}, {}]", StringToHex(start_key),
          StringToHex(end_key), StringToHex(region->GetRange().start_key), StringToHex(region->GetRange().end_key));

      CHECK(end_key > region->GetRange().start_key)
          << fmt::format("end_key should greater than region start_key, range: [{}, {}], region_range: [{}, {}]",
                         StringToHex(start_key), StringToHex(end_key), StringToHex(region->GetRange().start_key),
                         StringToHex(region->GetRange().end_key));
      return s;
    }
  }

  std::vector<std::shared_ptr<Region>> regions;
  s = ScanRegionsBetweenRange(start_key, end_key, 1, regions);
  if (s.IsOK() && !regions.empty()) {
    region = std::move(regions.front());
    DINGO_LOG_IF(WARNING, start_key < region->GetRange().start_key) << fmt::format(
        "start_key is less than region start_key, range: [{}, {}], region_range: [{}, {}]", StringToHex(start_key),
        StringToHex(end_key), StringToHex(region->GetRange().start_key), StringToHex(region->GetRange().end_key));

    CHECK(end_key > region->GetRange().start_key) << fmt::format(
        "end_key should greater than region start_key, range: [{}, {}], region_range: [{}, {}]", StringToHex(start_key),
        StringToHex(end_key), StringToHex(region->GetRange().start_key), StringToHex(region->GetRange().end_key));
  }

  return s;
}

Status MetaCache::ScanRegionsBetweenRange(std::string_view start_key, std::string_view end_key, int64_t limit,
                                          std::vector<std::shared_ptr<Region>>& regions) {
  DINGO_LOG(DEBUG) << fmt::format("ScanRegionsBetweenRange limit:{}, range: [{}, {}]", limit, StringToHex(start_key),
                                  StringToHex(end_key));
  CHECK(!start_key.empty()) << "start_key should not empty";
  CHECK(!end_key.empty()) << "end_key should not empty";
  CHECK_GE(limit, 0) << "limit should greater or equal 0";
  ScanRegionsRpc rpc;
  rpc.MutableRequest()->set_key(std::string(start_key));
  rpc.MutableRequest()->set_range_end(std::string(end_key));
  rpc.MutableRequest()->set_limit(limit);

  DINGO_RETURN_NOT_OK(coordinator_rpc_controller_->SyncCall(rpc));

  return ProcessScanRegionsBetweenRangeResponse(*rpc.Response(), regions);
}

Status MetaCache::ScanRegionsBetweenContinuousRange(std::string_view start_key, std::string_view end_key,
                                                    std::vector<std::shared_ptr<Region>>& regions) {
  std::vector<std::shared_ptr<Region>> to_return;
  {
    ReadLockGuard guard(rw_lock_);

    // find region start_key >= start_key
    auto start_iter = region_by_key_.lower_bound(start_key);
    if (start_iter != region_by_key_.end() && start_iter->first == start_key) {
      // find region start_key >= end_key
      auto end_iter = region_by_key_.lower_bound(end_key);
      if (end_iter != region_by_key_.begin()) {
        end_iter--;
        if (end_iter->second->GetRange().end_key == end_key) {
          auto iter = start_iter;
          while (iter != end_iter) {
            to_return.push_back(iter->second);
            iter++;
          }
          to_return.push_back(end_iter->second);
        }
      }
    }
  }

  if (!to_return.empty()) {
    if (to_return.size() == 1) {
      auto& find = to_return[0];
      CHECK_EQ(find->GetRange().start_key, start_key);
      CHECK_EQ(find->GetRange().end_key, end_key);
      regions.swap(to_return);
      return Status::OK();
    } else {
      auto cur = to_return.begin();
      auto next = cur;
      next++;
      CHECK(next != to_return.end());

      bool continues = true;
      while (next != to_return.end()) {
        if ((*cur)->GetRange().end_key != (*next)->GetRange().start_key) {
          continues = false;
          break;
        }
        ++cur;
        ++next;
      }

      if (continues) {
        CHECK(!to_return.empty());
        regions.swap(to_return);
        return Status::OK();
      }
    }
  }

  ScanRegionsRpc rpc;
  rpc.MutableRequest()->set_key(std::string(start_key));
  rpc.MutableRequest()->set_range_end(std::string(end_key));
  rpc.MutableRequest()->set_limit(0);

  DINGO_RETURN_NOT_OK(coordinator_rpc_controller_->SyncCall(rpc));

  return ProcessScanRegionsBetweenRangeResponse(*rpc.Response(), regions);
}

void MetaCache::ClearRange(const std::shared_ptr<Region>& region) {
  WriteLockGuard guard(rw_lock_);

  auto iter = region_by_id_.find(region->RegionId());
  if (region->IsStale()) {
    DINGO_LOG(DEBUG) << "region is stale, no need clear, region:" << region->ToString();
    return;
  } else {
    CHECK(iter != region_by_id_.end());
    RemoveRegionUnlocked(region->RegionId());
  }
}

void MetaCache::RemoveRegion(int64_t region_id) {
  WriteLockGuard guard(rw_lock_);

  RemoveRegionIfPresentUnlocked(region_id);
}

void MetaCache::RemoveRegionIfPresentUnlocked(int64_t region_id) {
  if (region_by_id_.find(region_id) != region_by_id_.end()) {
    RemoveRegionUnlocked(region_id);
  }
}

void MetaCache::ClearCache() {
  WriteLockGuard guard(rw_lock_);
  for (const auto& [region_id, region] : region_by_id_) {
    region->MarkStale();
  }
  region_by_key_.clear();
  region_by_id_.clear();
}

void MetaCache::MaybeAddRegion(const std::shared_ptr<Region>& new_region) {
  if (new_region->range_.start_key >= new_region->range_.end_key) {
    DINGO_LOG(WARNING) << "err : region start_key >= region end_key\n" << new_region->ToString();
    return;
  }

  WriteLockGuard guard(rw_lock_);

  MaybeAddRegionUnlocked(new_region);
}

void MetaCache::MaybeAddRegionUnlocked(const std::shared_ptr<Region>& new_region) {
  CHECK(new_region.get() != nullptr);
  auto region_id = new_region->RegionId();
  auto iter = region_by_id_.find(region_id);
  if (iter != region_by_id_.end()) {
    // old region has same region_id
    if (NeedUpdateRegion(iter->second, new_region)) {
      // old region is stale
      RemoveRegionUnlocked(region_id);
    } else {
      // old region same epoch or newer
      return;
    }
  }

  AddRangeToCacheUnlocked(new_region);
}

Status MetaCache::FastLookUpRegionByKeyUnlocked(std::string_view key, std::shared_ptr<Region>& region) {
  auto iter = region_by_key_.upper_bound(key);
  if (iter == region_by_key_.begin()) {
    return Status::NotFound(fmt::format("not found region for key:{}", key));
  }

  iter--;
  auto found_region = iter->second;
  CHECK(!found_region->IsStale());

  auto range = found_region->GetRange();
  CHECK(key >= range.start_key) << fmt::format("key:{} is less than range start_key:{}, region:{}", StringToHex(key),
                                               StringToHex(range.start_key), found_region->ToString());

  if (key >= range.end_key) {
    std::string msg = fmt::format(
        "not found region for key:{} in cache, key is out of bounds, nearest found_region:{} range:({}-{})",
        StringToHex(key), found_region->RegionId(), StringToHex(range.start_key), StringToHex(range.end_key));

    DINGO_LOG(DEBUG) << msg;
    return Status::NotFound(msg);
  } else {
    // lucky we found it
    region = found_region;
    return Status::OK();
  }
}

Status MetaCache::SlowLookUpRegionByKey(std::string_view key, std::shared_ptr<Region>& region) {
  ScanRegionsRpc rpc;
  rpc.MutableRequest()->set_key(std::string(key));

  Status send = coordinator_rpc_controller_->SyncCall(rpc);
  if (!send.IsOK()) {
    return send;
  }

  return ProcessScanRegionsByKeyResponse(*rpc.Response(), region);
}
Status MetaCache::FastLookUpRegionByRegionIdUnlocked(int64_t region_id, std::shared_ptr<Region>& region) {
  auto it = region_by_id_.find(region_id);
  if (it == region_by_id_.end()) {
    return Status::NotFound(fmt::format("not found region for region_id:{}", region_id));
  } else {
    region = region_by_id_[region_id];
    return Status::OK();
  }
}

Status MetaCache::SlowLookUpRegionByRegionId(int64_t region_id, std::shared_ptr<Region>& region) {
  QueryRegionRpc rpc;
  rpc.MutableRequest()->set_region_id(region_id);

  Status send = coordinator_rpc_controller_->SyncCall(rpc);
  if (!send.IsOK()) {
    return send;
  }
  return ProcessQueryRegionsByRegionIdResponse(*rpc.Response(), region);
}

Status MetaCache::ProcessQueryRegionsByRegionIdResponse(const pb::coordinator::QueryRegionResponse& response,
                                                        std::shared_ptr<Region>& region) {
  if (response.has_region()) {
    const auto& region_pb = response.region();
    std::shared_ptr<Region> new_region;
    ProcesssQueryRegion(region_pb, new_region);
    {
      WriteLockGuard guard(rw_lock_);

      MaybeAddRegionUnlocked(new_region);
      auto iter = region_by_id_.find(region_pb.id());
      CHECK(iter != region_by_id_.end());
      CHECK(iter->second.get() != nullptr);
      region = iter->second;
    }
    return Status::OK();
  } else {
    DINGO_LOG(WARNING) << "response:" << response.DebugString();
    return Status::NotFound("region not found");
  }
}
Status MetaCache::ProcessScanRegionsByKeyResponse(const pb::coordinator::ScanRegionsResponse& response,
                                                  std::shared_ptr<Region>& region) {
  if (response.regions_size() > 0) {
    CHECK(response.regions_size() == 1) << "expect ScanRegionsResponse  has one region";

    const auto& scan_region_info = response.regions(0);
    std::shared_ptr<Region> new_region;
    ProcessScanRegionInfo(scan_region_info, new_region);
    {
      WriteLockGuard guard(rw_lock_);

      MaybeAddRegionUnlocked(new_region);
      auto iter = region_by_id_.find(scan_region_info.region_id());
      CHECK(iter != region_by_id_.end());
      CHECK(iter->second.get() != nullptr);
      region = iter->second;
    }
    return Status::OK();
  } else {
    DINGO_LOG(WARNING) << "response:" << response.DebugString();
    return Status::NotFound("region not found");
  }
}

Status MetaCache::ProcessScanRegionsBetweenRangeResponse(const pb::coordinator::ScanRegionsResponse& response,
                                                         std::vector<std::shared_ptr<Region>>& regions) {
  if (response.regions_size() > 0) {
    std::vector<std::shared_ptr<Region>> tmp_regions;

    for (const auto& scan_region_info : response.regions()) {
      std::shared_ptr<Region> new_region;
      ProcessScanRegionInfo(scan_region_info, new_region);
      {
        WriteLockGuard guard(rw_lock_);

        MaybeAddRegionUnlocked(new_region);
        auto iter = region_by_id_.find(scan_region_info.region_id());
        CHECK(iter != region_by_id_.end());
        CHECK(iter->second.get() != nullptr);
        tmp_regions.push_back(iter->second);
      }
    }

    CHECK(!tmp_regions.empty());
    regions = std::move(tmp_regions);

    return Status::OK();
  } else {
    DINGO_LOG(DEBUG) << "no scan_region_info in ScanRegionsResponse, response:" << response.DebugString();
    return Status::NotFound("regions not found");
  }
}

// TODO: check region state
void MetaCache::ProcessScanRegionInfo(const ScanRegionInfo& scan_region_info, std::shared_ptr<Region>& region) {
  int64_t region_id = scan_region_info.region_id();
  CHECK(scan_region_info.has_range());
  CHECK(scan_region_info.has_region_epoch());

  std::vector<Replica> replicas;
  if (scan_region_info.has_leader()) {
    const auto& leader = scan_region_info.leader();
    if (leader.host().empty() || leader.port() == 0) {
      DINGO_LOG(WARNING) << fmt::format("receive leader is invalid: {} {}", leader.host(), leader.port());
    } else {
      auto endpoint = LocationToEndPoint(leader);
      replicas.push_back({endpoint, kLeader});
    }
  }

  for (const auto& voter : scan_region_info.voters()) {
    if (voter.host().empty() || voter.port() == 0) {
      DINGO_LOG(WARNING) << fmt::format("receive voter is invalid: {} {}", voter.host(), voter.port());
    } else {
      auto endpoint = LocationToEndPoint(voter);
      replicas.push_back({endpoint, kFollower});
    }
  }

  // TODO: support learner
  for (const auto& leaner : scan_region_info.learners()) {
    if (leaner.host().empty() || leaner.port() == 0) {
      DINGO_LOG(WARNING) << fmt::format("receive voter is invalid: {} {}", leaner.host(), leaner.port());
    } else {
      auto endpoint = LocationToEndPoint(leaner);
      replicas.push_back({endpoint, kFollower});
    }
  }

  region = std::make_shared<Region>(region_id, scan_region_info.range(), scan_region_info.region_epoch(),
                                    scan_region_info.status().region_type(), replicas);
}
void MetaCache::ProcesssQueryRegion(const pb::common::Region& query_region, std::shared_ptr<Region>& new_region) {
  CHECK(query_region.has_definition());
  std::vector<Replica> replicas;
  for (const auto& peer : query_region.definition().peers()) {
    auto endpoint = LocationToEndPoint(peer.server_location());
    if (query_region.leader_store_id() == peer.store_id()) {
      replicas.push_back({endpoint, kLeader});
    } else {
      replicas.push_back({endpoint, kFollower});
    }
  }

  new_region = std::make_shared<Region>(query_region.id(), query_region.definition().range(),
                                        query_region.definition().epoch(), query_region.region_type(), replicas);
}

bool MetaCache::NeedUpdateRegion(const std::shared_ptr<Region>& old_region, const std::shared_ptr<Region>& new_region) {
  return EpochCompare(old_region->GetEpoch(), new_region->GetEpoch()) > 0;
}

void MetaCache::RemoveRegionUnlocked(int64_t region_id) {
  auto iter = region_by_id_.find(region_id);
  CHECK(iter != region_by_id_.end());

  auto region = iter->second;
  region->MarkStale();
  region_by_id_.erase(iter);

  CHECK(region_by_key_.erase(region->GetRange().start_key) == 1);

  DINGO_LOG(DEBUG) << "remove region and mark stale, region_id:" << region_id << ", region: " << region->ToString();
}

void MetaCache::AddRangeToCacheUnlocked(const std::shared_ptr<Region>& region) {
  auto region_start_key = region->GetRange().start_key;

  std::vector<std::shared_ptr<Region>> to_removes;
  auto key_iter = region_by_key_.lower_bound(region_start_key);

  // remove before range when end_key > region_start_key
  if (key_iter != region_by_key_.begin()) {
    key_iter--;
    auto to_remove_start_key = key_iter->second->GetRange().start_key;
    CHECK(to_remove_start_key < region_start_key)
        << "to_remove_start_key:" << to_remove_start_key << " expect le:" << region_start_key;

    if (key_iter->second->GetRange().end_key > region_start_key) {
      to_removes.emplace_back(key_iter->second);
    }
    key_iter++;
  }

  auto region_end_key = region->GetRange().end_key;
  // remove ranges which  region_start_key <= start_key < region_end_key
  while (key_iter != region_by_key_.end() && key_iter->second->GetRange().start_key < region_end_key) {
    to_removes.emplace_back(key_iter->second);
    key_iter++;
  }

  for (const auto& remove : to_removes) {
    RemoveRegionUnlocked(remove->RegionId());
  }

  // add region to cache
  CHECK(region_by_id_.insert(std::make_pair(region->RegionId(), region)).second);
  CHECK(region_by_key_.insert(std::make_pair(region->GetRange().start_key, region)).second);

  region->UnMarkStale();

  DINGO_LOG(DEBUG) << "add region success, region:" << region->ToString();
}

void MetaCache::Dump() {
  ReadLockGuard guard(rw_lock_);

  DumpUnlocked();
}

void MetaCache::DumpUnlocked() {
  for (const auto& r : region_by_id_) {
    std::string dump = fmt::format("region_id:{}, region:{}", r.first, r.second->ToString());
    DINGO_LOG(INFO) << dump;
  }

  for (const auto& r : region_by_key_) {
    std::string dump = fmt::format("start_key:{}, region:{}", r.first, r.second->ToString());
    DINGO_LOG(INFO) << dump;
  }
}

}  // namespace sdk
}  // namespace dingodb