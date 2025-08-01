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

#ifndef DINGODB_SDK_COMMON_H_
#define DINGODB_SDK_COMMON_H_

#include <fmt/base.h>
#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/rpc.h"
#include "sdk/utils/net_util.h"

static const int64_t kPhysicalShiftBits = 18;
static const int64_t kLogicalMask = (1 << kPhysicalShiftBits) - 1;

namespace dingodb {
namespace sdk {

enum LogLevel { kDEBUG = 0, kINFO = 1, kWARNING = 2, kERROR = 3, kFATAL = 4 };

static int64_t Tso2Timestamp(pb::meta::TsoTimestamp tso) {
  return (tso.physical() << kPhysicalShiftBits) + tso.logical();
}

// if a == b, return 0
// if a < b, return 1
// if a > b, return -1
static int EpochCompare(const pb::common::RegionEpoch& a, const pb::common::RegionEpoch& b) {
  if (b.version() > a.version()) {
    return 1;
  }

  if (b.version() < a.version()) {
    return -1;
  }

  // below version equal

  if (b.conf_version() > a.conf_version()) {
    return 1;
  }

  if (b.conf_version() < a.conf_version()) {
    return -1;
  }

  // version equal && conf_version equal
  return 0;
}

static void FillRpcContext(pb::store::Context& context, const int64_t region_id, const pb::common::RegionEpoch& epoch) {
  context.set_region_id(region_id);
  *context.mutable_region_epoch() = epoch;
}

static void FillRpcContext(pb::store::Context& context, const int64_t region_id, const pb::common::RegionEpoch& epoch,
                           const pb::store::IsolationLevel isolation) {
  FillRpcContext(context, region_id, epoch);

  context.set_isolation_level(isolation);
}

static void FillRpcContext(pb::store::Context& context, const int64_t region_id, const pb::common::RegionEpoch& epoch,
                           const std::vector<uint64_t>& resolved_locks, const pb::store::IsolationLevel isolation) {
  FillRpcContext(context, region_id, epoch, isolation);

  for (auto resolved_lock : resolved_locks) {
    if (resolved_lock != 0) {
      context.add_resolved_locks(resolved_lock);
    }
  }
}

static EndPoint LocationToEndPoint(const pb::common::Location& location) {
  CHECK(!location.host().empty());

  EndPoint endpoint(location.host(), location.port());

  return endpoint;
}

static pb::common::Location EndPointToLocation(const EndPoint& endpoint) {
  pb::common::Location location;
  location.set_host(endpoint.Host());
  location.set_port(endpoint.Port());

  return location;
}

static const pb::error::Error& GetRpcResponseError(Rpc& rpc) {
  const auto* response = rpc.RawResponse();
  const auto* descriptor = response->GetDescriptor();
  const auto* reflection = response->GetReflection();

  const auto* error_field = descriptor->FindFieldByName("error");
  CHECK(error_field) << "no error field";

  auto* msg = reflection->MutableMessage(rpc.RawMutableResponse(), error_field);
  CHECK(msg) << "get error mutable message fail";

  auto* error = google::protobuf::DynamicCastToGenerated<pb::error::Error>(msg);
  CHECK(error) << "dynamic cast msg to error fail";
  return *error;
}

static bool IsRetryErrorCode(int32_t error_code) {
  return error_code == pb::error::EREGION_VERSION || error_code == pb::error::EREGION_NOT_FOUND ||
         error_code == pb::error::EKEY_OUT_OF_RANGE || error_code == pb::error::EVECTOR_INDEX_NOT_READY ||
         error_code == pb::error::ERAFT_NOT_FOUND || error_code == pb::error::EREGION_STANDBY ||
         error_code == pb::error::EREGION_NEW;
}

static void TraceRpcPerformance(int64_t elapse_time, const std::string& method_name, const std::string& endpoint,
                                const std::string& str) {
  if (FLAGS_enable_trace_rpc_performance) {
    if (elapse_time > FLAGS_rpc_elapse_time_threshold_us) {
      DINGO_LOG(INFO) << fmt::format("[sdk.trace.rpc][{}][{:.6f}][endpoint({})] {}", method_name, elapse_time / 1e6,
                                     endpoint, str);
    }
  }
}

static bool IsUniversalNeedRetryError(Status status) {
  return status.IsNetworkError() || status.IsRemoteError() || status.IsNotLeader() || status.IsNoLeader();
}

static bool IsTxnNeedRetryError(Status status) { return status.IsTxnMemLockConflict(); }

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_COMMON_H_