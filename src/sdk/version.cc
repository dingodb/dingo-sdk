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

#include "dingosdk/version.h"

#include <cstdint>
#include <utility>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/version.pb.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/version_rpc.h"

namespace dingodb {
namespace sdk {

static Version::KVWithExt ToKVWithExt(const pb::version::Kv& kv) {
  Version::KVWithExt out_kv;

  out_kv.kv.key = kv.kv().key();
  out_kv.kv.value = kv.kv().value();
  out_kv.create_revision = kv.create_revision();
  out_kv.mod_revision = kv.mod_revision();
  out_kv.version = kv.version();
  out_kv.lease = kv.lease();

  return std::move(out_kv);
}

Status Version::KvRange(const Options& options, const Range& range, int64_t limit, std::vector<KVWithExt>& out_kvs,
                        bool& out_more, int64_t& out_count) {
  version::KvRangeRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_key(range.start_key);
  request->set_range_end(range.end_key);
  request->set_limit(limit);
  request->set_keys_only(options.keys_only);
  request->set_count_only(options.count_only);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("range fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();
  out_more = response->more();
  out_count = response->count();

  for (const auto& kv : response->kvs()) {
    out_kvs.push_back(ToKVWithExt(kv));
  }

  return Status::OK();
}

Status Version::KvPut(const Options& options, const KVPair& kv, KVWithExt& out_prev_kv) {
  version::KvPutRpc rpc;
  auto* request = rpc.MutableRequest();
  request->mutable_key_value()->set_key(kv.key);
  request->mutable_key_value()->set_value(kv.value);

  request->set_need_prev_kv(options.need_prev_kv);
  request->set_ignore_value(options.ignore_value);
  request->set_ignore_lease(options.ignore_lease);
  request->set_lease(options.lease_id);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("put fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();
  out_prev_kv = ToKVWithExt(response->prev_kv());

  return Status::OK();
}

Status Version::KvDeleteRange(const Options& options, const Range& range, int64_t& out_deleted,
                              std::vector<KVWithExt>& out_prev_kvs) {
  version::KvDeleteRangeRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_key(range.start_key);
  request->set_range_end(range.end_key);
  request->set_need_prev_kv(options.need_prev_kv);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("delete range fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  out_deleted = response->deleted();
  for (const auto& kv : response->prev_kvs()) {
    out_prev_kvs.push_back(ToKVWithExt(kv));
  }

  return Status::OK();
}

Status Version::KvCompaction(const Range& range, int64_t revision, int64_t& out_count) {
  version::KvCompactionRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_key(range.start_key);
  request->set_range_end(range.end_key);
  request->set_compact_revision(revision);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("compaction fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  out_count = response->compaction_count();

  return Status::OK();
}

Status Version::LeaseGrant(int64_t id, int64_t ttl, int64_t& out_id, int64_t& out_ttl) {
  version::LeaseGrantRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_id(id);
  request->set_ttl(ttl);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("lease grant fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  out_id = response->id();
  out_ttl = response->ttl();

  return Status::OK();
}

Status Version::LeaseRevoke(int64_t id) {
  version::LeaseRevokeRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_id(id);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("lease revoke fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  return Status::OK();
}

Status Version::LeaseRenew(int64_t id, int64_t& out_ttl) {
  version::LeaseRenewRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_id(id);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("lease revoke fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  out_ttl = response->ttl();

  return Status::OK();
}

Status Version::LeaseQuery(int64_t id, bool is_get_key, int64_t& out_ttl, int64_t& out_granted_ttl,
                           std::vector<std::string>& out_keys) {
  version::LeaseQueryRpc rpc;
  auto* request = rpc.MutableRequest();

  request->set_id(id);
  request->set_keys(is_get_key);

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("lease query fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  out_ttl = response->ttl();
  out_granted_ttl = response->grantedttl();

  for (const auto& key : response->keys()) {
    out_keys.push_back(key);
  }

  return Status::OK();
}

Status Version::ListLeases(std::vector<int64_t>& out_ids) {
  version::ListLeasesRpc rpc;

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("lease list fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  for (const auto& lease : response->leases()) {
    out_ids.push_back(lease.id());
  }

  return Status::OK();
}

static pb::version::EventFilterType ToEventFilterType(Version::FilterType filter) {
  switch (filter) {
    case Version::FilterType::kNoput:
      return pb::version::EventFilterType::NOPUT;

    case Version::FilterType::kNodelete:
      return pb::version::EventFilterType::NODELETE;

    default:
      DINGO_LOG(FATAL) << "not support type.";
      return pb::version::EventFilterType::NOPUT;
  }
}

static Version::EventType ToEventType(pb::version::Event::EventType type) {
  switch (type) {
    case pb::version::Event_EventType_NONE:
      return Version::EventType::kNone;

    case pb::version::Event_EventType_PUT:
      return Version::EventType::kPut;

    case pb::version::Event_EventType_DELETE:
      return Version::EventType::kDelete;

    case pb::version::Event_EventType_NOT_EXISTS:
      return Version::EventType::kNotExists;

    default:
      DINGO_LOG(FATAL) << "not support type.";
      return Version::EventType::kNone;
  }
}

static Version::Event ToEvent(const pb::version::Event& event) {
  Version::Event out_event;

  out_event.type = ToEventType(event.type());
  out_event.kv = ToKVWithExt(event.kv());
  out_event.prev_kv = ToKVWithExt(event.prev_kv());

  return std::move(out_event);
}

Status Version::Watch(const WatchParams& param, WatchOut& out) {
  version::WatchRpc rpc;
  auto* request = rpc.MutableRequest();

  if (param.type == WatchType::kOneTime) {
    CHECK(!param.one_time_watch.key.empty()) << "key must not empty";

    auto* one_time_request = request->mutable_one_time_request();
    one_time_request->set_key(param.one_time_watch.key);
    one_time_request->set_start_revision(param.one_time_watch.start_revision);
    one_time_request->set_need_prev_kv(param.one_time_watch.need_prev_kv);
    one_time_request->set_wait_on_not_exist_key(param.one_time_watch.wait_on_not_exist_key);

    for (const auto& filter : param.one_time_watch.filter_types) {
      one_time_request->add_filters(ToEventFilterType(filter));
    }
  } else {
    DINGO_LOG(FATAL) << "type not support.";
  }

  Status status = stub_.GetVersionRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("watch fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  const auto& response = rpc.Response();

  for (const auto& event : response->events()) {
    out.events.push_back(ToEvent(event));
  }

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb