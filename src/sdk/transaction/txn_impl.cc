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

#include "sdk/transaction/txn_impl.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/store.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/region.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/transaction/txn_task/txn_batch_get_task.h"
#include "sdk/transaction/txn_task/txn_batch_rollback_task.h"
#include "sdk/transaction/txn_task/txn_commit_task.h"
#include "sdk/transaction/txn_task/txn_get_task.h"
#include "sdk/transaction/txn_task/txn_heartbeat_task.h"
#include "sdk/transaction/txn_task/txn_prewrite_task.h"

namespace dingodb {
namespace sdk {

TxnImpl::TxnImpl(const ClientStub& stub, const TransactionOptions& options, std::weak_ptr<TxnManager> txn_manager)
    : stub_(stub), options_(options), txn_manager_(txn_manager), state_(kInit), buffer_(new TxnBuffer()) {}

TxnImplSPtr TxnImpl::GetSelfPtr() { return std::dynamic_pointer_cast<TxnImpl>(shared_from_this()); }

Status TxnImpl::Begin() {
  int64_t start_ts;
  Status status = stub_.GetTsoProvider()->GenTs(2, start_ts);
  if (status.ok()) {
    state_.store(kActive);
    start_ts_.store(start_ts);
  }

  return status;
}

Status TxnImpl::Get(const std::string& key, std::string& value) {
  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  TxnMutation mutation;
  Status ret = buffer_->Get(key, mutation);
  if (ret.ok()) {
    switch (mutation.type) {
      case kPut:
        value = mutation.value;
        return Status::OK();
      case kDelete:
        return Status::NotFound("");
      case kPutIfAbsent:
        // NOTE: directy return is ok?
        value = mutation.value;
        return Status::OK();
      default:
        CHECK(false) << "unknow mutation type, mutation: " << mutation.ToString();
    }
  }

  return DoTxnGet(key, value);
}

Status TxnImpl::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  for (const auto& key : keys) {
    if (key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  std::vector<std::string> not_found_keys;
  std::vector<KVPair> result_kvs;
  result_kvs.reserve(keys.size());
  Status status;
  for (const auto& key : keys) {
    TxnMutation mutation;
    status = buffer_->Get(key, mutation);
    if (status.IsOK()) {
      switch (mutation.type) {
        case kPut:
          result_kvs.push_back({key, mutation.value});
          continue;
        case kDelete:
          continue;
        case kPutIfAbsent:
          // NOTE: use this value is ok?
          result_kvs.push_back({key, mutation.value});
          continue;
        default:
          CHECK(false) << "unknow mutation type, mutation:" << mutation.ToString();
      }
    } else {
      CHECK(status.IsNotFound());
      not_found_keys.push_back(key);
    }
  }

  if (!not_found_keys.empty()) {
    std::vector<KVPair> remote_kvs;
    status = DoTxnBatchGet(not_found_keys, remote_kvs);
    result_kvs.insert(result_kvs.end(), std::make_move_iterator(remote_kvs.begin()),
                      std::make_move_iterator(remote_kvs.end()));
  }

  kvs = std::move(result_kvs);

  return status;
}

Status TxnImpl::Put(const std::string& key, const std::string& value) {
  CheckStateActive();

  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  return buffer_->Put(key, value);
}

Status TxnImpl::BatchPut(const std::vector<KVPair>& kvs) {
  CheckStateActive();

  for (const auto& kv : kvs) {
    if (kv.key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  return buffer_->BatchPut(kvs);
}

Status TxnImpl::PutIfAbsent(const std::string& key, const std::string& value) {
  CheckStateActive();

  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  return buffer_->PutIfAbsent(key, value);
}

Status TxnImpl::BatchPutIfAbsent(const std::vector<KVPair>& kvs) {
  CheckStateActive();

  for (const auto& kv : kvs) {
    if (kv.key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  return buffer_->BatchPutIfAbsent(kvs);
}

Status TxnImpl::Delete(const std::string& key) {
  CheckStateActive();

  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  return buffer_->Delete(key);
}

Status TxnImpl::BatchDelete(const std::vector<std::string>& keys) {
  CheckStateActive();

  for (const auto& key : keys) {
    if (key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  return buffer_->BatchDelete(keys);
}

Status TxnImpl::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                     std::vector<KVPair>& out_kvs) {
  CheckStateActive();

  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key");
  }

  return DoScan(start_key, end_key, limit, out_kvs);
}

Status TxnImpl::PreCommit() { return DoPreCommit(); }

Status TxnImpl::Commit() { return DoCommit(); }

Status TxnImpl::Rollback() { return DoRollback(); }

bool TxnImpl::IsNeedRetry(int& times) {
  bool retry = times++ < FLAGS_txn_op_max_retry;
  if (retry) {
    (void)usleep(FLAGS_txn_op_delay_ms * 1000);
  }

  return retry;
}

bool TxnImpl::IsNeedRetry(const Status& status) { return status.IsIncomplete() && (IsRetryErrorCode(status.Errno())); }

Status TxnImpl::LookupRegion(const std::string_view& key, RegionPtr& region) {
  return stub_.GetMetaCache()->LookupRegionByKey(key, region);
}

Status TxnImpl::LookupRegion(std::string_view start_key, std::string_view end_key, std::shared_ptr<Region>& region) {
  return stub_.GetMetaCache()->LookupRegionBetweenRange(start_key, end_key, region);
}

Status TxnImpl::DoTxnGet(const std::string& key, std::string& value) {
  TxnGetTask task(stub_, key, value, shared_from_this());
  return task.Run();
}

// TODO: return not found keys
Status TxnImpl::DoTxnBatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  TxnBatchGetTask task(stub_, keys, kvs, shared_from_this());
  return task.Run();
}

Status TxnImpl::ProcessScanState(ScanState& scan_state, uint64_t limit, std::vector<KVPair>& out_kvs) {
  int mutations_offset = 0;
  auto& local_mutations = scan_state.local_mutations;
  while (scan_state.pending_offset < scan_state.pending_kvs.size()) {
    auto& kv = scan_state.pending_kvs[scan_state.pending_offset];
    ++scan_state.pending_offset;

    if (mutations_offset >= local_mutations.size()) {
      out_kvs.push_back(std::move(kv));
      if (out_kvs.size() == limit) {
        return Status::OK();
      }
      continue;
    }

    const auto& mutation = local_mutations[mutations_offset];
    if (kv.key == mutation.key) {
      if (mutation.type == TxnMutationType::kDelete) {
        continue;

      } else if (mutation.type == TxnMutationType::kPut) {
        out_kvs.push_back({std::move(kv.key), mutation.value});

      } else {
        CHECK(false) << "unknow mutation type, mutation:" << mutation.ToString();
      }

      ++mutations_offset;

    } else if (kv.key < mutation.key) {
      out_kvs.push_back(std::move(kv));

    } else {
      do {
        if (mutation.type == TxnMutationType::kPutIfAbsent) {
          out_kvs.push_back({std::move(kv.key), mutation.value});
        }

        ++mutations_offset;

        if (out_kvs.size() == limit) {
          return Status::OK();
        }

      } while (mutations_offset < local_mutations.size() && kv.key > local_mutations[mutations_offset].key);
    }

    if (out_kvs.size() == limit) {
      return Status::OK();
    }
  }

  return Status::OK();
}

Status TxnImpl::DoScan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                       std::vector<KVPair>& out_kvs) {
  // check whether region exist
  RegionPtr region;
  Status status = LookupRegion(start_key, end_key, region);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("[sdk.txn.{}] scan lookup region fail, [{},{}) status({}).", ID(),
                                    StringToHex(start_key), StringToHex(end_key), status.ToString());
    return status;
  }

  // get or create scan state
  std::string state_key = start_key + end_key;
  auto it = scan_states_.find(state_key);
  if (it == scan_states_.end()) {
    ScanState scan_state = {.next_key = start_key};
    CHECK(buffer_->Range(start_key, end_key, scan_state.local_mutations).ok());

    scan_states_.emplace(std::make_pair(state_key, std::move(scan_state)));
    it = scan_states_.find(state_key);
  }
  auto& scan_state = it->second;

  if (scan_state.pending_offset < scan_state.pending_kvs.size()) {
    ProcessScanState(scan_state, limit, out_kvs);
    if (!out_kvs.empty()) {
      scan_state.next_key = out_kvs.back().key;
    }
    if (out_kvs.size() == limit) {
      return Status::OK();
    }
  }

  // loop scan
  while (scan_state.next_key < end_key) {
    DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] scan range [{},{}).", ID(), StringToHex(scan_state.next_key),
                                    StringToHex(end_key));

    auto scanner = scan_state.scanner;
    if (scanner == nullptr) {
      // get region
      RegionPtr region;
      Status status = LookupRegion(scan_state.next_key, end_key, region);
      if (!status.IsOK()) {
        DINGO_LOG(ERROR) << fmt::format("[sdk.txn.{}] scan lookup region fail, [{},{}) status({}).", ID(),
                                        StringToHex(start_key), StringToHex(end_key), status.ToString());

        if (status.IsNotFound()) {
          scan_state.next_key = end_key;
          continue;
        }
        return status;
      }

      std::string amend_start_key =
          scan_state.next_key <= region->GetRange().start_key ? region->GetRange().start_key : scan_state.next_key;
      std::string amend_end_key = end_key <= region->GetRange().end_key ? end_key : region->GetRange().end_key;
      CHECK(amend_start_key < amend_end_key)
          << "amend_start_key should less than amend_end_key, " << StringToHex(amend_start_key)
          << " >= " << StringToHex(amend_end_key) << " start_key:" << StringToHex(start_key)
          << " end_key:" << StringToHex(end_key) << " region start_key:" << StringToHex(region->GetRange().start_key)
          << " region end_key:" << StringToHex(region->GetRange().end_key);

      DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] scan region({}) range[{}, {}).", ID(), region->RegionId(),
                                      StringToHex(amend_start_key), StringToHex(amend_end_key));

      ScannerOptions scan_options(stub_, region, amend_start_key, amend_end_key, options_, start_ts_.load());
      CHECK(stub_.GetTxnRegionScannerFactory()->NewRegionScanner(scan_options, scanner).IsOK());
      CHECK(scanner->Open().ok());

      scan_state.scanner = scanner;
    }

    bool is_retry = false;
    while (scanner->HasMore()) {
      std::vector<KVPair> scan_kvs;
      status = scanner->NextBatch(scan_kvs);
      if (!status.IsOK()) {
        DINGO_LOG(ERROR) << fmt::format("[sdk.txn.{}] scan next batch fail, region({}) status({}).", ID(),
                                        region->RegionId(), status.ToString());
        if (IsNeedRetry(status)) {
          is_retry = true;
          scanner->Close();
          scan_state.scanner = nullptr;
          break;
        }
        return status;
      }
      if (scan_kvs.empty()) {
        CHECK(!scanner->HasMore()) << "scan_kvs is empty, so scanner should not has more.";
        break;
      }

      CHECK(scan_state.pending_offset == scan_state.pending_kvs.size()) << "pending_kvs is not empty.";

      scan_state.pending_kvs = std::move(scan_kvs);
      scan_state.pending_offset = 0;

      ProcessScanState(scan_state, limit, out_kvs);
      if (!out_kvs.empty()) {
        scan_state.next_key = out_kvs.back().key;
      }
      if (out_kvs.size() == limit) {
        return Status::OK();
      }
    }

    if (is_retry) continue;

    auto region = scanner->GetRegion();
    CHECK(region != nullptr) << "region should not nullptr.";
    scan_state.next_key = region->GetRange().end_key;
    scanner->Close();
    scan_state.scanner = nullptr;
  }

  scan_states_.erase(state_key);

  return Status::OK();
}

void TxnImpl::CheckPreCommitResponse(const TxnPrewriteResponse* response) const {
  std::string pk = buffer_->GetPrimaryKey();
  auto txn_result_size = response->txn_result_size();
  if (0 == txn_result_size) {
    DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] precommit pk({}) success.", ID(), StringToHex(pk));

  } else if (1 == txn_result_size) {
    const auto& txn_result = response->txn_result(0);
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] precommit pk({}) lock or confict, txn_result({}).", ID(),
                                   StringToHex(pk), txn_result.ShortDebugString());

  } else {
    DINGO_LOG(FATAL) << fmt::format("[sdk.txn.{}] precommit unexpected response, size({}) response().", ID(),
                                    txn_result_size, response->ShortDebugString());
  }
}

Status TxnImpl::TryResolveTxnPreCommitConflict(const TxnPrewriteResponse* response) const {
  Status status;
  const std::string& pk = buffer_->GetPrimaryKey();
  for (const auto& txn_result : response->txn_result()) {
    status = CheckTxnResultInfo(txn_result);

    if (status.ok()) {
      continue;

    } else if (status.IsTxnLockConflict()) {
      Status local_status = stub_.GetTxnLockResolver()->ResolveLock(txn_result.locked(), start_ts_.load());
      if (!local_status.ok()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit resolve lock fail, pk() status({}) txn_result({}).",
                                          ID(), StringToHex(pk), local_status.ToString(),
                                          txn_result.ShortDebugString());
        status = local_status;
      }

    } else if (status.IsTxnWriteConflict()) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit write conflict, pk({}) status({}) txn_result({}).",
                                        ID(), StringToHex(pk), status.ToString(), txn_result.ShortDebugString());
      return status;

    } else {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit unexpect response, status({}) response({}).", ID(),
                                        status.ToString(), response->ShortDebugString());
    }
  }

  return status;
}

void TxnImpl::ScheduleHeartBeat() {
  stub_.GetActuator()->Schedule(
      [shared_this = shared_from_this(), start_ts = start_ts_.load(), primary_key = buffer_->GetPrimaryKey()] {
        shared_this->DoHeartBeat(start_ts, primary_key);
      },
      FLAGS_txn_heartbeat_interval_ms);
}

void TxnImpl::DoHeartBeat(int64_t start_ts, std::string primary_key) {
  State state = state_.load();
  if (state != kPreCommitted && state != kPreCommitting && state != kCommitting) {
    return;
  }
  std::shared_ptr<TxnHeartbeatTask> heartbeat_task = std::make_shared<TxnHeartbeatTask>(stub_, start_ts, primary_key);
  auto status = heartbeat_task->Run();
  if (status.ok()) {
    ScheduleHeartBeat();
  } else {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] heartbeat stoped , because last run status({}).", ID(),
                                      status.ToString());
  }
}

// TODO: process AlreadyExist if mutaion is PutIfAbsent
Status TxnImpl::DoPreCommit() {
  State state = state_.load();
  if (state == kPreCommitted) {
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] already precommitted.", ID());
    return Status::OK();
  } else if (state != kActive) {
    return Status::IllegalState("state is not active, state:" + std::string(StateName(state)));
  }

  if (buffer_->IsEmpty()) {
    state_.store(kFinshed);
    is_one_pc_ = true;
    Cleanup();
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] precommit success, no mutation.", ID());
    return Status::OK();
  }

  state_.store(kPreCommitting);

  CHECK(buffer_->Mutations().find(buffer_->GetPrimaryKey()) != buffer_->Mutations().end())
      << "primary key must in mutations, primary key:" << buffer_->GetPrimaryKey();

  // check whether 1pc
  std::set<int64_t> region_ids;
  auto meta_cache = stub_.GetMetaCache();
  for (const auto& [key, mutation] : buffer_->Mutations()) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(mutation.key, tmp);
    if (!s.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[sdk.txn.{}] precommit lookup region fail, key({}) status({}).", ID(),
                                      StringToHex(mutation.key), s.ToString());
      return s;
    }
    region_ids.insert(tmp->RegionId());
  }

  is_one_pc_ = (region_ids.size() == 1) && (buffer_->Mutations().size() <= FLAGS_txn_max_batch_count);

  if (is_one_pc_) {
    // 1pc
    std::map<std::string, const TxnMutation*> mutations_map;

    for (const auto& [key, mutation] : buffer_->Mutations()) {
      mutations_map.emplace(std::make_pair(key, &mutation));
    }

    TxnPrewriteTask task(stub_, buffer_->GetPrimaryKey(), mutations_map, shared_from_this(), is_one_pc_);

    Status status = task.Run();

    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] 1pc precommit key fail, status({}).", ID(), status.ToString());
      return status;
    }
  } else {
    // 2pc
    // precommit primary key
    DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] precommit primary key, pk({}).", ID(),
                                    StringToHex(buffer_->GetPrimaryKey()));
    std::map<std::string, const TxnMutation*> mutations_map_primary_key;
    mutations_map_primary_key.emplace(
        std::make_pair(buffer_->GetPrimaryKey(), &buffer_->Mutations().at(buffer_->GetPrimaryKey())));
    TxnPrewriteTask task_primary(stub_, buffer_->GetPrimaryKey(), mutations_map_primary_key, shared_from_this(),
                                 is_one_pc_);

    Status status = task_primary.Run();

    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] 2pc precommit primary key fail, status({}).", ID(),
                                        status.ToString());
      return status;
    }

    // 2pc need schedule heartbeat to update lock ttl
    ScheduleHeartBeat();

    // precommit ordinary keys
    DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] precommit ordinary keys.", ID());
    std::map<std::string, const TxnMutation*> mutations_map_ordinary_keys;
    for (const auto& [key, mutation] : buffer_->Mutations()) {
      if (key == buffer_->GetPrimaryKey()) {
        continue;
      }
      mutations_map_ordinary_keys.emplace(std::make_pair(key, &mutation));
    }
    TxnPrewriteTask task_ordinary(stub_, buffer_->GetPrimaryKey(), mutations_map_ordinary_keys, shared_from_this(),
                                  is_one_pc_);
    status = task_ordinary.Run();
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] 2pc precommit ordinary keys fail, status({}).", ID(),
                                        status.ToString());
      return status;
    }
  }

  if (is_one_pc_) {
    state_.store(kFinshed);
    Cleanup();
  } else {
    state_.store(kPreCommitted);
  }

  return Status::OK();
}

Status TxnImpl::ProcessTxnCommitResponse(const TxnCommitResponse* response, bool is_primary) {
  std::string pk = buffer_->GetPrimaryKey();
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] commit response, pk({}) response({}).", ID(), pk,
                                  response->ShortDebugString());

  if (!response->has_txn_result()) {
    return Status::OK();
  }

  const auto& txn_result = response->txn_result();
  if (txn_result.has_locked()) {
    const auto& lock_info = txn_result.locked();
    DINGO_LOG(FATAL) << fmt::format("[sdk.txn.{}] commit lock conflict, is_primary({}) pk({}) response({}).", ID(),
                                    is_primary, StringToHex(pk), response->ShortDebugString());

  } else if (txn_result.has_txn_not_found()) {
    DINGO_LOG(FATAL) << fmt::format("[sdk.txn.{}] commit not found, is_primary({}) pk({}) response({}).", ID(),
                                    is_primary, StringToHex(pk), response->ShortDebugString());

  } else if (txn_result.has_write_conflict()) {
    if (!is_primary) {
      DINGO_LOG(FATAL) << fmt::format("[sdk.txn.{}] commit write conlict, pk({}) response({}).", ID(), StringToHex(pk),
                                      txn_result.write_conflict().ShortDebugString());
    }
    return Status::TxnWriteConflict("txn write conflict");

  } else if (txn_result.has_commit_ts_expired()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit ts expired, is_primary({}) pk({}) response({}).", ID(),
                                      is_primary, StringToHex(pk), txn_result.commit_ts_expired().ShortDebugString());
    if (is_primary) {
      int64_t new_commit_ts;
      auto status = stub_.GetTsoProvider()->GenTs(2, new_commit_ts);
      commit_ts_.store(new_commit_ts);
      if (!status.IsOK()) return status;
      return Status::TxnCommitTsExpired("txn commit ts expired");
    }
  }

  return Status::OK();
}

Status TxnImpl::CommitPrimaryKey() {
  std::vector<std::string> keys = {buffer_->GetPrimaryKey()};

  TxnCommitTask task(stub_, keys, shared_from_this(), true);
  return task.Run();
}

Status TxnImpl::CommitOrdinaryKey() {
  std::string pk = buffer_->GetPrimaryKey();
  std::vector<std::string> keys;
  for (const auto& [key, _] : buffer_->Mutations()) {
    if (key != pk) {
      keys.push_back(key);
    }
  }
  // async commit ordinary keys
  stub_.GetActuator()->Schedule(
      [shared_this = shared_from_this(), ordinary_keys = keys] { shared_this->DoCommitOrdinaryKey(ordinary_keys); }, 0);
  return Status::OK();
}

void TxnImpl::DoCommitOrdinaryKey(std::vector<std::string> keys) {
  CHECK(state_.load() == kCommitted) << "state is not committed, state:" << StateName(state_.load());
  std::shared_ptr<TxnCommitTask> txn_commit_task =
      std::make_shared<TxnCommitTask>(stub_, keys, shared_from_this(), false);

  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}]commit ordinary keys, size({}).", ID(), keys.size());

  auto status = txn_commit_task->Run();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit ordinary keys fail. status({}).", ID(), status.ToString());
  }
  state_.store(kFinshed);
  Cleanup();
}

Status TxnImpl::DoCommit() {
  State state = state_.load();
  if (state == kCommitted || state == kFinshed) {
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] already committed.", ID());
    state_.store(kFinshed);
    return Status::OK();
  } else if (state != kPreCommitted) {
    return Status::IllegalState(
        fmt::format("forbid commit, state {}, expect {}", StateName(state), StateName(kPreCommitted)));
  }

  if (buffer_->IsEmpty()) {
    state_.store(kFinshed);
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] buffer is empty, commit success.", ID());
    return Status::OK();
  }

  state_.store(kCommitting);

  int64_t commit_ts = commit_ts_.load();
  if (commit_ts == 0) {
    // only init once, if commit_ts_ not set, get a new one
    DINGO_RETURN_NOT_OK(stub_.GetTsoProvider()->GenTs(2, commit_ts));
    commit_ts_.store(commit_ts);
  }

  int64_t start_ts = start_ts_.load();
  CHECK(commit_ts > start_ts) << fmt::format("commit_ts({}) must greater than start_ts({}).", commit_ts, start_ts);

  // commit primary key
  // TODO: if commit primary key and find txn is rolled back, should we rollback all the mutation?
  Status status = CommitPrimaryKey();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit primary key fail, status({}).", ID(), status.ToString());
    // commit primary key fail, maybe network error, so state is uncertain, set to precommitted
    state_.store(kPreCommitted);
    return status;
  }

  state_.store(kCommitted);

  // commit ordinary keys
  status = CommitOrdinaryKey();
  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit ordinary keys fail, status({}).", ID(), status.ToString());
  }

  return Status::OK();
}

Status TxnImpl::RollbackPrimaryKey() {
  std::vector<std::string> keys;
  std::string pk = buffer_->GetPrimaryKey();
  keys.push_back(pk);
  if (is_one_pc_) {
    for (const auto& [key, _] : buffer_->Mutations()) {
      if (key != pk) {
        keys.push_back(key);
      }
    }
  }
  TxnBatchRollbackTask task(stub_, keys, shared_from_this());
  return task.Run();
}

Status TxnImpl::RollbackOrdinaryKey() {
  std::string pk = buffer_->GetPrimaryKey();

  std::vector<std::string> keys;
  for (const auto& [key, mutaion] : buffer_->Mutations()) {
    if (key == pk) {
      continue;
    }
    keys.push_back(key);
  }
  TxnBatchRollbackTask task(stub_, keys, shared_from_this());
  return task.Run();
}

Status TxnImpl::DoRollback() {
  // TODO: client txn status maybe inconsistence with server
  // so we should check txn status first and then take action
  // TODO: maybe support rollback when txn is active
  State state = state_.load();
  if (state != kPreCommitting && state != kPreCommitted) {
    return Status::IllegalState(fmt::format("forbid rollback, state {}", StateName(state)));
  }

  state_.store(kRollbacking);

  // rollback primary key
  auto status = RollbackPrimaryKey();

  if (!status.IsOK()) {
    state_.store(kRollbackfailed);
    Cleanup();
    return status;
  }

  if (is_one_pc_) {
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] 1pc rollback success.", ID());
    state_.store(kFinshed);
    Cleanup();
    return Status::OK();
  }

  state_.store(kRollbacked);

  // rollback ordinary keys
  status = RollbackOrdinaryKey();
  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rollback ordinary keys fail, status({}).", ID(), status.ToString());
  }
  state_.store(kFinshed);
  Cleanup();

  return Status::OK();
}

void TxnImpl::CheckStateActive() const {
  State state = state_.load();
  CHECK(state == kActive) << "state is not active, state:" << StateName(state);
}

void TxnImpl::Cleanup() {
  if (txn_manager_.lock()) {
    txn_manager_.lock()->UnregisterTxn(ID());
  } else {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] txn manager is nullptr.", ID());
  }
}

}  // namespace sdk
}  // namespace dingodb