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
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {

Transaction::TxnImpl::TxnImpl(const ClientStub& stub, const TransactionOptions& options)
    : stub_(stub), options_(options), state_(kInit), buffer_(new TxnBuffer()) {}

Transaction::TxnImplSPtr Transaction::TxnImpl::GetSelfPtr() {
  return std::dynamic_pointer_cast<Transaction::TxnImpl>(shared_from_this());
}

Status Transaction::TxnImpl::Begin() {
  Status status = stub_.GetAdminTool()->GetCurrentTimeStamp(start_ts_, 2);
  if (status.ok()) {
    state_ = kActive;
  }

  return status;
}

Status Transaction::TxnImpl::Get(const std::string& key, std::string& value) {
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

Status Transaction::TxnImpl::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
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

Status Transaction::TxnImpl::Put(const std::string& key, const std::string& value) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  return buffer_->Put(key, value);
}

Status Transaction::TxnImpl::BatchPut(const std::vector<KVPair>& kvs) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  for (const auto& kv : kvs) {
    if (kv.key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  return buffer_->BatchPut(kvs);
}

Status Transaction::TxnImpl::PutIfAbsent(const std::string& key, const std::string& value) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  return buffer_->PutIfAbsent(key, value);
}

Status Transaction::TxnImpl::BatchPutIfAbsent(const std::vector<KVPair>& kvs) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  for (const auto& kv : kvs) {
    if (kv.key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  return buffer_->BatchPutIfAbsent(kvs);
}

Status Transaction::TxnImpl::Delete(const std::string& key) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  if (key.empty()) {
    return Status::InvalidArgument("param key is empty");
  }

  return buffer_->Delete(key);
}

Status Transaction::TxnImpl::BatchDelete(const std::vector<std::string>& keys) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  for (const auto& key : keys) {
    if (key.empty()) {
      return Status::InvalidArgument("param key is empty");
    }
  }

  return buffer_->BatchDelete(keys);
}

Status Transaction::TxnImpl::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                                  std::vector<KVPair>& out_kvs) {
  CHECK(state_ == kActive) << "state is not active, state:" << StateName(state_);

  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key");
  }

  return DoScan(start_key, end_key, limit, out_kvs);
}

Status Transaction::TxnImpl::PreCommit() { return DoPreCommit(); }

Status Transaction::TxnImpl::Commit() { return DoCommit(); }

Status Transaction::TxnImpl::Rollback() { return DoRollback(); }

bool Transaction::TxnImpl::IsNeedRetry(int& times) {
  bool retry = times++ < FLAGS_txn_op_max_retry;
  if (retry) {
    (void)usleep(FLAGS_txn_op_delay_ms * 1000);
  }

  return retry;
}

bool Transaction::TxnImpl::IsNeedRetry(const Status& status) {
  return status.IsIncomplete() &&
         (status.Errno() == pb::error::EREGION_VERSION || status.Errno() == pb::error::EKEY_OUT_OF_RANGE);
}

Status Transaction::TxnImpl::LookupRegion(const std::string_view& key, RegionPtr& region) {
  return stub_.GetMetaCache()->LookupRegionByKey(key, region);
}

Status Transaction::TxnImpl::LookupRegion(std::string_view start_key, std::string_view end_key,
                                          std::shared_ptr<Region>& region) {
  return stub_.GetMetaCache()->LookupRegionBetweenRange(start_key, end_key, region);
}

Status Transaction::TxnImpl::DoTxnGet(const std::string& key, std::string& value) {
  auto gen_rpc_func = [&](const RegionPtr& region, uint64_t resolved_lock) -> std::unique_ptr<TxnGetRpc> {
    auto rpc = std::make_unique<TxnGetRpc>();

    rpc->MutableRequest()->set_start_ts(start_ts_);
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(), {resolved_lock},
                   ToIsolationLevel(options_.isolation));

    return std::move(rpc);
  };

  uint64_t resolved_lock = 0;
  std::unique_ptr<TxnGetRpc> rpc;
  RegionPtr region;
  Status status;
  int retry = 0;
  do {
    status = LookupRegion(key, region);
    if (!status.IsOK()) {
      break;
    }

    rpc = gen_rpc_func(region, resolved_lock);
    rpc->MutableRequest()->set_key(key);

    status = LogAndSendRpc(stub_, *rpc, region);
    if (!status.IsOK()) {
      // retry
      if (IsNeedRetry(status)) {
        continue;
      }
      break;
    }

    const auto* response = rpc->Response();
    if (response->has_txn_result()) {
      const auto& txn_result = response->txn_result();
      status = CheckTxnResultInfo(txn_result);
      if (status.IsTxnLockConflict()) {
        status = stub_.GetTxnLockResolver()->ResolveLock(txn_result.locked(), start_ts_);
        // retry
        if (status.ok()) {
          continue;
        } else if (status.IsPushMinCommitTs()) {
          resolved_lock = txn_result.locked().lock_ts();
          continue;
        }
      }
    }

    break;

  } while (IsNeedRetry(retry));

  if (status.ok()) {
    const auto* response = rpc->Response();
    if (response->value().empty()) {
      status = Status::NotFound(fmt::format("not found key({})", key));
    } else {
      value = response->value();
    }

  } else {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] get fail, key({}) retry({}) status({}).", ID(), StringToHex(key),
                                      retry, status.ToString());
  }

  return status;
}

void Transaction::TxnImpl::DoTaskForBatchGet(TxnTask& task) {
  CHECK(!task.keys.empty()) << "task keys is empty.";

  auto gen_rpc_func = [&](const RegionPtr& region, uint64_t resolved_lock) -> std::unique_ptr<TxnBatchGetRpc> {
    auto rpc = std::make_unique<TxnBatchGetRpc>();

    rpc->MutableRequest()->set_start_ts(start_ts_);
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(), {resolved_lock},
                   ToIsolationLevel(options_.isolation));

    return std::move(rpc);
  };

  uint64_t resolved_lock = 0;
  std::unique_ptr<TxnBatchGetRpc> rpc;
  auto region = task.region;
  Status status;
  int retry = 0;
  do {
    rpc = gen_rpc_func(region, resolved_lock);
    for (const auto& key : task.keys) {
      *rpc->MutableRequest()->add_keys() = key;
    }

    status = LogAndSendRpc(stub_, *rpc, region);
    if (!status.IsOK()) {
      break;
    }

    const auto* response = rpc->Response();
    if (response->has_txn_result()) {
      const auto& txn_result = response->txn_result();
      status = CheckTxnResultInfo(txn_result);
      if (status.IsTxnLockConflict()) {
        status = stub_.GetTxnLockResolver()->ResolveLock(txn_result.locked(), start_ts_);
        // retry
        if (status.ok()) {
          continue;
        } else if (status.IsPushMinCommitTs()) {
          resolved_lock = txn_result.locked().lock_ts();
          continue;
        }
      }
    }

    break;

  } while (IsNeedRetry(retry));

  if (status.ok()) {
    for (const auto& kv : rpc->Response()->kvs()) {
      if (!kv.value().empty()) {
        task.result_kvs.push_back({kv.key(), kv.value()});
      }
    }

  } else {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] batchget fail, key({}) retry({}) status({}).", ID(),
                                      StringToHex(task.keys[0]), retry, status.ToString());
  }

  task.status = status;
}

// TODO: return not found keys
Status Transaction::TxnImpl::DoTxnBatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  struct RegionEntry {
    RegionPtr region;
    std::vector<std::string_view> keys;
  };

  std::set<std::string_view> done_keys;
  // region_id -> region item
  std::unordered_map<int64_t, RegionEntry> region_entry_map;
  auto gen_region_entry_func = [&]() -> Status {
    region_entry_map.clear();
    for (const auto& key : keys) {
      if (done_keys.count(key) > 0) {
        continue;
      }

      RegionPtr region;
      auto status = LookupRegion(key, region);
      if (!status.IsOK()) {
        return status;
      }

      auto it = region_entry_map.find(region->RegionId());
      if (it == region_entry_map.end()) {
        region_entry_map.emplace(std::make_pair(region->RegionId(), RegionEntry{region, {key}}));
      } else {
        it->second.keys.push_back(key);
      }
    }

    return Status::OK();
  };

  std::vector<KVPair> result_kvs;
  result_kvs.reserve(keys.size());
  Status status;
  int retry = 0;
  do {
    status = gen_region_entry_func();
    if (!status.IsOK()) {
      break;
    }

    std::vector<TxnTask> tasks;
    tasks.reserve(region_entry_map.size());
    for (const auto& [_, entry] : region_entry_map) {
      tasks.emplace_back(entry.keys, entry.region);
    }

    // parallel execute sub task
    ParallelExecutor::Execute(tasks.size(),
                              [&tasks, this](uint32_t i) { Transaction::TxnImpl::DoTaskForBatchGet(tasks[i]); });

    bool need_retry = false;
    for (auto& task : tasks) {
      if (task.status.IsOK()) {
        for (const auto& key : task.keys) done_keys.insert(key);
        result_kvs.insert(result_kvs.end(), std::make_move_iterator(task.result_kvs.begin()),
                          std::make_move_iterator(task.result_kvs.end()));
        continue;
      }

      if (status.ok()) status = task.status;

      if (IsNeedRetry(task.status)) {
        need_retry = true;
      } else {
        need_retry = false;
        status = task.status;
        break;
      }
    }

    if (need_retry) continue;

    break;

  } while (IsNeedRetry(retry));

  if (status.IsOK()) {
    kvs = std::move(result_kvs);

  } else {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] batchget fail, key({}) retry({}) status({}).", ID(),
                                      StringToHex(keys[0]), retry, status.ToString());
  }

  return status;
}

Status Transaction::TxnImpl::ProcessScanState(ScanState& scan_state, uint64_t limit, std::vector<KVPair>& out_kvs) {
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

Status Transaction::TxnImpl::DoScan(const std::string& start_key, const std::string& end_key, uint64_t limit,
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
          scan_state.next_key <= region->Range().start_key() ? region->Range().start_key() : scan_state.next_key;
      std::string amend_end_key = end_key <= region->Range().end_key() ? end_key : region->Range().end_key();

      DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] scan region({}) range[{}, {}).", ID(), region->RegionId(),
                                      StringToHex(amend_start_key), StringToHex(amend_end_key));

      ScannerOptions scan_options(stub_, region, amend_start_key, amend_end_key, options_, start_ts_);
      CHECK(stub_.GetTxnRegionScannerFactory()->NewRegionScanner(scan_options, scanner).IsOK());
      CHECK(scanner->Open().ok());

      scan_state.scanner = scanner;
    }

    bool is_retry = false;
    while (scanner->HasMore()) {
      std::vector<KVPair> scan_kvs;
      status = scanner->NextBatch(scan_kvs);
      if (!status.IsOK()) {
        DINGO_LOG(ERROR) << fmt::format("[sdk.txn.{}] sacn next batch fail, region({}) status({}).", ID(),
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
    scan_state.next_key = region->Range().end_key();
    scanner->Close();
    scan_state.scanner = nullptr;
  }

  scan_states_.erase(state_key);

  return Status::OK();
}

void Transaction::TxnImpl::CheckPreCommitResponse(const TxnPrewriteResponse* response) const {
  const std::string& pk = buffer_->GetPrimaryKey();
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

Status Transaction::TxnImpl::TryResolveTxnPreCommitConflict(const TxnPrewriteResponse* response) const {
  Status status;
  const std::string& pk = buffer_->GetPrimaryKey();
  for (const auto& txn_result : response->txn_result()) {
    status = CheckTxnResultInfo(txn_result);

    if (status.ok()) {
      continue;

    } else if (status.IsTxnLockConflict()) {
      Status local_status = stub_.GetTxnLockResolver()->ResolveLock(txn_result.locked(), start_ts_);
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

void Transaction::TxnImpl::DoTaskForPreCommit(TxnTask& task) {
  auto gen_rpc_func = [&](const RegionPtr& region) -> std::unique_ptr<TxnPrewriteRpc> {
    auto rpc = std::make_unique<TxnPrewriteRpc>();

    rpc->MutableRequest()->set_start_ts(start_ts_);
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                   ToIsolationLevel(options_.isolation));

    const std::string& pk = buffer_->GetPrimaryKey();
    rpc->MutableRequest()->set_primary_lock(pk);
    rpc->MutableRequest()->set_txn_size(buffer_->MutationsSize());

    // FIXME: set ttl
    rpc->MutableRequest()->set_lock_ttl(INT64_MAX);

    return std::move(rpc);
  };

  auto region = task.region;

  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] precommit key, region({}) mutations({}).", ID(), region->RegionId(),
                                  task.mutations.size());

  std::unique_ptr<TxnPrewriteRpc> rpc;

  Status status;
  int retry = 0;
  do {
    rpc = gen_rpc_func(region);
    if (is_one_pc_) rpc->MutableRequest()->set_try_one_pc(true);

    for (const auto& mutation : task.mutations) {
      TxnMutation2MutationPB(*mutation, rpc->MutableRequest()->add_mutations());
    }

    status = LogAndSendRpc(stub_, *rpc, region);
    if (!status.IsOK()) {
      break;
    }

    const auto* response = rpc->Response();
    CheckPreCommitResponse(response);

    status = TryResolveTxnPreCommitConflict(response);
    if (status.ok()) {
      break;
    } else if (status.IsTxnWriteConflict()) {
      // no need retry
      // TODO: should we change txn state?
      break;
    }

  } while (IsNeedRetry(retry));

  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit key fail, region({}) retry({}) status({}).", ID(),
                                      region->RegionId(), retry, status.ToString());
  }

  task.status = status;
}

// TODO: process AlreadyExist if mutaion is PutIfAbsent
Status Transaction::TxnImpl::DoPreCommit() {
  struct RegionEntry {
    RegionPtr region;
    std::vector<const TxnMutation*> mutations;
  };

  state_ = kPreCommitting;

  if (buffer_->IsEmpty()) {
    state_ = kPreCommitted;
    return Status::OK();
  }

  const std::string& pk = buffer_->GetPrimaryKey();

  // pre commit primary and ordinary key
  // group mutations by region
  std::set<const TxnMutation*> done_mutations;
  std::unordered_map<int64_t, RegionEntry> region_entry_map;
  auto gen_region_entry_func = [&]() -> Status {
    region_entry_map.clear();
    for (const auto& [key, mutation] : buffer_->Mutations()) {
      if (done_mutations.count(&mutation) > 0) {
        continue;
      }

      RegionPtr region;
      Status status = LookupRegion(key, region);
      if (!status.IsOK()) {
        return status;
      }

      auto iter = region_entry_map.find(region->RegionId());
      if (iter == region_entry_map.end()) {
        region_entry_map.emplace(std::make_pair(region->RegionId(), RegionEntry{region, {&mutation}}));
      } else {
        if (pk != key) {
          iter->second.mutations.push_back(&mutation);

        } else {
          // primary key should be first
          const auto* front = iter->second.mutations.front();
          iter->second.mutations[0] = &mutation;
          iter->second.mutations.push_back(front);
        }
      }
    }

    return Status::OK();
  };

  Status status;
  int retry = 0;
  bool already_set_one_pc = false;
  do {
    status = gen_region_entry_func();
    if (!status.IsOK()) {
      break;
    }

    CHECK(!region_entry_map.empty()) << "region_entry_map is empty.";

    std::vector<TxnTask> tasks;
    tasks.reserve(region_entry_map.size());
    for (const auto& [_, entry] : region_entry_map) {
      auto region = entry.region;

      std::vector<const TxnMutation*> mutations;
      for (const auto* mutation : entry.mutations) {
        mutations.push_back(mutation);

        if (mutations.size() == FLAGS_txn_max_batch_count) {
          tasks.emplace_back(mutations, region);
          mutations.clear();
        }
      }

      if (!mutations.empty()) {
        tasks.emplace_back(mutations, region);
      }
    }

    // set one pc flag
    if (!already_set_one_pc) {
      is_one_pc_ = (tasks.size() == 1);
      already_set_one_pc = true;
    }

    if (is_one_pc_) {
      DoTaskForPreCommit(tasks.front());

    } else {
      // parallel execute sub task
      ParallelExecutor::Execute(tasks.size(),
                                [&tasks, this](uint32_t i) { Transaction::TxnImpl::DoTaskForPreCommit(tasks[i]); });
    }

    bool need_retry = false;
    for (auto& task : tasks) {
      if (task.status.IsOK()) {
        for (const auto& mutation : task.mutations) {
          done_mutations.insert(mutation);
        }
        continue;
      }

      if (status.ok()) status = task.status;

      if (IsNeedRetry(task.status)) {
        need_retry = true;
      } else {
        need_retry = false;
        status = task.status;
        break;
      }
    }

    if (need_retry) continue;

    break;

  } while (IsNeedRetry(retry));

  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit key fail, retry({}) status({}).", ID(), retry,
                                      status.ToString());
    return status;
  }

  state_ = is_one_pc_ ? kCommitted : kPreCommitted;

  return Status::OK();
}

std::unique_ptr<TxnCommitRpc> Transaction::TxnImpl::GenCommitRpc(const RegionPtr& region) const {
  auto rpc = std::make_unique<TxnCommitRpc>();
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 ToIsolationLevel(options_.isolation));

  rpc->MutableRequest()->set_start_ts(start_ts_);
  rpc->MutableRequest()->set_commit_ts(commit_ts_);

  return std::move(rpc);
}

Status Transaction::TxnImpl::ProcessTxnCommitResponse(const TxnCommitResponse* response, bool is_primary) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] commit response, pk({}) response({}).", ID(),
                                  response->ShortDebugString());

  const std::string& pk = buffer_->GetPrimaryKey();

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
    return Status::TxnRolledBack("txn write conflict");

  } else if (txn_result.has_commit_ts_expired()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit ts expired, is_primary({}) pk({}) response({}).", ID(),
                                      is_primary, StringToHex(pk), txn_result.commit_ts_expired().ShortDebugString());
    if (is_primary) {
      auto status = stub_.GetAdminTool()->GetCurrentTimeStamp(commit_ts_);
      if (!status.IsOK()) return status;
      return Status::TxnCommitTsExpired("txn commit ts expired");
    }
  }

  return Status::OK();
}

Status Transaction::TxnImpl::CommitPrimaryKey() {
  const std::string& pk = buffer_->GetPrimaryKey();

  Status status;
  int retry = 0;
  do {
    RegionPtr region;
    status = LookupRegion(pk, region);
    if (!status.IsOK()) {
      break;
    }

    auto rpc = GenCommitRpc(region);
    *rpc->MutableRequest()->add_keys() = pk;

    status = LogAndSendRpc(stub_, *rpc, region);
    if (!status.IsOK()) {
      if (IsNeedRetry(status)) {
        continue;
      }
      break;
    }

    status = ProcessTxnCommitResponse(rpc->Response(), true);
    if (status.IsTxnCommitTsExpired()) {
      continue;  // retry with new commit_ts
    }

    break;

  } while (IsNeedRetry(retry));

  return status;
}

void Transaction::TxnImpl::DoTaskForCommit(AsyncTxnTaskSPtr task) {
  auto region = task->region;
  auto rpc = GenCommitRpc(region);
  for (const auto& key : task->keys) {
    rpc->MutableRequest()->add_keys(key);
  }

  auto status = LogAndSendRpc(stub_, *rpc, region);
  if (status.ok()) {
    status = ProcessTxnCommitResponse(rpc->Response(), false);
  }

  if (!status.IsOK()) {
    DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] commit oridnary key fail, region({}) status({}).", ID(),
                                   region->RegionId(), status.ToString());
  }
}

Status Transaction::TxnImpl::CommitOrdinaryKey() {
  struct RegionEntry {
    RegionPtr region;
    std::vector<std::string_view> keys;
  };

  const std::string& pk = buffer_->GetPrimaryKey();

  // group mutations by region
  std::unordered_map<int64_t, RegionEntry> region_entry_map;
  auto gen_region_entry_func = [&]() -> Status {
    for (const auto& [key, mutation] : buffer_->Mutations()) {
      if (key == pk) {
        continue;
      }

      RegionPtr region;
      Status status = LookupRegion(key, region);
      if (!status.IsOK()) {
        return status;
      }

      auto iter = region_entry_map.find(region->RegionId());
      if (iter == region_entry_map.end()) {
        region_entry_map.emplace(std::make_pair(region->RegionId(), RegionEntry{region, {mutation.key}}));
      } else {
        iter->second.keys.push_back(mutation.key);
      }
    }

    return Status::OK();
  };

  auto status = gen_region_entry_func();
  if (!status.IsOK()) {
    return status;
  }

  // generate rpcs task
  std::vector<AsyncTxnTaskSPtr> tasks;
  tasks.reserve(region_entry_map.size());
  for (const auto& [_, entry] : region_entry_map) {
    auto region = entry.region;

    std::vector<std::string_view> keys;
    for (const auto& key : entry.keys) {
      keys.push_back(key);

      if (keys.size() == FLAGS_txn_max_batch_count) {
        tasks.push_back(std::make_shared<AsyncTxnTask>(GetSelfPtr(), keys, region));
        keys.clear();
      }
    }

    if (!keys.empty()) {
      tasks.push_back(std::make_shared<AsyncTxnTask>(GetSelfPtr(), keys, region));
    }
  }

  // parallel execute sub task
  ParallelExecutor::AsyncExecute<AsyncTxnTaskSPtr>(tasks,
                                                   [](AsyncTxnTaskSPtr task) { task->txn->DoTaskForCommit(task); });

  return Status::OK();
}

Status Transaction::TxnImpl::DoCommit() {
  if (state_ == kCommitted) {
    return Status::OK();
  } else if (state_ != kPreCommitted) {
    return Status::IllegalState(
        fmt::format("forbid commit, state {}, expect {}", StateName(state_), StateName(kPreCommitted)));
  }

  if (buffer_->IsEmpty()) {
    state_ = kCommitted;
    return Status::OK();
  }

  state_ = kCommitting;

  DINGO_RETURN_NOT_OK(stub_.GetAdminTool()->GetCurrentTimeStamp(commit_ts_));

  CHECK(commit_ts_ > start_ts_) << fmt::format("commit_ts({}) must greater than start_ts({}).", commit_ts_, start_ts_);

  // commit primary key
  // TODO: if commit primary key and find txn is rolled back, should we rollback all the mutation?
  Status status = CommitPrimaryKey();
  if (!status.ok()) {
    if (status.IsTxnRolledBack()) {
      state_ = kRollbackted;
    } else {
      DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] commit primary key fail, status({}).", ID(), status.ToString());
    }

    return status;
  }

  state_ = kCommitted;

  // commit ordinary keys
  CommitOrdinaryKey();

  return Status::OK();
}

std::unique_ptr<TxnBatchRollbackRpc> Transaction::TxnImpl::GenBatchRollbackRpc(const RegionPtr& region) const {
  auto rpc = std::make_unique<TxnBatchRollbackRpc>();
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 ToIsolationLevel(options_.isolation));
  rpc->MutableRequest()->set_start_ts(start_ts_);
  return std::move(rpc);
}

void Transaction::TxnImpl::CheckTxnBatchRollbackResponse(const TxnBatchRollbackResponse* response) const {
  if (response->has_txn_result() && response->txn_result().has_locked()) {
    const std::string& pk = buffer_->GetPrimaryKey();
    const auto& txn_result = response->txn_result();
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rollback fail, pk({}) txn_result({}).", ID(), StringToHex(pk),
                                      txn_result.ShortDebugString());
  }
}

Status Transaction::TxnImpl::RollbackPrimaryKey() {
  const std::string& pk = buffer_->GetPrimaryKey();

  Status status;
  int retry = 0;
  do {
    RegionPtr region;
    status = LookupRegion(pk, region);
    if (!status.IsOK()) {
      break;
    }

    auto rpc = GenBatchRollbackRpc(region);
    *rpc->MutableRequest()->add_keys() = pk;
    if (is_one_pc_) {
      for (const auto& [key, _] : buffer_->Mutations()) {
        if (key != pk) *rpc->MutableRequest()->add_keys() = key;
      }
    }

    status = LogAndSendRpc(stub_, *rpc, region);
    if (!status.IsOK()) {
      if (IsNeedRetry(status)) {
        continue;
      }
      break;
    }

    const auto* response = rpc->Response();
    CheckTxnBatchRollbackResponse(response);
    if (response->has_txn_result()) {
      // TODO: which state should we transfer to ?
      const auto& txn_result = response->txn_result();
      if (txn_result.has_locked()) {
        return Status::TxnLockConflict(txn_result.locked().ShortDebugString());
      }
    }

    break;

  } while (IsNeedRetry(retry));

  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rollback primary key fail, pk({}) retry({}) status({}).", ID(),
                                      StringToHex(pk), retry, status.ToString());
  }

  return status;
}

void Transaction::TxnImpl::DoTaskForRollback(TxnTask& task) {
  auto rpc = GenBatchRollbackRpc(task.region);
  for (const auto& key : task.keys) {
    *rpc->MutableRequest()->add_keys() = key;
  }

  auto status = LogAndSendRpc(stub_, *rpc, task.region);
  if (!status.ok()) {
    task.status = status;
    return;
  }

  const auto* response = rpc->Response();
  CheckTxnBatchRollbackResponse(response);
  if (response->has_txn_result() && response->txn_result().has_locked()) {
    task.status = Status::TxnLockConflict("");
  }
}

Status Transaction::TxnImpl::RollbackOrdinaryKey() {
  struct RegionEntry {
    RegionPtr region;
    std::vector<std::string_view> keys;
  };

  const std::string& pk = buffer_->GetPrimaryKey();

  std::set<std::string_view> done_keys;
  // region id -> region entry
  std::unordered_map<int64_t, RegionEntry> region_entry_map;
  auto gen_region_entry_func = [&]() {
    region_entry_map.clear();
    for (const auto& [key, mutaion] : buffer_->Mutations()) {
      if (key == pk || done_keys.count(key) > 0) {
        continue;
      }

      RegionPtr region;
      Status status = LookupRegion(key, region);
      if (!status.IsOK()) {
        continue;
      }

      auto iter = region_entry_map.find(region->RegionId());
      if (iter == region_entry_map.end()) {
        region_entry_map.emplace(std::make_pair(region->RegionId(), RegionEntry{region, {key}}));
      } else {
        iter->second.keys.push_back(key);
      }
    }
  };

  Status status;
  int retry = 0;
  do {
    gen_region_entry_func();

    if (region_entry_map.empty()) break;

    std::vector<TxnTask> tasks;
    tasks.reserve(region_entry_map.size());
    for (const auto& [_, entry] : region_entry_map) {
      tasks.emplace_back(entry.keys, entry.region);
    }

    // parallel execute sub task
    ParallelExecutor::Execute(tasks.size(),
                              [&tasks, this](uint32_t i) { Transaction::TxnImpl::DoTaskForRollback(tasks[i]); });

    bool need_retry = false;
    for (auto& task : tasks) {
      if (task.status.IsOK()) {
        for (const auto& key : task.keys) done_keys.insert(key);
        continue;
      }

      DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] rollback ordinary key fail, region({}) status({}).", ID(),
                                     task.region->RegionId(), task.status.ToString());

      if (status.ok()) status = task.status;

      if (IsNeedRetry(task.status)) {
        need_retry = true;
      } else {
        need_retry = false;
        status = task.status;
        break;
      }
    }

    if (need_retry) continue;

    break;

  } while (IsNeedRetry(retry));

  return status;
}

Status Transaction::TxnImpl::DoRollback() {
  // TODO: client txn status maybe inconsistence with server
  // so we should check txn status first and then take action
  // TODO: maybe support rollback when txn is active
  if (state_ != kRollbacking && state_ != kPreCommitting && state_ != kPreCommitted) {
    return Status::IllegalState(fmt::format("forbid rollback, state {}", StateName(state_)));
  }

  state_ = kRollbacking;

  // rollback primary key
  auto status = RollbackPrimaryKey();
  if (!status.IsOK()) {
    return status;
  }

  state_ = kRollbackted;
  if (is_one_pc_) {
    return Status::OK();
  }

  // rollback ordinary keys
  RollbackOrdinaryKey();

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb