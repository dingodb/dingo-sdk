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

#ifndef DINGODB_SDK_TRANSACTION_IMPL_H_
#define DINGODB_SDK_TRANSACTION_IMPL_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/client_stub.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_manager.h"

namespace dingodb {
namespace sdk {

using pb::store::TxnBatchRollbackResponse;
using pb::store::TxnCommitResponse;
using pb::store::TxnPrewriteResponse;

class TxnImpl;
using TxnImplSPtr = std::shared_ptr<TxnImpl>;

// TODO: support read only txn
class TxnImpl : public std::enable_shared_from_this<TxnImpl> {
 public:
  TxnImpl(const TxnImpl&) = delete;
  const TxnImpl& operator=(const TxnImpl&) = delete;

  explicit TxnImpl(const ClientStub& stub, const TransactionOptions& options, TxnManager* txn_manager);

  ~TxnImpl() = default;

  TxnImplSPtr GetSelfPtr();

  enum State : uint8_t {
    kInit,
    kActive,
    kAborted,
    kRollbacking,
    kRollbacked,
    kRollbackfailed,
    kPreCommitting,
    kPreCommitted,
    kCommitting,
    kCommitted,
    kFinshed,
  };

  static const char* StateName(State state) {
    switch (state) {
      case kInit:
        return "INIT";
      case kActive:
        return "ACTIVE";
      case kAborted:
        return "ABORTED";
      case kRollbacking:
        return "ROLLBACKING";
      case kRollbacked:
        return "ROLLBACKED";
      case kRollbackfailed:
        return "ROLLBACKFAILED";
      case kPreCommitting:
        return "PRECOMMITTING";
      case kPreCommitted:
        return "PRECOMMITTED";
      case kCommitting:
        return "COMMITTING";
      case kCommitted:
        return "COMMITTED";
      case kFinshed:
        return "FINISHED";
      default:
        CHECK(false) << "unknow transaction state";
    }
  }

  int64_t ID() const { return start_ts_.load(); }

  Status Begin();

  Status Get(const std::string& key, std::string& value);

  Status BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  Status Put(const std::string& key, const std::string& value);

  Status BatchPut(const std::vector<KVPair>& kvs);

  Status PutIfAbsent(const std::string& key, const std::string& value);

  Status BatchPutIfAbsent(const std::vector<KVPair>& kvs);

  Status Delete(const std::string& key);

  Status BatchDelete(const std::vector<std::string>& keys);

  // maybe multiple invoke, when out_kvs.size < limit is over.
  Status Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& out_kvs);

  Status PreWriteAndCommit();

  Status Rollback();

  bool IsOnePc() const { return is_one_pc_.load(); }

  bool IsAsyncCommit() const { return !is_one_pc_.load() && use_async_commit_.load(); }

  int64_t GetStartTs() const { return start_ts_.load(); }
  int64_t GetCommitTs() const { return commit_ts_.load(); }
  std::string GetPrimaryKey() const { return buffer_->GetPrimaryKey(); }
  TransactionOptions GetOptions() const { return options_; }

  bool CheckFinished() const {
    State state = state_.load();
    return state == kFinshed || state == kRollbackfailed || state == kAborted;
  }

  bool CheckFrontTaskCompleted() const {
    State state = state_.load();
    return state == kFinshed || state == kRollbackfailed || state == kAborted || state == kRollbacked ||
           state == kCommitted;
  }

  void Clean() {
    State state = state_.load();
    if (state == kActive) {
      DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}]clean active txn", ID());
      state_.store(kAborted);
      Cleanup();
    }
  }

  std::string DebugString() const { return fmt::format("Txn: id={}, state={}", ID(), StateName(state_.load())); }

  bool TEST_IsInitState() { return state_.load() == kInit; }                      // NOLINT
  bool TEST_IsActiveState() { return state_.load() == kActive; }                  // NOLINT
  bool TEST_IsRollbackingState() { return state_.load() == kRollbacking; }        // NOLINT
  bool TEST_IsRollbacktedState() { return state_.load() == kRollbacked; }         // NOLINT
  bool TEST_IsRollbackFailedState() { return state_.load() == kRollbackfailed; }  // NOLINT
  bool TEST_IsPreCommittingState() { return state_.load() == kPreCommitting; }    // NOLINT
  bool TEST_IsPreCommittedState() { return state_.load() == kPreCommitted; }      // NOLINT
  bool TEST_IsCommittingState() { return state_.load() == kCommitting; }          // NOLINT
  bool TEST_IsCommittedState() { return state_.load() == kCommitted; }            // NOLINT
  bool TEST_IsFinishedState() { return state_.load() == kFinshed; }               // NOLINT
  bool TEST_IsAbortedState() { return state_.load() == kAborted; }                // NOLINT
  bool TEST_IsOnePc() { return is_one_pc_.load(); }                               // NOLINT
  int64_t TEST_GetStartTs() { return start_ts_.load(); }                          // NOLINT
  int64_t TEST_GetCommitTs() { return commit_ts_.load(); }                        // NOLINT
  int64_t TEST_MutationsSize() { return buffer_->MutationsSize(); }               // NOLINT
  std::string TEST_GetPrimaryKey() { return buffer_->GetPrimaryKey(); }           // NOLINT
  void TEST_SetStateFinished() { state_.store(kFinshed); }                        // NOLINT
  Status TEST_PreCommit() { return DoPreCommit(); }                               // NOLINT
  Status TEST_Commit() { return DoCommit(); }                                     // NOLINT

 private:
  struct ScanState {
    std::string next_key;
    std::shared_ptr<RegionScanner> scanner;
    std::vector<TxnMutation> local_mutations;
    std::vector<KVPair> pending_kvs;
    uint32_t pending_offset{0};
  };

  static bool IsNeedRetry(int& times);
  static bool IsNeedRetry(const Status& status);
  Status LookupRegion(const std::string_view& key, RegionPtr& region);
  Status LookupRegion(std::string_view start_key, std::string_view end_key, std::shared_ptr<Region>& region);

  // txn get
  Status DoTxnGet(const std::string& key, std::string& value);

  // txn batch get
  Status DoTxnBatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  // txn scan
  static Status ProcessScanState(ScanState& scan_state, uint64_t limit, std::vector<KVPair>& out_kvs);
  Status DoScan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& out_kvs);

  // txn precommit
  Status DoPreCommit();
  Status PreCommit1PC();
  Status PreCommit2PC();

  // txn commit
  Status CommitPrimaryKey();
  Status CommitOrdinaryKey();
  Status AsyncCommitKeys();
  void DoCommitKeys(std::vector<std::string> keys);
  Status DoCommit();

  // txn rollback
  Status RollbackPrimaryKey();
  Status RollbackOrdinaryKey();
  void DoRollbackOrdinaryKey(std::vector<std::string> keys);
  Status DoRollback();

  void DoHeartBeat(int64_t start_ts, std::string primary_key);
  void ScheduleHeartBeat();

  void CheckStateActive() const;

  void Cleanup();

  const ClientStub& stub_;
  const TransactionOptions options_;

  std::atomic<State> state_;

  std::atomic<int64_t> start_ts_{0};
  std::atomic<uint64_t> commit_ts_{0};

  std::atomic<bool> is_one_pc_{false};
  std::atomic<bool> use_async_commit_{false};

  TxnBufferUPtr buffer_;

  // for stream scan
  // start_key+end_key -> ScanState
  std::map<std::string, ScanState> scan_states_;

  TxnManager* txn_manager_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_IMPL_H_