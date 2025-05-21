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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/client_stub.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_buffer.h"

namespace dingodb {
namespace sdk {

using pb::store::TxnBatchRollbackResponse;
using pb::store::TxnCommitResponse;
using pb::store::TxnPrewriteResponse;

// TODO: support read only txn
class TxnImpl {
 public:
  TxnImpl(const TxnImpl&) = delete;
  const TxnImpl& operator=(const TxnImpl&) = delete;

  explicit TxnImpl(const ClientStub& stub, const TransactionOptions& options);

  ~TxnImpl() = default;

  enum State : uint8_t {
    kInit,
    kActive,
    kRollbacking,
    kRollbackted,
    kPreCommitting,
    kPreCommitted,
    kCommitting,
    kCommitted
  };

  static const char* StateName(State state) {
    switch (state) {
      case kInit:
        return "INIT";
      case kActive:
        return "ACTIVE";
      case kRollbacking:
        return "ROLLBACKING";
      case kRollbackted:
        return "ROLLBACKTED";
      case kPreCommitting:
        return "PRECOMMITTING";
      case kPreCommitted:
        return "PRECOMMITTED";
      case kCommitting:
        return "COMMITTING";
      case kCommitted:
        return "COMMITTED";
      default:
        CHECK(false) << "unknow transaction state";
    }
  }

  int64_t ID() const { return start_ts_; }

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

  Status PreCommit();

  Status Commit();

  Status Rollback();

  bool IsOnePc() const { return is_one_pc_; }

  State TEST_GetTransactionState() { return state_; }                    // NOLINT
  int64_t TEST_GetStartTs() { return start_ts_; }                        // NOLINT
  int64_t TEST_GetCommitTs() { return commit_ts_; }                      // NOLINT
  int64_t TEST_MutationsSize() { return buffer_->MutationsSize(); }      // NOLINT
  std::string TEST_GetPrimaryKey() { return buffer_->GetPrimaryKey(); }  // NOLINT

 private:
  struct ScanState {
    std::string next_key;
    std::shared_ptr<RegionScanner> scanner;
    std::vector<TxnMutation> local_mutations;
    std::vector<KVPair> pending_kvs;
    uint32_t pending_offset{0};
  };

  struct TxnSubTask {
    std::vector<std::string_view> keys;
    std::vector<const TxnMutation*> mutations;
    RegionPtr region;
    Status status;
    std::vector<KVPair> result_kvs;

    TxnSubTask(const std::vector<std::string_view>& keys, RegionPtr p_region)
        : keys(keys), region(std::move(p_region)) {}
    TxnSubTask(const std::vector<const TxnMutation*>& mutations, RegionPtr p_region)
        : mutations(mutations), region(std::move(p_region)) {}
  };

  static bool IsNeedRetry(int& times);
  Status LookupRegion(const std::string_view& key, RegionPtr& region);
  Status LookupRegion(std::string_view start_key, std::string_view end_key, std::shared_ptr<Region>& region);
  bool IsSingleRegionTxn(TxnBuffer& buffer);

  // txn get
  Status DoTxnGet(const std::string& key, std::string& value);

  // txn batch get
  void DoSubTaskForBatchGet(TxnSubTask* sub_task);
  Status DoTxnBatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs);

  // txn scan
  static Status ProcessScanState(ScanState& scan_state, uint64_t limit, std::vector<KVPair>& out_kvs);
  Status DoScan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& out_kvs);

  // txn prewrite
  std::unique_ptr<TxnPrewriteRpc> GenPrewriteRpc(const RegionPtr& region) const;
  void CheckPreCommitPrimaryKeyResponse(const TxnPrewriteResponse* response) const;
  Status TryResolveTxnPrewriteLockConflict(const TxnPrewriteResponse* response) const;
  Status PreCommitPrimaryKey();
  void DoSubTaskForPrewrite(TxnSubTask* sub_task);
  Status PreCommityOrdinaryKey();
  Status DoPreCommit();

  // txn commit
  std::unique_ptr<TxnCommitRpc> GenCommitRpc(const RegionPtr& region) const;
  Status ProcessTxnCommitResponse(const TxnCommitResponse* response, bool is_primary) const;
  Status CommitPrimaryKey();
  void DoSubTaskForCommit(TxnSubTask* sub_task);
  Status CommitOrdinaryKey();
  Status DoCommit();

  // txn rollback
  std::unique_ptr<TxnBatchRollbackRpc> GenBatchRollbackRpc(const RegionPtr& region) const;
  void CheckTxnBatchRollbackResponse(const TxnBatchRollbackResponse* response) const;
  void DoSubTaskForRollback(TxnSubTask* sub_task);
  Status RollbackPrimaryKey();
  Status RollbackOrdinaryKey();
  Status DoRollback();

  Status HeartBeat();

  const ClientStub& stub_;
  const TransactionOptions options_;
  State state_;
  std::unique_ptr<TxnBuffer> buffer_;

  int64_t start_ts_{0};
  int64_t commit_ts_{0};

  // for stream scan
  // start_key+end_key -> ScanState
  std::map<std::string, ScanState> scan_states_;

  bool is_one_pc_{false};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_IMPL_H_