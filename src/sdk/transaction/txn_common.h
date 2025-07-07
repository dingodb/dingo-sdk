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

#ifndef DINGODB_SDK_TRANSACTION_COMMON_H_
#define DINGODB_SDK_TRANSACTION_COMMON_H_

#include "dingosdk/client.h"
#include "glog/logging.h"
#include "proto/store.pb.h"

namespace dingodb {
namespace sdk {

static pb::store::IsolationLevel ToIsolationLevel(TransactionIsolation isolation) {
  switch (isolation) {
    case kSnapshotIsolation:
      return pb::store::IsolationLevel::SnapshotIsolation;
    case kReadCommitted:
      return pb::store::IsolationLevel::ReadCommitted;
    default:
      CHECK(false) << "unknow isolation:" << isolation;
  }
}

static Status CheckTxnResultInfo(const pb::store::TxnResultInfo& txn_result_info) {
  if (txn_result_info.has_locked()) {
    return Status::TxnLockConflict(txn_result_info.locked().ShortDebugString());
  }

  if (txn_result_info.has_write_conflict()) {
    return Status::TxnWriteConflict(txn_result_info.write_conflict().ShortDebugString());
  }

  if (txn_result_info.has_txn_not_found()) {
    return Status::TxnNotFound(txn_result_info.txn_not_found().ShortDebugString());
  }

  if (txn_result_info.has_primary_mismatch()) {
    return Status::TxnPrimaryMismatch(txn_result_info.primary_mismatch().ShortDebugString());
  }

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_COMMON_H_