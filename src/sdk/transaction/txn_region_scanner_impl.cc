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

#include "sdk/transaction/txn_region_scanner_impl.h"

#include <memory>

#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/region_scanner.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_common.h"

namespace dingodb {
namespace sdk {

TxnRegionScannerImpl::TxnRegionScannerImpl(const ClientStub& stub, std::shared_ptr<Region> region,
                                           const TransactionOptions& txn_options, int64_t txn_start_ts,
                                           std::string start_key, std::string end_key)
    : RegionScanner(stub, region),
      txn_options_(txn_options),
      txn_start_ts_(txn_start_ts),
      start_key_(std::move(start_key)),
      end_key_(std::move(end_key)),
      opened_(false),
      has_more_(false),
      batch_size_(FLAGS_scan_batch_size) {}

TxnRegionScannerImpl::~TxnRegionScannerImpl() { Close(); }

Status TxnRegionScannerImpl::Open() {
  CHECK(!opened_);
  has_more_ = true;
  opened_ = true;

  return Status::OK();
}

void TxnRegionScannerImpl::Close() {
  if (opened_) {
    opened_ = false;
  }
}

bool TxnRegionScannerImpl::HasMore() const { return has_more_; }

std::unique_ptr<TxnScanRpc> TxnRegionScannerImpl::PrepareTxnScanRpc() {
  auto rpc = std::make_unique<TxnScanRpc>();
  rpc->MutableRequest()->set_start_ts(txn_start_ts_);
  FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 TransactionIsolation2IsolationLevel(txn_options_.isolation));

  auto* range_with_option = rpc->MutableRequest()->mutable_range();
  auto* range = range_with_option->mutable_range();
  range->set_start_key(start_key_);
  range->set_end_key(end_key_);
  range_with_option->set_with_start(true);
  range_with_option->set_with_end(false);

  auto* stream_meta = rpc->MutableRequest()->mutable_stream_meta();
  stream_meta->set_stream_id(stream_id_);
  stream_meta->set_limit(batch_size_);

  return std::move(rpc);
}

Status TxnRegionScannerImpl::NextBatch(std::vector<KVPair>& kvs) {
  CHECK(opened_);

  std::unique_ptr<TxnScanRpc> rpc = PrepareTxnScanRpc();

  int retry = 0;
  Status ret;
  while (true) {
    DINGO_RETURN_NOT_OK(LogAndSendRpc(stub, *rpc, region));

    const auto* response = rpc->Response();
    if (response->has_txn_result()) {
      ret = CheckTxnResultInfo(response->txn_result());
    }

    if (ret.ok()) {
      break;
    } else if (ret.IsTxnLockConflict()) {
      ret = stub.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), txn_start_ts_);
      if (!ret.ok()) {
        break;
      }
    } else {
      DINGO_LOG(WARNING) << "unexpect txn scan rpc response, status:" << ret.ToString()
                         << " response:" << response->DebugString();
      break;
    }

    if (NeedRetryAndInc(retry)) {
      DINGO_LOG(INFO) << "try to delay:" << FLAGS_txn_op_delay_ms << "ms";
      DelayRetry(FLAGS_txn_op_delay_ms);
    } else {
      break;
    }
  }

  if (!ret.ok()) {
    DINGO_LOG(WARNING) << "Fail scan, txn start_tx:" << txn_start_ts_ << ", region:" << region->RegionId()
                       << ", status:" << ret.ToString();
    return ret;
  }

  const auto* response = rpc->Response();

  for (const auto& kv : response->kvs()) {
    DINGO_LOG(DEBUG) << "Success scan, key:" << kv.key() << ", value:" << kv.value() << ", end_key:" << end_key_;
    kvs.push_back({kv.key(), kv.value()});
  }
  has_more_ = response->stream_meta().has_more();
  stream_id_ = response->stream_meta().stream_id();

  return ret;
}

Status TxnRegionScannerImpl::SetBatchSize(int64_t size) {
  uint64_t to_size = size;
  if (size <= kMinScanBatchSize) {
    to_size = kMinScanBatchSize;
  }

  if (size > kMaxScanBatchSize) {
    to_size = kMaxScanBatchSize;
  }

  batch_size_ = to_size;
  return Status::OK();
}

bool TxnRegionScannerImpl::NeedRetryAndInc(int& times) {
  bool retry = times < FLAGS_txn_op_max_retry;
  times++;
  return retry;
}

void TxnRegionScannerImpl::DelayRetry(int64_t delay_ms) { (void)usleep(delay_ms * 1000); }

TxnRegionScannerFactoryImpl::TxnRegionScannerFactoryImpl() = default;

TxnRegionScannerFactoryImpl::~TxnRegionScannerFactoryImpl() = default;

Status TxnRegionScannerFactoryImpl::NewRegionScanner(const ScannerOptions& options,
                                                     std::shared_ptr<RegionScanner>& scanner) {
  if (!options.txn_options) {
    return Status::InvalidArgument("txn options not set");
  }

  if (!options.start_ts) {
    return Status::InvalidArgument("txn start_ts not set");
  }

  CHECK(options.start_key < options.end_key);
  CHECK(options.start_key >= options.region->Range().start_key())
      << fmt::format("start_key:{} should greater than region range start_key:{}", options.start_key,
                     options.region->Range().start_key());
  CHECK(options.end_key <= options.region->Range().end_key()) << fmt::format(
      "end_key:{} should little than region range end_key:{}", options.end_key, options.region->Range().end_key());

  std::shared_ptr<RegionScanner> tmp(new TxnRegionScannerImpl(options.stub, options.region, options.txn_options.value(),
                                                              options.start_ts.value(), options.start_key,
                                                              options.end_key));
  scanner = std::move(tmp);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb