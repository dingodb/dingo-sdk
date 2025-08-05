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

#include "sdk/document/document_scan_query_task.h"

#include "dingosdk/status.h"
#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/document/document_translater.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

Status DocumentScanQueryTask::Init() {
  if (scan_query_param_.max_scan_count < 0) {
    return Status::InvalidArgument("max_scan_count must be greater than or equal to 0");
  }

  if (scan_query_param_.doc_id_start < 0) {
    return Status::InvalidArgument("doc_id_start must be greater than or equal to 0");
  }

  if (scan_query_param_.doc_id_end < 0) {
    return Status::InvalidArgument("doc_id_end must be greater than or equal to 0");
  }

  if (scan_query_param_.is_reverse) {
    if (!(scan_query_param_.doc_id_end < scan_query_param_.doc_id_start)) {
      return Status::InvalidArgument("doc_id_end must be less than doc_id_start in reverse scan");
    }
  } else {
    if (scan_query_param_.doc_id_end != 0 && !(scan_query_param_.doc_id_start < scan_query_param_.doc_id_end)) {
      return Status::InvalidArgument("doc_id_end must be greater than doc_id_start in forward scan");
    }
  }

  if (scan_query_param_.max_scan_count <= 0) {
    return Status::InvalidArgument("max_scan_count must bigger than 0");
  }

  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetDocumentIndexCache()->GetDocumentIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  doc_index_ = std::move(tmp);

  WriteLockGuard guard(rw_lock_);
  auto part_ids = doc_index_->GetPartitionIds();

  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  return Status::OK();
}

void DocumentScanQueryTask::DoAsync() {
  std::set<int64_t> next_part_ids;
  {
    WriteLockGuard guard(rw_lock_);
    next_part_ids = next_part_ids_;
    status_ = Status::OK();
  }

  if (next_part_ids.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  sub_tasks_count_.store(next_part_ids.size());

  for (const auto& part_id : next_part_ids) {
    auto* sub_task = new DocumentScanQueryPartTask(stub, doc_index_, part_id, scan_query_param_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void DocumentScanQueryTask::SubTaskCallback(Status status, DocumentScanQueryPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    WriteLockGuard guard(rw_lock_);
    std::vector<DocWithId> vectors = sub_task->GetResult();
    for (auto& result : vectors) {
      CHECK(vector_ids_.find(result.id) == vector_ids_.end()) << "scan query find duplicate vector id: " << result.id;
      result_docs_.push_back(std::move(result));
    }

    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      ConstructResultUnlocked();
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

void DocumentScanQueryTask::ConstructResultUnlocked() {
  if (scan_query_param_.is_reverse) {
    std::sort(result_docs_.begin(), result_docs_.end(),
              [](const DocWithId& a, const DocWithId& b) { return a.id > b.id; });
  } else {
    std::sort(result_docs_.begin(), result_docs_.end(),
              [](const DocWithId& a, const DocWithId& b) { return a.id < b.id; });
  }

  if (result_docs_.size() > scan_query_param_.max_scan_count) {
    result_docs_.resize(scan_query_param_.max_scan_count);
  }

  out_result_.docs = std::move(result_docs_);
}

void DocumentScanQueryPartTask::DoAsync() {
  const auto& range = doc_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    WriteLockGuard guard(rw_lock_);
    result_docs_.clear();
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<DocumentScanQueryRpc>();
    FillDocumentScanQueryRpcRequest(rpc->MutableRequest(), region);

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), regions.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(regions.size());

  for (auto i = 0; i < regions.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { DocumentScanQueryRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void DocumentScanQueryPartTask::FillDocumentScanQueryRpcRequest(pb::document::DocumentScanQueryRequest* request,
                                                                const std::shared_ptr<Region>& region) {
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->GetEpoch());

  request->set_document_id_start(scan_query_param_.doc_id_start);
  request->set_is_reverse_scan(scan_query_param_.is_reverse);
  request->set_max_scan_count(scan_query_param_.max_scan_count);
  request->set_document_id_end(scan_query_param_.doc_id_end);
  request->set_without_scalar_data(!scan_query_param_.with_scalar_data);
  if (scan_query_param_.with_scalar_data) {
    for (const auto& key : scan_query_param_.selected_keys) {
      request->add_selected_keys(key);
    }
  }
}

void DocumentScanQueryPartTask::DocumentScanQueryRpcCallback(Status status, DocumentScanQueryRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    {
      WriteLockGuard guard(rw_lock_);
      for (const auto& doc_with_id : rpc->Response()->documents()) {
        result_docs_.emplace_back(DocumentTranslater::InternalDocumentWithIdPB2DocWithId(doc_with_id));
      }
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

}  // namespace sdk
}  // namespace dingodb