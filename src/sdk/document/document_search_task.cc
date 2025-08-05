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

#include "sdk/document/document_search_task.h"

#include <cstdint>
#include <iterator>
#include <memory>

#include "common/logging.h"
#include "dingosdk/document.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/document.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/document/document_translater.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

Status DocumentSearchTask::Init() {
  WriteLockGuard guard(rw_lock_);

  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetDocumentIndexCache()->GetDocumentIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  doc_index_ = std::move(tmp);

  auto part_ids = doc_index_->GetPartitionIds();

  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  DocumentTranslater::FillInternalDocSearchParams(&search_parameter_, search_param_);

  return Status::OK();
}

void DocumentSearchTask::DoAsync() {
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
    auto* sub_task = new DocumentSearchPartTask(stub, index_id_, part_id, search_parameter_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void DocumentSearchTask::SubTaskCallback(Status status, DocumentSearchPartTask* sub_task) {
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
    std::vector<DocWithStore> sub_results = sub_task->GetDocSearchResult();
    std::move(sub_results.begin(), sub_results.end(), std::back_inserter(out_result_.doc_sores));
    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      WriteLockGuard guard(rw_lock_);

      std::sort(out_result_.doc_sores.begin(), out_result_.doc_sores.end(),
                [](const DocWithStore& a, const DocWithStore& b) { return a.score > b.score; });

      if (search_param_.top_n > 0 && search_param_.top_n < out_result_.doc_sores.size()) {
        out_result_.doc_sores.resize(search_param_.top_n);
      }

      tmp = status_;
    }

    DoAsyncDone(tmp);
  }
}

Status DocumentSearchPartTask::Init() {
  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetDocumentIndexCache()->GetDocumentIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  doc_index_ = std::move(tmp);

  return Status::OK();
}

void DocumentSearchPartTask::DoAsync() {
  const auto& range = doc_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    WriteLockGuard guard(rw_lock_);
    search_result_.clear();
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<DocumentSearchRpc>();
    FillDocumentSearchRpcRequest(rpc->MutableRequest(), region);

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
        [this, rpc = rpcs_[i].get()](auto&& s) { DocumentSearchRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void DocumentSearchPartTask::FillDocumentSearchRpcRequest(pb::document::DocumentSearchRequest* request,
                                                          const std::shared_ptr<Region>& region) {
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->GetEpoch());
  *(request->mutable_parameter()) = search_parameter_;
}

void DocumentSearchPartTask::DocumentSearchRpcCallback(const Status& status, DocumentSearchRpc* rpc) {
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
      for (const auto& doc_with_score : rpc->Response()->document_with_scores()) {
        DocWithStore distance = DocumentTranslater::InternalDocumentWithScore2DocWithStore(doc_with_score);
        search_result_.push_back(std::move(distance));
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