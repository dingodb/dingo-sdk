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

#include "sdk/document/document_get_index_metrics_task.h"

#include <cstdint>
#include <memory>

#include "glog/logging.h"
#include "sdk/common/common.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

Status DocumentGetIndexMetricsTask::Init() {
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

void DocumentGetIndexMetricsTask::DoAsync() {
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
    auto* sub_task = new DocumentGetIndexMetricsPartTask(stub, doc_index_, part_id);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void DocumentGetIndexMetricsTask::SubTaskCallback(Status status, DocumentGetIndexMetricsPartTask* sub_task) {
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
    DocIndexMetricsResult result = sub_task->GetResult();
    MergeDocIndexMetricsResult(result, tmp_result_);
    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
      if (tmp.ok()) {
        if (tmp_result_.min_doc_id == INT64_MAX) {
          tmp_result_.min_doc_id = 0;
        }
        out_result_ = tmp_result_;
      }
    }

    DoAsyncDone(tmp);
  }
}

void DocumentGetIndexMetricsPartTask::DoAsync() {
  const auto& range = doc_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    WriteLockGuard guard(rw_lock_);
    region_id_to_metrics_.clear();
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<DocumentGetRegionMetricsRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch());

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), regions.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(regions.size());

  for (auto i = 0; i < regions.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall([this, rpc = rpcs_[i].get()](auto&& s) {
      DocumentGetRegionMetricsRpcCallback(std::forward<decltype(s)>(s), rpc);
    });
  }
}

void DocumentGetIndexMetricsPartTask::DocumentGetRegionMetricsRpcCallback(const Status& status,
                                                                          DocumentGetRegionMetricsRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    WriteLockGuard guard(rw_lock_);
    CHECK(region_id_to_metrics_.emplace(rpc->Request()->context().region_id(), rpc->Response()->metrics()).second);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;

      for (const auto& [region_id, metrics] : region_id_to_metrics_) {
        total_metrics_.total_num_docs += metrics.total_num_docs();
        total_metrics_.total_num_tokens += metrics.total_num_tokens();
        total_metrics_.max_doc_id = std::max(total_metrics_.max_doc_id, metrics.max_id());
        if (total_metrics_.min_doc_id != 0) {
          total_metrics_.min_doc_id = std::min(total_metrics_.min_doc_id, metrics.min_id());
        }
        if (total_metrics_.meta_json.empty()) {
          total_metrics_.meta_json = metrics.meta_json();
        }
        if (total_metrics_.json_parameter.empty()) {
          total_metrics_.json_parameter = metrics.json_parameter();
        }
      }

      DoAsyncDone(tmp);
    }
  }
}

}  // namespace sdk
}  // namespace dingodb