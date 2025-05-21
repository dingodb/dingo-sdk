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

#include "sdk/document/document_add_task.h"

#include <cstdint>
#include <unordered_map>

#include "dingosdk/document.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/common/common.h"
#include "sdk/document/document_helper.h"
#include "sdk/document/document_index.h"
#include "sdk/document/document_translater.h"

namespace dingodb {
namespace sdk {

Status DocumentAddTask::Init() {
  if (docs_.empty()) {
    return Status::InvalidArgument("vectors is empty, no need add vector");
  }

  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetDocumentIndexCache()->GetDocumentIndexById(index_id_, tmp));
  doc_index_ = std::move(tmp);

  if (doc_index_->HasAutoIncrement()) {
    auto incrementer = stub.GetAutoIncrementerManager()->GetOrCreateDocumentIndexIncrementer(doc_index_);
    std::vector<int64_t> ids;
    int64_t id_count = docs_.size();
    ids.reserve(id_count);

    DINGO_RETURN_NOT_OK(incrementer->GetNextIds(ids, id_count));
    CHECK_EQ(ids.size(), id_count);

    for (auto i = 0; i < id_count; i++) {
      docs_[i].id = ids[i];
    }
  } else {
    for (auto& doc : docs_) {
      int64_t id = doc.id;
      if (id <= 0) {
        return Status::InvalidArgument("doc id must be positive");
      }
    }
  }

  WriteLockGuard guard(rw_lock_);
  doc_id_to_idx_.clear();

  for (int64_t i = 0; i < docs_.size(); i++) {
    int64_t id = docs_[i].id;
    if (!doc_id_to_idx_.insert(std::make_pair(id, i)).second) {
      return Status::InvalidArgument("duplicate vector id: " + std::to_string(id));
    }
  }

  return Status::OK();
}

void DocumentAddTask::DoAsync() {
  std::unordered_map<int64_t, int64_t> next_batch;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = doc_id_to_idx_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<int64_t>> region_docs_to_ids;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& [id, idx] : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(document_helper::DocumentIdToRangeKey(*doc_index_, id), tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    };

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_docs_to_ids[tmp->RegionId()].push_back(id);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_docs_to_ids) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<DocumentAddRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());

    for (const auto& id : entry.second) {
      int64_t idx = doc_id_to_idx_[id];
      DocumentTranslater::FillDocumentWithIdPB(rpc->MutableRequest()->add_documents(), docs_[idx]);
    }

    rpc->MutableRequest()->set_is_update(true);

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), region_docs_to_ids.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_docs_to_ids.size());

  for (auto i = 0; i < region_docs_to_ids.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { DocumentAddRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void DocumentAddTask::DocumentAddRpcCallback(const Status& status, DocumentAddRpc* rpc) {
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
    for (const auto& doc : rpc->Request()->documents()) {
      doc_id_to_idx_.erase(doc.id());
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