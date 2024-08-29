
#include "sdk/vector/diskann/vector_diskann_import_task.h"

#include <cstdint>
#include <unordered_map>

#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/common/common.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/status.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_helper.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

Status VectorImportAddTask::Init() {
  if (vectors_.empty()) {
    return Status::InvalidArgument("vectors is empty, no need add vector");
  }

  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  if (vector_index_->HasAutoIncrement()) {
    auto incrementer = stub.GetAutoIncrementerManager()->GetOrCreateVectorIndexIncrementer(vector_index_);
    std::vector<int64_t> ids;
    int64_t id_count = vectors_.size();
    ids.reserve(id_count);

    DINGO_RETURN_NOT_OK(incrementer->GetNextIds(ids, id_count));
    CHECK_EQ(ids.size(), id_count);

    for (auto i = 0; i < id_count; i++) {
      vectors_[i].id = ids[i];
    }
  } else {
    for (auto& vector : vectors_) {
      int64_t id = vector.id;
      if (id <= 0) {
        return Status::InvalidArgument("vector id must be positive");
      }
    }
  }

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  vector_id_to_idx_.clear();

  for (int64_t i = 0; i < vectors_.size(); i++) {
    int64_t id = vectors_[i].id;
    if (!vector_id_to_idx_.insert(std::make_pair(id, i)).second) {
      return Status::InvalidArgument("duplicate vector id: " + std::to_string(id));
    }
  }

  return Status::OK();
}

void VectorImportAddTask::DoAsync() {
  std::unordered_map<int64_t, int64_t> next_batch;
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    next_batch = vector_id_to_idx_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<int64_t>> region_vectors_to_ids;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& [id, idx] : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(vector_helper::VectorIdToRangeKey(*vector_index_, id), tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    };

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_vectors_to_ids[tmp->RegionId()].push_back(id);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_vectors_to_ids) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<VectorImportRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());

    for (const auto& id : entry.second) {
      int64_t idx = vector_id_to_idx_[id];
      FillVectorWithIdPB(rpc->MutableRequest()->add_vectors(), vectors_[idx]);
    }
    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), region_vectors_to_ids.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_vectors_to_ids.size());

  for (auto i = 0; i < region_vectors_to_ids.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorImportAddRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorImportAddTask::VectorImportAddRpcCallback(const Status& status, VectorImportRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    DINGO_LOG(INFO) << "import region id : " << rpc->Request()->context().region_id();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    for (const auto& vector : rpc->Request()->vectors()) {
      vector_id_to_idx_.erase(vector.id());
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

// VectorImportDelete
Status VectorImportDeleteTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  next_vector_ids_.clear();

  for (const auto& id : vector_ids_) {
    if (!next_vector_ids_.insert(id).second) {
      return Status::InvalidArgument("duplicate vector id: " + std::to_string(id));
    }
  }

  return Status::OK();
}

void VectorImportDeleteTask::DoAsync() {
  std::set<int64_t> next_batch;
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    next_batch = next_vector_ids_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<int64_t>> region_vectors_to_ids;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& id : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(vector_helper::VectorIdToRangeKey(*vector_index_, id), tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    };

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_vectors_to_ids[tmp->RegionId()].push_back(id);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_vectors_to_ids) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<VectorImportRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());

    for (const auto& id : entry.second) {
      rpc->MutableRequest()->add_delete_ids(id);
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), region_vectors_to_ids.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_vectors_to_ids.size());

  for (auto i = 0; i < region_vectors_to_ids.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorImportDeleteRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorImportDeleteTask::VectorImportDeleteRpcCallback(const Status& status, VectorImportRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    for (auto id : rpc->Request()->delete_ids()) {
      out_result_.push_back({id, false});
    }
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    DINGO_LOG(INFO) << "delete  region id : " << rpc->Request()->context().region_id();
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

}  // namespace sdk
}  // namespace dingodb