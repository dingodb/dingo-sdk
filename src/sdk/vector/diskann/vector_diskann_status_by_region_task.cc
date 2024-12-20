#include "sdk/vector/diskann/vector_diskann_status_by_region_task.h"

#include <cstdint>
#include <set>
#include <utility>

#include "common/logging.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/common/common.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {

namespace sdk {

// StatusByRegion
Status VectorStatusByRegionTask ::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  if (vector_index_->GetVectorIndexType() != VectorIndexType::kDiskAnn) {
    return Status::InvalidArgument("vector_index is not diskann");
  }

  if (region_id_.empty()) {
    return Status::InvalidArgument("region_ids is empty");
  }

  std::set<int64_t> tmp_region_ids;

  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);

    for (const auto& id : region_id_) {
      if (!region_ids_.insert(id).second) {
        return Status::InvalidArgument("duplicate vector id: " + std::to_string(id));
      }
    }

    tmp_region_ids = region_ids_;
  }

  for (auto region_id : tmp_region_ids) {
    std::shared_ptr<Region> region;
    Status s = stub.GetMetaCache()->LookupRegionByRegionId(region_id, region);
    if (!s.ok()) {
      DINGO_LOG(WARNING) << "region_id : " << region_id << " is not found \n"
                         << "status : " << s.ToString();
      return s;
    }

    if (!vector_index_->ExistRegion(region)) {
      DINGO_LOG(WARNING) << "region_id : " << region_id << "dose not exist in "
                         << " index_id :" << vector_index_->GetId();
      return Status::InvalidArgument("region is not DiskANN Index Region");
    }
  }

  return Status::OK();
}

void VectorStatusByRegionTask::DoAsync() {
  std::set<int64_t> region_ids;
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    status_ = Status::OK();
    region_ids = region_ids_;
  }

  CHECK(!region_ids.empty()) << "region_ids is empty";

  controllers_.clear();
  rpcs_.clear();

  std::unordered_map<int64_t, std::shared_ptr<Region>> regions;

  for (auto region_id : region_ids) {
    std::shared_ptr<Region> region;
    Status s = stub.GetMetaCache()->LookupRegionByRegionId(region_id, region);
    if (!s.ok()) {
      DINGO_LOG(WARNING) << "region_id : " << region_id << " is not found \n"
                         << "status : " << s.ToString();
      DoAsyncDone(s);
    }

    if (!vector_index_->ExistRegion(region)) {
      DINGO_LOG(WARNING) << "region_id : " << region_id << "dose not exist in "
                         << " index_id :" << vector_index_->GetId();
      DoAsyncDone(Status::InvalidArgument("region is not DiskANN Index Region"));
    } else {
      regions[region->RegionId()] = region;
    }
  }

  for (auto& it : regions) {
    auto region = it.second;
    auto rpc = std::make_unique<VectorStatusRpc>();

    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());

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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorStatusByRegionRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorStatusByRegionTask::VectorStatusByRegionRpcCallback(const Status& status, VectorStatusRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      status_ = status;
    }

  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    region_ids_.erase(rpc->Request()->context().region_id());

    RegionState region_states;
    region_states.region_id = rpc->Request()->context().region_id();
    region_states.state = DiskANNStatePB2DiskANNState(rpc->Response()->state().diskann().diskann_state());
    result_.region_states.push_back(region_states);
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