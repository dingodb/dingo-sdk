
#include "sdk/vector/diskann/vector_diskann_build_by_region_task.h"

#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "glog/logging.h"
#include "proto/error.pb.h"
#include "sdk/common/common.h"
#include "sdk/region.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

// BuildByRegion
Status VectorBuildByRegionTask ::Init() {
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

  WriteLockGuard guard(rw_lock_);

  for (const auto& id : region_id_) {
    if (!region_ids_.insert(id).second) {
      return Status::InvalidArgument("duplicate vector id: " + std::to_string(id));
    }
  }

  return Status::OK();
}

void VectorBuildByRegionTask::DoAsync() {
  std::set<int64_t> region_ids;
  {
    WriteLockGuard guard(rw_lock_);
    status_ = Status::OK();
    region_ids = region_ids_;
  }

  controllers_.clear();
  rpcs_.clear();

  std::unordered_map<int64_t, std::shared_ptr<Region>> regions;
  ErrStatusResult tmp_result;
  for (auto region_id : region_ids) {
    std::shared_ptr<Region> region;
    Status s = stub.GetMetaCache()->LookupRegionByRegionId(region_id, region);
    if (!s.ok()) {
      RegionStatus region_status;
      region_status.region_id = region_id;
      region_status.status = s;
      tmp_result.region_status.push_back(std::move(region_status));
      DINGO_LOG(WARNING) << "region_id : " << region_id << " is not found \n"
                         << "status : " << s.ToString();
      continue;
    }

    if (!vector_index_->ExistRegion(region)) {
      RegionStatus region_status;
      region_status.region_id = region_id;
      region_status.status = Status::InvalidArgument("region is not DiskANN Index Region");
      tmp_result.region_status.push_back(std::move(region_status));
      DINGO_LOG(WARNING) << "region_id : " << region_id << "dose not exist in "
                         << " index_id :" << vector_index_->GetId();
    } else {
      CHECK_EQ(region->RegionId(), region_id);
      regions[region->RegionId()] = region;
    }
  }

  {
    WriteLockGuard guard(rw_lock_);
    result_.region_status = tmp_result.region_status;
  }

  if (regions.empty()) {
    DoAsyncDone(Status::BuildFailed(""));
    return;
  }

  for (auto& it : regions) {
    auto region = it.second;
    auto rpc = std::make_unique<VectorBuildRpc>();

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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorBuildByRegionRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorBuildByRegionTask::VectorBuildByRegionRpcCallback(const Status& status, VectorBuildRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    WriteLockGuard guard(rw_lock_);
    if (pb::error::Errno::EDISKANN_IS_NO_DATA != status.Errno()) {
      RegionStatus region_status;
      region_status.region_id = rpc->Request()->context().region_id();
      region_status.status = status;
      result_.region_status.push_back(std::move(region_status));
    } else {
      region_ids_.erase(rpc->Request()->context().region_id());
    }
  } else {
    WriteLockGuard guard(rw_lock_);
    region_ids_.erase(rpc->Request()->context().region_id());
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    CHECK(status_.ok());
    if (!result_.region_status.empty()) {
      DoAsyncDone(Status::BuildFailed(""));
    } else {
      DoAsyncDone(Status::OK());
    }
  }
}

bool VectorBuildByRegionTask::NeedRetry() {
  for (auto& result : result_.region_status) {
    if (retry_count_ >= FLAGS_vector_op_max_retry) {
      return false;
    }
    auto status = result.status;
    if (status.IsIncomplete()) {
      auto error_code = status_.Errno();
      if (error_code == pb::error::EREGION_VERSION || error_code == pb::error::EREGION_NOT_FOUND ||
          error_code == pb::error::EKEY_OUT_OF_RANGE || error_code == pb::error::EVECTOR_INDEX_NOT_READY ||
          error_code == pb::error::ERAFT_NOT_FOUND) {
        retry_count_++;
        if (retry_count_ < FLAGS_vector_op_max_retry) {
          std::string msg = fmt::format("Task:{} will retry, reason:{}, retry_count_:{}, max_retry:{}", Name(),
                                        pb::error::Errno_Name(error_code), retry_count_, FLAGS_vector_op_max_retry);
          DINGO_LOG(INFO) << msg;
          return true;
        } else {
          std::string msg =
              fmt::format("Fail task:{} retry too times:{}, last err:{}", Name(), retry_count_, status_.ToString());
          status_ = Status::Aborted(status_.Errno(), msg);
          DINGO_LOG(INFO) << msg;
        }
      }
    }
  }

  return false;
}

}  // namespace sdk
}  // namespace dingodb