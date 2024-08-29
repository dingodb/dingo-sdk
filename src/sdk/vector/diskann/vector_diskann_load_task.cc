
#include "sdk/vector/diskann/vector_diskann_load_task.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/common/common.h"
#include "sdk/region.h"
#include "sdk/status.h"
#include "sdk/utils/scoped_cleanup.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

// LoadByIndex
Status VectorLoadByIndexTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  if (vector_index_->GetVectorIndexType() != VectorIndexType::kDiskAnn) {
    return Status::InvalidArgument("vector_index is not diskann");
  }

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  auto part_ids = vector_index_->GetPartitionIds();

  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  return Status::OK();
}

void VectorLoadByIndexTask::DoAsync() {
  std::set<int64_t> next_part_ids;
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    next_part_ids = next_part_ids_;
    status_ = Status::OK();
  }

  if (next_part_ids.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }
  paramer_.mutable_diskann()->set_warmup(true);
  paramer_.mutable_diskann()->set_num_nodes_to_cache(0);
  sub_tasks_count_.store(next_part_ids.size());
  for (const auto& part_id : next_part_ids) {
    auto* sub_task = new VectorLoadPartTask(stub, vector_index_, part_id, paramer_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorLoadByIndexTask::SubTaskCallback(const Status& status, VectorLoadPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(INFO) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();
    if (status.IsLoadFailed()) {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      ErrStatusResult result = sub_task->GetResult();
      for (auto& err_status : result.region_status) {
        result_.region_status.push_back(err_status);
      }
      if (status_.ok()) {
        status_ = status;
      }
    } else {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      status_ = status;
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

// LoadPartTask
void VectorLoadPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();
  result_.region_status.clear();

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorLoadRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
    *(rpc->MutableRequest()->mutable_parameter()) = param_;
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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorLoadRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorLoadPartTask::VectorLoadRpcCallback(const Status& status, VectorLoadRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    std::unique_lock<std::shared_mutex> w(rw_lock_);

    if (pb::error::Errno::EDISKANN_IS_NO_DATA == status.Errno()) {
      DINGO_LOG(INFO) << "ignore error : " << status.ToString()
                      << " region id : " << rpc->Request()->context().region_id();

    } else {
      RegionStatus region_status;
      region_status.region_id = rpc->Request()->context().region_id();
      region_status.status = status;
      result_.region_status.push_back(std::move(region_status));
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    if (status_.ok() && !result_.region_status.empty()) {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      status_ = Status::LoadFailed("");
    }
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

bool VectorLoadPartTask::NeedRetry() {
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

// LoadByRegion
Status VectorLoadByRegionTask ::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  if (vector_index_->GetVectorIndexType() != VectorIndexType::kDiskAnn) {
    return Status::InvalidArgument("vector_index is not diskann");
  }
  if (region_ids_.empty()) {
    return Status::InvalidArgument("region_ids is empty");
  }

  std::unique_lock<std::shared_mutex> w(rw_lock_);
  for (auto region_id : region_ids_) {
    std::shared_ptr<Region> region;
    Status s = stub.GetMetaCache()->LookupRegionByRegionId(region_id, region);
    if (!s.ok()) {
      RegionStatus region_status;
      region_status.region_id = region_id;
      region_status.status = s;
      result_.region_status.push_back(std::move(region_status));
      DINGO_LOG(WARNING) << "region_id : " << region_id << " is not found \n"
                         << "status : " << s.ToString();
      continue;
    }

    if (!vector_index_->ExistRegion(region)) {
      RegionStatus region_status;
      region_status.region_id = region_id;
      region_status.status = Status::InvalidArgument("region is not DiskANN Index Region");
      result_.region_status.push_back(std::move(region_status));

      DINGO_LOG(WARNING) << "region_id : " << region_id << "dose not exist in "
                         << " index_id :" << vector_index_->GetId();
    } else {
      CHECK_EQ(region->RegionId(), region_id);
      regions_[region->RegionId()] = region;
    }
  }

  return Status::OK();
}

void VectorLoadByRegionTask::DoAsync() {
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    status_ = Status::OK();
  }

  if (regions_.empty()) {
    DINGO_LOG(WARNING) << "regions are empty ";
    DoAsyncDone(Status::LoadFailed(""));
  }

  param_.mutable_diskann()->set_warmup(true);
  param_.mutable_diskann()->set_num_nodes_to_cache(0);

  controllers_.clear();
  rpcs_.clear();
  result_.region_status.clear();

  for (auto& it : regions_) {
    auto region = it.second;
    auto rpc = std::make_unique<VectorLoadRpc>();

    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
    *(rpc->MutableRequest()->mutable_parameter()) = param_;

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);
    rpcs_.push_back(std::move(rpc));
  }
  DCHECK_EQ(rpcs_.size(), regions_.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(regions_.size());

  for (auto i = 0; i < regions_.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorLoadByRegionRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorLoadByRegionTask::VectorLoadByRegionRpcCallback(const Status& status, VectorLoadRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (pb::error::Errno::EDISKANN_IS_NO_DATA != status.Errno()) {
      RegionStatus region_status;
      region_status.region_id = rpc->Request()->context().region_id();
      region_status.status = status;
      result_.region_status.push_back(std::move(region_status));
    } else {
      regions_.erase(rpc->Request()->context().region_id());
    }
  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    regions_.erase(rpc->Request()->context().region_id());
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    if (status_.ok() && !result_.region_status.empty()) {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      status_ = Status::LoadFailed("");
    }
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}
bool VectorLoadByRegionTask::NeedRetry() {
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