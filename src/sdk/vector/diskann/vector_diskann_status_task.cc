#include "sdk/vector/diskann/vector_diskann_status_task.h"

#include <cstdint>
#include <set>
#include <utility>

#include "common/logging.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/common/common.h"
#include "sdk/status.h"
#include "sdk/utils/scoped_cleanup.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {

namespace sdk {

// StatusByIndex
Status VectorStatusByIndexTask::Init() {
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

void VectorStatusByIndexTask::DoAsync() {
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
  sub_tasks_count_.store(next_part_ids.size());
  for (const auto& part_id : next_part_ids) {
    auto* sub_task = new VectorStatusPartTask(stub, vector_index_, part_id);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorStatusByIndexTask::SubTaskCallback(const Status& status, VectorStatusPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(INFO) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      status_ = status;
    }

  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    StateResult result = sub_task->GetResult();
    for (auto& err_status : result.region_states) {
      result_.region_states.push_back(err_status);
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

// StatusPartTask
void VectorStatusPartTask::DoAsync() {
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
  result_.region_states.clear();

  for (const auto& region : regions) {
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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorStatusRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorStatusPartTask::VectorStatusRpcCallback(const Status& status, VectorStatusRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    RegionState region_states;
    region_states.region_id = rpc->Request()->context().region_id();
    region_states.status = status;
    result_.region_states.push_back(std::move(region_states));

  } else {
    RegionState region_states;
    region_states.region_id = rpc->Request()->context().region_id();
    region_states.status = Status::OK();
    CHECK(rpc->Response()->has_state()) << "no state";
    region_states.state = DiskANNStatePB2DiskANNState(rpc->Response()->state().diskann().diskann_state());
    result_.region_states.push_back(std::move(region_states));
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

bool VectorStatusPartTask::NeedRetry() {
  for (auto& result : result_.region_states) {
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

// StatusByRegion
Status VectorStatusByRegionTask ::Init() {
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

  std::set<int64_t> check_duplicates_ids;
  for (const auto& id : region_ids_) {
    if (!check_duplicates_ids.insert(id).second) {
      return Status::InvalidArgument("duplicate vector id: " + std::to_string(id));
    }
  }

  for (auto region_id : region_ids_) {
    std::shared_ptr<Region> region;
    Status s = stub.GetMetaCache()->LookupRegionByRegionId(region_id, region);
    if (!s.ok()) {
      RegionState region_states;
      region_states.region_id = region_id;
      region_states.status = s;
      tmp_result_.region_states.push_back(std::move(region_states));
      DINGO_LOG(WARNING) << "region_id : " << region_id << " is not found \n"
                         << "status : " << s.ToString();
      continue;
    }

    if (!vector_index_->ExistRegion(region)) {
      RegionState region_states;
      region_states.region_id = region_id;
      region_states.status = Status::InvalidArgument("region is not DiskANN Index Region");
      tmp_result_.region_states.push_back(std::move(region_states));

      DINGO_LOG(WARNING) << "region_id : " << region_id << "dose not exist in "
                         << " index_id :" << vector_index_->GetId();
    } else {
      CHECK_EQ(region->RegionId(), region_id);
      regions_[region->RegionId()] = region;
    }
  }
  return Status::OK();
}

void VectorStatusByRegionTask::DoAsync() {
  {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    status_ = Status::OK();
  }

  if (regions_.empty()) {
    result_.region_states = tmp_result_.region_states;
    DoAsyncDone(Status::OK());
    return;
  }

  controllers_.clear();
  rpcs_.clear();
  result_.region_states.clear();

  for (auto& it : regions_) {
    auto region = it.second;
    auto rpc = std::make_unique<VectorStatusRpc>();

    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());

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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorStatusByRegionRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorStatusByRegionTask::VectorStatusByRegionRpcCallback(const Status& status, VectorStatusRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    std::unique_lock<std::shared_mutex> w(rw_lock_);

    RegionState region_states;
    region_states.region_id = rpc->Request()->context().region_id();
    region_states.status = status;
    result_.region_states.push_back(std::move(region_states));

  } else {
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    regions_.erase(rpc->Request()->context().region_id());

    RegionState region_states;
    region_states.region_id = rpc->Request()->context().region_id();
    region_states.status = Status::OK();
    region_states.state = DiskANNStatePB2DiskANNState(rpc->Response()->state().diskann().diskann_state());
    result_.region_states.push_back(std::move(region_states));
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    {
      std::unique_lock<std::shared_mutex> w(rw_lock_);
      for (auto& r : tmp_result_.region_states) {
        result_.region_states.push_back(r);
      }
    }
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

bool VectorStatusByRegionTask::NeedRetry() {
  for (auto& result : result_.region_states) {
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