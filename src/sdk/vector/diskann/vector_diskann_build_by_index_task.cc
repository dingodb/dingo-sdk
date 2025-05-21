
#include "sdk/vector/diskann/vector_diskann_build_by_index_task.h"

#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/common/common.h"
#include "sdk/region.h"
#include "sdk/utils/scoped_cleanup.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

// BuildByIndex
Status VectorBuildByIndexTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  if (vector_index_->GetVectorIndexType() != VectorIndexType::kDiskAnn) {
    return Status::InvalidArgument("vector_index is not diskann");
  }

  WriteLockGuard guard(rw_lock_);
  auto part_ids = vector_index_->GetPartitionIds();

  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  return Status::OK();
}

void VectorBuildByIndexTask::DoAsync() {
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
    auto* sub_task = new VectorBuildPartTask(stub, vector_index_, part_id);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorBuildByIndexTask::SubTaskCallback(const Status& status, VectorBuildPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(INFO) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();
    if (status.IsBuildFailed()) {
      WriteLockGuard guard(rw_lock_);
      ErrStatusResult result = sub_task->GetResult();
      for (auto& err_status : result.region_status) {
        result_.region_status.push_back(err_status);
      }
      if (status_.ok()) {
        status_ = status;
      }
    } else {
      WriteLockGuard guard(rw_lock_);
      status_ = status;
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

// BuildPartTask
void VectorBuildPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);
  std::vector<std::shared_ptr<Region>> regions;
  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    WriteLockGuard guard(rw_lock_);
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();
  result_.region_status.clear();

  for (const auto& region : regions) {
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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorBuildRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorBuildPartTask::VectorBuildRpcCallback(const Status& status, VectorBuildRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    WriteLockGuard guard(rw_lock_);

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
      WriteLockGuard guard(rw_lock_);
      status_ = Status::BuildFailed("");
    }
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

bool VectorBuildPartTask::NeedRetry() {
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