#include "sdk/vector/diskann/vector_diskann_status_by_index_task.h"

#include <cstdint>
#include <set>
#include <utility>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/common/common.h"
#include "sdk/utils/scoped_cleanup.h"
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
    if (status_.ok()) {
      status_ = status;
    }

  } else {
    CHECK(rpc->Response()->has_state()) << "no state";
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