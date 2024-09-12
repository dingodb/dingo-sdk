
#include "sdk/vector/diskann/vector_diskann_count_memory_task.h"

#include <cstdint>

#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/common/common.h"
#include "sdk/status.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

// VectorCountMemoryByIndexTask
Status VectorCountMemoryByIndexTask::Init() {
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

void VectorCountMemoryByIndexTask::DoAsync() {
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
    auto* sub_task = new VectorCountMemoryPartTask(stub, vector_index_, part_id);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorCountMemoryByIndexTask::SubTaskCallback(Status status, VectorCountMemoryPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    tmp_count_.fetch_add(sub_task->GetCount());
    std::unique_lock<std::shared_mutex> w(rw_lock_);
    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      std::shared_lock<std::shared_mutex> r(rw_lock_);
      tmp = status_;
    }

    if (tmp.ok()) {
      count_ = tmp_count_.load();
    }

    DoAsyncDone(tmp);
  }
}

// VectorCountMemoryPartTask
void VectorCountMemoryPartTask::DoAsync() {
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
  count_.store(0);

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorCountMemoryRpc>();
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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorCountMemoryRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorCountMemoryPartTask::VectorCountMemoryRpcCallback(const Status& status, VectorCountMemoryRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    std::unique_lock<std::shared_mutex> w(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    DINGO_LOG(INFO) << "CountMemory return count: " << rpc->Response()->count()
                    << " ,region id : " << rpc->Request()->context().region_id() << " ,part id : " << part_id_;
    count_.fetch_add(rpc->Response()->count());
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