
#include "sdk/vector/diskann/vector_diskann_dump.h"

#include <cstdint>
#include <string>
#include <vector>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/common/common.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

Status VectorDumpTask::Init() {
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

void VectorDumpTask::DoAsync() {
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
    auto* sub_task = new VectorDumpPartTask(stub, vector_index_, part_id);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorDumpTask::SubTaskCallback(Status status, VectorDumpPartTask* sub_task) {
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
    next_part_ids_.erase(sub_task->part_id_);
    for (auto& data : sub_task->GetPartDatas()) {
      datas_.push_back(data);
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

void VectorDumpPartTask::DoAsync() {
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

  for (const auto& region : regions) {
    auto rpc = std::make_unique<VectorDumpRpc>();
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
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorDumpRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorDumpPartTask::VectorDumpRpcCallback(const Status& status, VectorDumpRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    for (int i = 0; i < rpc->Response()->dump_datas_size(); i++) {
      part_datas_.push_back(rpc->Response()->dump_datas()[i]);
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