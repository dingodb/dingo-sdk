#ifndef DINGODB_SDK_VECTOR_DOSKANN_STATUS_BY_REGION_TASK_H_
#define DINGODB_SDK_VECTOR_DOSKANN_STATUS_BY_REGION_TASK_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "sdk/client_stub.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "dingosdk/vector.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorStatusByRegionTask : public VectorTask {
 public:
  VectorStatusByRegionTask(const ClientStub& stub, int64_t index_id, const std::vector<int64_t>& region_ids,
                           StateResult& result)
      : VectorTask(stub), index_id_(index_id), region_id_(region_ids), result_(result) {}

  ~VectorStatusByRegionTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  int retry_count_{0};
  DiskANNRegionState ChangeRegionState(VectorStatusRpc* rpc);

  std::string Name() const override { return fmt::format("VectorStatusByRegionTask-{}", index_id_); }

  void VectorStatusByRegionRpcCallback(const Status& status, VectorStatusRpc* rpc);

  const int64_t index_id_;
  const std::vector<int64_t>& region_id_;
  StateResult& result_;

  std::shared_ptr<VectorIndex> vector_index_;
  std::set<int64_t> region_ids_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorStatusRpc>> rpcs_;
  std::atomic<int> sub_tasks_count_{0};

  std::shared_mutex rw_lock_;
  Status status_;
};
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_DISKANN_STATUS_TASK_H_