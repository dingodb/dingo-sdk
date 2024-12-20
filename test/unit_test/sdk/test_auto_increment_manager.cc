// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <memory>
#include <mutex>

#include "gtest/gtest.h"
#include "proto/meta.pb.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/status.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index.h"
#include "test_base.h"

namespace dingodb {
namespace sdk {

class SDKAutoInrementerTest : public TestBase {
 public:
  void SetUp() override { Init(); }

  void TearDown() override {}

  std::shared_ptr<VectorIndex> vector_index;
  std::shared_ptr<VectorIndexAutoInrementer> incrementer;

  std::string index_name{"incrementer-test"};
  int64_t schema_id{2};
  std::vector<int64_t> index_and_part_ids{2, 3, 4, 5, 6};
  int64_t index_id = index_and_part_ids[0];
  std::vector<int64_t> range_seperator_ids = {5, 10, 20};
  FlatParam flat_param{1000, dingodb::sdk::MetricType::kL2};
  int64_t increment_start_id = 3;

 private:
  void Init() {
    pb::meta::IndexDefinitionWithId index_definition_with_id;
    {
      FillVectorIndexId(index_definition_with_id.mutable_index_id(), index_id, schema_id);
      auto* defination = index_definition_with_id.mutable_index_definition();
      defination->set_name(index_name);
      FillRangePartitionRule(defination->mutable_index_partition(), range_seperator_ids, index_and_part_ids);
      defination->set_replica(3);
      defination->set_with_auto_incrment(true);
      defination->set_auto_increment(increment_start_id);

      auto* index_parameter = defination->mutable_index_parameter();
      index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
      FillFlatParmeter(index_parameter->mutable_vector_index_parameter(), flat_param);
    }

    vector_index = std::make_shared<VectorIndex>(index_definition_with_id);
    incrementer = std::make_shared<VectorIndexAutoInrementer>(*stub, vector_index);
  }
};

TEST_F(SDKAutoInrementerTest, CacheEmpty) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
    EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
    EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
    EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
    t_rpc->MutableResponse()->set_start_id(1);
    t_rpc->MutableResponse()->set_end_id(3);
    return Status::OK();
  });

  int64_t id = 0;
  Status s = incrementer->GetNextId(id);

  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id, 1);
}

TEST_F(SDKAutoInrementerTest, FromCache) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
    EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
    EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
    EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
    t_rpc->MutableResponse()->set_start_id(1);
    t_rpc->MutableResponse()->set_end_id(3);
    return Status::OK();
  });

  {
    int64_t id = 0;
    Status s = incrementer->GetNextId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 1);
  }

  {
    int64_t id = 0;
    Status s = incrementer->GetNextId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 2);
  }
}

TEST_F(SDKAutoInrementerTest, FromCacheThenRefill) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(1);
        t_rpc->MutableResponse()->set_end_id(3);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(3);
        t_rpc->MutableResponse()->set_end_id(4);
        return Status::OK();
      });

  {
    int64_t id = 0;
    Status s = incrementer->GetNextId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 1);
  }

  {
    int64_t id = 0;
    Status s = incrementer->GetNextId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 2);
  }

  {
    int64_t id = 0;
    Status s = incrementer->GetNextId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 3);
  }
}

TEST_F(SDKAutoInrementerTest, MultiThreadGetNextId) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(1);
        t_rpc->MutableResponse()->set_end_id(100);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(100);
        t_rpc->MutableResponse()->set_end_id(200);
        return Status::OK();
      });

  int count = 50;

  auto func = [&]() {
    std::vector<int64_t> ids;
    Status s = incrementer->GetNextIds(ids, count);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ids.size(), count);
    int64_t next = ids[0];
    for (const auto& id : ids) {
      EXPECT_EQ(id, next);
      next++;
    }
  };

  std::thread t1(func);
  std::thread t2(func);
  std::thread t3(func);

  t1.join();
  t2.join();
  t3.join();
}

TEST_F(SDKAutoInrementerTest, CacheGetAutoId) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
    EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
    EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
    EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
    t_rpc->MutableResponse()->set_start_id(1);
    t_rpc->MutableResponse()->set_end_id(3);
    return Status::OK();
  });

  {
    int64_t id = 0;
    Status s = incrementer->GetAutoIncrementId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 1);
  }
}

TEST_F(SDKAutoInrementerTest, CacheUpdate) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(1);
        t_rpc->MutableResponse()->set_end_id(5);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<UpdateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->start_id(), 35);
        EXPECT_EQ(t_rpc->Request()->force(), true);
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(35);
        t_rpc->MutableResponse()->set_end_id(40);
        return Status::OK();
      });

  {
    int64_t id = 0;
    Status s = incrementer->GetAutoIncrementId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 1);
  }

  {
    int64_t id = 35;
    Status s = incrementer->UpdateAutoIncrementId(id);
    EXPECT_TRUE(s.ok());
  }

  {
    int64_t id = 0;
    Status s = incrementer->GetAutoIncrementId(id);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(id, 35);
  }
}

TEST_F(SDKAutoInrementerTest, MultiThreadUpdateId) {
  EXPECT_CALL(*meta_rpc_controller, SyncCall)
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(1);
        t_rpc->MutableResponse()->set_end_id(5);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<UpdateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->start_id(), 35);
        EXPECT_EQ(t_rpc->Request()->force(), true);
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(35);
        t_rpc->MutableResponse()->set_end_id(40);
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<UpdateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->start_id(), 75);
        EXPECT_EQ(t_rpc->Request()->force(), true);
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        return Status::OK();
      })
      .WillOnce([&](Rpc& rpc) {
        auto* t_rpc = dynamic_cast<GenerateAutoIncrementRpc*>(&rpc);
        EXPECT_EQ(t_rpc->Request()->count(), FLAGS_auto_incre_req_count);
        EXPECT_EQ(t_rpc->Request()->auto_increment_increment(), 1);
        EXPECT_EQ(t_rpc->Request()->auto_increment_offset(), vector_index->GetIncrementStartId());
        EXPECT_EQ(t_rpc->Request()->table_id().entity_id(), vector_index->GetId());
        t_rpc->MutableResponse()->set_start_id(75);
        t_rpc->MutableResponse()->set_end_id(80);
        return Status::OK();
      });

  std::mutex mutex;
  std::condition_variable cv;
  int current_thread = 1;

  auto get_begin_id_func = [&]() {
    {
      std::unique_lock<std::mutex> lk(mutex);

      int64_t id = 0;
      Status s = incrementer->GetAutoIncrementId(id);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(id, 1);

      current_thread = 2;
    }
    cv.notify_all();
  };

  auto update_id_func_1 = [&]() {
    {
      std::unique_lock<std::mutex> lk(mutex);
      cv.wait(lk, [&] { return current_thread == 2; });
      int64_t id = 35;
      Status s = incrementer->UpdateAutoIncrementId(id);
      EXPECT_TRUE(s.ok());
      current_thread = 3;
    }
    cv.notify_all();
  };

  auto get_update_id_func_1 = [&]() {
    {
      std::unique_lock<std::mutex> lk(mutex);
      cv.wait(lk, [&] { return current_thread == 3; });
      int64_t id = 0;
      Status s = incrementer->GetAutoIncrementId(id);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(id, 35);
      current_thread = 4;
    }
    cv.notify_all();
  };
  
  auto update_id_func_2 = [&]() {
    {
      std::unique_lock<std::mutex> lk(mutex);
      cv.wait(lk, [&] { return current_thread == 4; });
      int64_t id = 75;
      Status s = incrementer->UpdateAutoIncrementId(id);
      EXPECT_TRUE(s.ok());
      current_thread = 5;
    }
    cv.notify_all();
  };

  auto get_update_id_func_2 = [&]() {
    {
      std::unique_lock<std::mutex> lk(mutex);
      cv.wait(lk, [&] { return current_thread == 5; });
      int64_t id = 0;
      Status s = incrementer->GetAutoIncrementId(id);
      EXPECT_TRUE(s.ok());
      EXPECT_EQ(id, 75);
    }
    cv.notify_all();
  };

  std::thread t1(get_begin_id_func);
  std::thread t2(update_id_func_1);
  std::thread t3(get_update_id_func_1);
  std::thread t4(update_id_func_2);
  std::thread t5(get_update_id_func_2);

  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
}

}  // namespace sdk
}  // namespace dingodb