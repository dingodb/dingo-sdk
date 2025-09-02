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

#include <fmt/format.h>

#include <vector>

#include "dingosdk/status.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_coordinator_rpc_controller.h"
#include "proto/error.pb.h"
#include "sdk/common/param_config.h"
#include "sdk/region.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/rpc/coordinator_rpc_controller.h"
#include "sdk/rpc/rpc.h"
#include "sdk/utils/net_util.h"
#include "test_base.h"

namespace dingodb {
namespace sdk {

class SDKCoordinatorRpcControllerTest : public TestBase {};

TEST_F(SDKCoordinatorRpcControllerTest, RegionIsNotLeader) {
  auto test_start = std::chrono::high_resolution_clock::now();

  ScanRegionsRpc rpc;
  std::string start_key = "a";
  std::string end_key = "z";
  rpc.MutableRequest()->set_key(start_key);
  rpc.MutableRequest()->set_range_end(end_key);
  rpc.MutableRequest()->set_limit(1);

  CoordinatorRpcController controller(*stub);
  std::vector<EndPoint> endpoints = {EndPoint("127.0.0.1", 10000), EndPoint("127.0.0.1", 10001),
                                     EndPoint("127.0.0.1", 10002)};
  controller.Open(endpoints);

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* scan_rpc = dynamic_cast<ScanRegionsRpc*>(&rpc);
        CHECK_NOTNULL(scan_rpc);
        auto* response = scan_rpc->MutableResponse();
        response->mutable_error()->set_errcode(pb::error::ERAFT_NOTLEADER);
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* scan_rpc = dynamic_cast<ScanRegionsRpc*>(&rpc);
        CHECK_NOTNULL(scan_rpc);
        auto* response = scan_rpc->MutableResponse();
        response->mutable_regions()->Add()->set_region_id(1);
        cb();
      });

  Status got = controller.SyncCall(rpc);
  EXPECT_TRUE(got.ok());

  auto test_end = std::chrono::high_resolution_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(test_end - test_start);
  EXPECT_GT(duration_ms.count(), FLAGS_coordinator_interaction_delay_ms)
      << fmt::format("The entire test case took less than {} milliseconds", FLAGS_coordinator_interaction_delay_ms);
}

}  // namespace sdk
}  // namespace dingodb