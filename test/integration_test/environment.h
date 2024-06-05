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

#ifndef DINGODB_INTEGRATION_TEST_ENVIROMENT_
#define DINGODB_INTEGRATION_TEST_ENVIROMENT_

#include <mutex>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "sdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/common/helper.h"
#include "sdk/common/logging.h"
#include "sdk/rpc/coordinator_rpc.h"

DECLARE_string(coordinator_addrs);

namespace dingodb {

namespace integration_test {

class Environment : public testing::Environment {
 public:
  Environment() : client_stub_(std::make_shared<sdk::ClientStub>()) {}

  static Environment& GetInstance() {
    static Environment* environment = nullptr;

    static std::once_flag flag;
    std::call_once(flag, [&]() {
      // gtest free
      environment = new Environment();
    });

    return *environment;
  }

  void SetUp() override {
    DINGO_LOG(INFO) << "using FLAGS_coordinator_addrs: " << FLAGS_coordinator_addrs;

    std::vector<sdk::EndPoint> endpoints = sdk::StringToEndpoints(FLAGS_coordinator_addrs);
    auto s = client_stub_->Open(endpoints);
    CHECK(s.ok()) << "Open coordinator proxy failed, please check parameter --coordinator_addrs="
                  << FLAGS_coordinator_addrs;

    sdk::Client* tmp;
    auto status = sdk::Client::BuildFromAddrs(FLAGS_coordinator_addrs, &tmp);
    CHECK(status.ok()) << fmt::format("Build sdk client failed, error: {}", status.ToString());
    client_.reset(tmp);

    // Get dingo-store version info
    version_info_ = GetVersionInfo();
    PrintVersionInfo(version_info_);
  }

  void TearDown() override {}

  std::shared_ptr<sdk::Client> GetClient() { return client_; }

  pb::common::VersionInfo VersionInfo() { return version_info_; }

 private:
  pb::common::VersionInfo GetVersionInfo() {
    sdk::coordinator::HelloRpc rpc;
    rpc.MutableRequest()->set_is_just_version_info(true);

    LOG(INFO) << "Hello request: " << rpc.Request()->ShortDebugString();

    auto status = client_stub_->GetCoordinatorRpcController()->SyncCall(rpc);
    CHECK(status.ok()) << fmt::format("Hello failed, status: {}", status.ToString());

    return rpc.Response()->version_info();
  }

  static void PrintVersionInfo(const pb::common::VersionInfo& version_info) {
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_hash", version_info.git_commit_hash());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_tag_name", version_info.git_tag_name());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_user", version_info.git_commit_user());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_mail", version_info.git_commit_mail());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "git_commit_time", version_info.git_commit_time());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "major_version", version_info.major_version());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "minor_version", version_info.minor_version());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "dingo_build_type", version_info.dingo_build_type());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "dingo_contrib_build_type", version_info.dingo_contrib_build_type());
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_mkl", (version_info.use_mkl() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_openblas", (version_info.use_openblas() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_tcmalloc", (version_info.use_tcmalloc() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_profiler", (version_info.use_profiler() ? "true" : "false"));
    LOG(INFO) << fmt::format("{:<24}: {:>64}", "use_sanitizer", (version_info.use_sanitizer() ? "true" : "false"));
  }

  std::shared_ptr<sdk::ClientStub> client_stub_;
  std::shared_ptr<sdk::Client> client_;

  pb::common::VersionInfo version_info_;
};

}  // namespace integration_test

}  // namespace dingodb

#endif  // DINGODB_INTEGRATION_TEST_ENVIROMENT_
