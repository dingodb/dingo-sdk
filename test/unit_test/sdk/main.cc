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

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "report/allure.h"
#include "sdk/common/param_config.h"
#include "sdk/sdk_version.h"

DEFINE_string(allure_report, "", "allure report directory");

static dingodb::pb::common::VersionInfo BuildSdkVersionInfo() {
  dingodb::pb::common::VersionInfo version_info;

  version_info.set_git_commit_hash(GIT_VERSION);
  version_info.set_git_tag_name(GIT_TAG_NAME);
  version_info.set_git_commit_user(GIT_COMMIT_USER);
  version_info.set_git_commit_mail(GIT_COMMIT_MAIL);
  version_info.set_git_commit_time(GIT_COMMIT_TIME);
  version_info.set_major_version(MAJOR_VERSION);
  version_info.set_minor_version(MINOR_VERSION);
  version_info.set_dingo_build_type(DINGO_SDK_BUILD_TYPE);
  version_info.set_dingo_contrib_build_type(DINGO_SDK_BUILD_TYPE);
  version_info.set_use_mkl(false);
  version_info.set_use_openblas(false);
  version_info.set_use_tcmalloc(false);
  version_info.set_use_profiler(false);
  version_info.set_use_sanitizer(false);
  version_info.set_use_diskann(false);
  version_info.set_diskann_depend_on_system(false);
  version_info.set_boost_summary("");

  return version_info;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  FLAGS_store_rpc_retry_delay_ms = 100;
  FLAGS_store_rpc_max_retry = 5;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  int ret = RUN_ALL_TESTS();

  const auto version_info = BuildSdkVersionInfo();

  if (!FLAGS_allure_report.empty()) {
    dingodb::report::allure::Allure::GenReport(
        testing::UnitTest::GetInstance(), version_info, FLAGS_allure_report,
        {{"epic", "dingosdk(c++)"}, {"parentSuite", "c++ sdk unit"}});
  }

  return ret;
}
