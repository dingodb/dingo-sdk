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

#include "sdk/sdk_version.h"

#include <iostream>

#include "common/logging.h"
#include "fmt/format.h"

namespace dingodb {
namespace sdk {

static const std::string kGitCommitHash = GIT_VERSION;
static const std::string kGitTagName = GIT_TAG_NAME;
static const std::string kGitCommitUser = GIT_COMMIT_USER;
static const std::string kGitCommitMail = GIT_COMMIT_MAIL;
static const std::string kGitCommitTime = GIT_COMMIT_TIME;
static const std::string kMajorVersion = MAJOR_VERSION;
static const std::string kMinorVersion = MINOR_VERSION;
static const std::string kDingoSdkBuildType = DINGO_SDK_BUILD_TYPE;

static std::string GetBuildFlag() {
  // TODO
  return "";
}

void DingoSdkShowVersion() {
  std::cout << fmt::format("DINGO_SDK VERSION:[{}-{}]\n", kMajorVersion.c_str(), kMinorVersion.c_str());
  std::cout << fmt::format("DINGO_SDK GIT_TAG_VERSION:[{}]\n", kGitTagName.c_str());
  std::cout << fmt::format("DINGO_SDK GIT_COMMIT_HASH:[{}]\n", kGitCommitHash.c_str());
  std::cout << fmt::format("DINGO_SDK GIT_COMMIT_USER:[{}]\n", kGitCommitUser.c_str());
  std::cout << fmt::format("DINGO_SDK GIT_COMMIT_MAIL:[{}]\n", kGitCommitMail.c_str());
  std::cout << fmt::format("DINGO_SDK GIT_COMMIT_TIME:[{}]\n", kGitCommitTime.c_str());
  std::cout << fmt::format("DINGO_SDK BUILD_TYPE:[{}]\n", kDingoSdkBuildType.c_str());
  std::cout << GetBuildFlag() << "\n";
}

void DingoSdkLogVersion() {
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK VERSION:[{}-{}]", kMajorVersion, kMinorVersion);
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK GIT_TAG_VERSION:[{}]", kGitTagName);
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK GIT_COMMIT_HASH:[{}]", kGitCommitHash);
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK GIT_COMMIT_USER:[{}]", kGitCommitUser);
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK GIT_COMMIT_MAIL:[{}]", kGitCommitMail);
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK GIT_COMMIT_TIME:[{}]", kGitCommitTime);
  DINGO_LOG(INFO) << fmt::format("DINGO_SDK BUILD_TYPE:[{}]", kDingoSdkBuildType);
  DINGO_LOG(INFO) << GetBuildFlag();
}

std::vector<std::pair<std::string, std::string>> DingoSdkVersion() {
  std::vector<std::pair<std::string, std::string>> result;
  result.emplace_back("VERSION", fmt::format("{}-{}", kMajorVersion, kMinorVersion));
  result.emplace_back("TAG_VERSION", kGitTagName);
  result.emplace_back("COMMIT_HASH", kGitCommitHash);
  result.emplace_back("COMMIT_USER", kGitCommitUser);
  result.emplace_back("COMMIT_MAIL", kGitCommitMail);
  result.emplace_back("COMMIT_TIME", kGitCommitTime);
  result.emplace_back("BUILD_TYPE", kDingoSdkBuildType);

  return result;
}

}  // namespace sdk
}  // namespace dingodb