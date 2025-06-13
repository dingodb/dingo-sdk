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

#include <butil/string_printf.h>

#include "common/logging.h"

namespace dingodb {
namespace sdk {

DEFINE_string(git_commit_hash, GIT_VERSION, "current git commit version");
DEFINE_string(git_tag_name, GIT_TAG_NAME, "current dingo sdk  git tag version");
DEFINE_string(git_commit_user, GIT_COMMIT_USER, "current dingo sdk  git commit user");
DEFINE_string(git_commit_mail, GIT_COMMIT_MAIL, "current dingo sdk  git commit mail");
DEFINE_string(git_commit_time, GIT_COMMIT_TIME, "current dingo sdk  git commit time");
DEFINE_string(major_version, MAJOR_VERSION, "current dingo sdk  major version");
DEFINE_string(minor_version, MINOR_VERSION, "current dingo sdk  minor version");
DEFINE_string(dingo_build_type, DINGO_SDK_BUILD_TYPE, "current dingo sdk  build type");

std::string GetBuildFlag() {
  // TODO
  return "";
}

void DingoSdkShowVerion() {
  printf("DINGO_SDK VERSION:[%s-%s]\n", FLAGS_major_version.c_str(), FLAGS_minor_version.c_str());
  printf("DINGO_SDK GIT_TAG_VERSION:[%s]\n", FLAGS_git_tag_name.c_str());
  printf("DINGO_SDK GIT_COMMIT_HASH:[%s]\n", FLAGS_git_commit_hash.c_str());
  printf("DINGO_SDK GIT_COMMIT_USER:[%s]\n", FLAGS_git_commit_user.c_str());
  printf("DINGO_SDK GIT_COMMIT_MAIL:[%s]\n", FLAGS_git_commit_mail.c_str());
  printf("DINGO_SDK GIT_COMMIT_TIME:[%s]\n", FLAGS_git_commit_time.c_str());
  printf("DINGO_SDK BUILD_TYPE:[%s]\n", FLAGS_dingo_build_type.c_str());
  printf("%s", GetBuildFlag().c_str());
  printf("PID: %d\n\n", getpid());
}

void DingoSdkLogVerion() {
  DINGO_LOG(INFO) << "DINGO_SDK VERSION:[" << FLAGS_major_version << "-" << FLAGS_minor_version << "]";
  DINGO_LOG(INFO) << "DINGO_SDK GIT_TAG_VERSION:[" << FLAGS_git_tag_name << "]";
  DINGO_LOG(INFO) << "DINGO_SDK GIT_COMMIT_HASH:[" << FLAGS_git_commit_hash << "]";
  DINGO_LOG(INFO) << "DINGO_SDK GIT_COMMIT_USER:[" << FLAGS_git_commit_user << "]";
  DINGO_LOG(INFO) << "DINGO_SDK GIT_COMMIT_MAIL:[" << FLAGS_git_commit_mail << "]";
  DINGO_LOG(INFO) << "DINGO_SDK GIT_COMMIT_TIME:[" << FLAGS_git_commit_time << "]";
  DINGO_LOG(INFO) << "DINGO_SDK BUILD_TYPE:[" << FLAGS_dingo_build_type << "]";
  DINGO_LOG(INFO) << GetBuildFlag();
  DINGO_LOG(INFO) << "PID: " << getpid() << "\n";
}

}  // namespace sdk
}  // namespace dingodb