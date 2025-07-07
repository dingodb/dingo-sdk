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

#ifndef DINGODB_SDK_SDK_VERSION_H_
#define DINGODB_SDK_SDK_VERSION_H_

#include <string>
#include <vector>

namespace dingodb {
namespace sdk {

#ifndef GIT_VERSION
#define GIT_VERSION "unknown"
#endif

#ifndef MAJOR_VERSION
#define MAJOR_VERSION "unknown"
#endif

#ifndef MINOR_VERSION
#define MINOR_VERSION "unknown"
#endif

#ifndef GIT_TAG_NAME
#define GIT_TAG_NAME "unknown"
#endif

#ifndef GIT_COMMIT_USER
#define GIT_COMMIT_USER "unknown"
#endif

#ifndef GIT_COMMIT_MAIL
#define GIT_COMMIT_MAIL "unknown"
#endif

#ifndef GIT_COMMIT_TIME
#define GIT_COMMIT_TIME "unknown"
#endif

#ifndef DINGO_SDK_BUILD_TYPE
#define DINGO_SDK_BUILD_TYPE "unknown"
#endif

void DingoSdkShowVersion();
void DingoSdkLogVersion();
std::vector<std::pair<std::string, std::string>> DingoSdkVersion();

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_SDK_VERSION_H_