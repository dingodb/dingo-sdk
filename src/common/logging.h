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

#ifndef DINGODB_SDK_COMMON_LOGGING_H_
#define DINGODB_SDK_COMMON_LOGGING_H_

#include "glog/logging.h"

namespace dingodb {

const int64_t kSdkVlogLevel = 60;
#define SDK_DEBUG 56
#define SDK_INFO 53
#define SDK_WARN 50

/**
 * define the debug log level.
 * The larger the number, the more comprehensive information is displayed.
 */
#define DINGO_DEBUG 79
static constexpr int kGlobalValueOfDebug = DINGO_DEBUG;

#define CURRENT_FUNC_NAME "[" << __func__ << "] "

#define DINGO_LOG(level) DINGO_LOG_##level

#define DINGO_LOG_IF(level, condition) DINGO_LOG_IF_##level(condition)

#define DINGO_LOG_DEBUG VLOG(DINGO_DEBUG) << CURRENT_FUNC_NAME
#define DINGO_LOG_INFO LOG(INFO) << CURRENT_FUNC_NAME
#define DINGO_LOG_WARNING LOG(WARNING) << CURRENT_FUNC_NAME
#define DINGO_LOG_ERROR LOG(ERROR) << CURRENT_FUNC_NAME
#define DINGO_LOG_FATAL LOG(FATAL) << CURRENT_FUNC_NAME

#define DINGO_LOG_IF(level, condition) DINGO_LOG_IF_##level(condition)

#define DINGO_LOG_IF_DEBUG(condition) VLOG_IF(DINGO_DEBUG, condition) << CURRENT_FUNC_NAME
#define DINGO_LOG_IF_INFO(condition) LOG_IF(INFO, condition) << CURRENT_FUNC_NAME
#define DINGO_LOG_IF_WARNING(condition) LOG_IF(WARNING, condition) << CURRENT_FUNC_NAME
#define DINGO_LOG_IF_ERROR(condition) LOG_IF(ERROR, condition) << CURRENT_FUNC_NAME
#define DINGO_LOG_IF_FATAL(condition) LOG_IF(FATAL, condition) << CURRENT_FUNC_NAME

}  // namespace dingodb

#endif  // DINGODB_SDK_COMMON_LOGGING_H_
