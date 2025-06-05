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

#ifndef DINGODB_SDK_CONSTANT_H_
#define DINGODB_SDK_CONSTANT_H_

#include <cstdint>

namespace dingodb {

// Should Consistent with Dingo-Store
class Constant {
 public:
  // region range prefix
  inline static const char kExecutorRaw = 'r';
  inline static const char kExecutorTxn = 't';
  inline static const char kClientRaw = 'w';
  inline static const char kClientTxn = 'x';

  // vector key len
  inline static const uint32_t kVectorKeyMinLenWithPrefix = 9;
  inline static const uint32_t kVectorKeyMaxLenWithPrefix = 17;

  // document key len
  inline static const uint32_t kDocumentKeyMinLenWithPrefix = 9;
  inline static const uint32_t kDocumentKeyMaxLenWithPrefix = 17;
};

}  // namespace dingodb

#endif  // DINGODB_SDK_CONSTANT_H_
