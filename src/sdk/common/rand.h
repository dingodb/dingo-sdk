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

#ifndef DINGODB_SDK_RAND_H_
#define DINGODB_SDK_RAND_H_

#include <sys/types.h>

#include <cstdint>
#include <string>

namespace dingodb {
namespace sdk {

class RandHelper {
 public:
  RandHelper() = default;
  ~RandHelper() = default;

  static uint64_t RandUInt64();
  static std::string RandString(uint64_t length);

 private:
  struct RandSeed {
    uint64_t seed[2];
  };

  static bool NeedInit();
  static void InitRandSeed();

  static uint64_t XORShift128();
  static uint64_t SplitMix64(uint64_t* seed);
  static int64_t GetTimeOfDayUs();
  static RandSeed rand_seed;
};

}  // namespace sdk
}  // namespace dingodb
#endif