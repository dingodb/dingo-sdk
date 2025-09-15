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

#include "sdk/common/rand.h"

#include <bits/types/struct_timeval.h>
#include <sys/select.h>
#include <sys/time.h>

#include <cstdint>

namespace dingodb {
namespace sdk {

RandHelper::RandSeed RandHelper::rand_seed = {0, 0};

bool RandHelper::NeedInit() { return rand_seed.seed[0] == 0 && rand_seed.seed[1] == 0; }

uint64_t RandHelper::SplitMix64(uint64_t* seed) {
  uint64_t z = (*seed += UINT64_C(0x9E3779B97F4A7C15));
  z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
  z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
  return z ^ (z >> 31);
}

int64_t RandHelper::GetTimeOfDayUs() {
  timeval now;
  gettimeofday(&now, nullptr);
  return now.tv_sec * 1000000 + now.tv_usec;
}

void RandHelper::InitRandSeed() {
  uint64_t time_seed = static_cast<uint64_t>(GetTimeOfDayUs());
  rand_seed.seed[0] = SplitMix64(&time_seed);
  rand_seed.seed[1] = SplitMix64(&time_seed);
}

uint64_t RandHelper::XORShift128() {
  uint64_t s1 = rand_seed.seed[0];
  const uint64_t s0 = rand_seed.seed[1];
  rand_seed.seed[0] = s0;
  s1 ^= s1 << 23;
  rand_seed.seed[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5); 
  return rand_seed.seed[1] + s0;
}

uint64_t RandHelper::RandUInt64() {
  if (NeedInit()) {
    InitRandSeed();
  }
  return XORShift128();
}

std::string RandHelper::RandString(uint64_t length) {
  if (length == 0) return "";
  std::string result;
  result.resize(length);

  const uint64_t n = length / 8;
  const uint64_t m = length % 8;

  for (uint64_t i = 0; i < n; ++i) {
    uint64_t random_chunk = RandUInt64();
    for (int j = 0; j < 8; ++j) {
      result[i * 8 + j] = static_cast<char>(random_chunk & 0xFF);
      random_chunk >>= 8;
    }
  }

  if (m > 0) {
    uint64_t random_chunk = RandUInt64();
    for (uint64_t i = 0; i < m; ++i) {
      result[n * 8 + i] = static_cast<char>(random_chunk & 0xFF);
      random_chunk >>= 8;
    }
  }

  return result;
}

}  // namespace sdk
}  // namespace dingodb
