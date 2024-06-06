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

#ifndef DINGODB_BENCHMARK_UTIL_H_
#define DINGODB_BENCHMARK_UTIL_H_

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "butil/strings/string_split.h"
#include "common/logging.h"
#include "fmt/format.h"

namespace dingodb {
namespace benchmark {

static void SplitString(const std::string& str, char c, std::vector<std::string>& vec) {
  butil::SplitString(str, c, &vec);
}

static void SplitString(const std::string& str, char c, std::vector<int64_t>& vec) {
  std::vector<std::string> strs;
  SplitString(str, c, strs);
  for (auto& s : strs) {
    try {
      vec.push_back(std::stoll(s));
    } catch (const std::exception& e) {
      DINGO_LOG(ERROR) << "stoll exception: " << e.what();
    }
  }
}

static int64_t GenerateRealRandomInteger(int64_t min_value, int64_t max_value) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_int_distribution<int64_t> dis(min_value, max_value);

  // Generate and print a random int64 number
  int64_t random_number = dis(gen);

  return random_number;
}

static int64_t GenerateRandomInteger(int64_t min_value, int64_t max_value) {
  std::mt19937 rng;
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
}

static float GenerateRandomFloat(float min_value, float max_value) {
  std::random_device rd;  // Obtain a random seed from the hardware
  std::mt19937 rng(rd());
  std::uniform_real_distribution<> distrib(min_value, max_value);

  return distrib(rng);
}

static std::string GenerateRandomString(int length) {
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string rand_string;

  unsigned int seed = time(nullptr);  // Get seed value for rand_r()

  for (int i = 0; i < length; i++) {
    int rand_index = rand_r(&seed) % chars.size();
    rand_string += chars[rand_index];
  }

  return rand_string;
}

static int64_t GenId() {
  static int64_t id = 0;
  return ++id;
}

static std::vector<float> GenerateFloatVector(int dimension) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_real_distribution<> dis(0, 1);

  std::vector<float> vec;
  vec.reserve(dimension);
  for (int i = 0; i < dimension; ++i) {
    vec.push_back(dis(gen));
  }
  return vec;
}

static std::vector<uint8_t> GenerateInt8Vector(int dimension) {
  // Create a random number generator engine
  std::random_device rd;      // Obtain a random seed from the hardware
  std::mt19937_64 gen(rd());  // Standard 64-bit mersenne_twister_engine seeded with rd()

  // Create a distribution for the desired range
  std::uniform_int_distribution<uint8_t> dis(0, 255);

  std::vector<uint8_t> vec;
  vec.reserve(dimension);
  for (int i = 0; i < dimension; ++i) {
    vec.push_back(dis(gen));
  }
  return vec;
}

static float DingoHnswL2Sqr(const float* p_vect1v, const float* p_vect2v, size_t d) {
  float* p_vect1 = (float*)p_vect1v;
  float* p_vect2 = (float*)p_vect2v;

  float res = 0;
  for (size_t i = 0; i < d; i++) {
    float t = *p_vect1 - *p_vect2;
    p_vect1++;
    p_vect2++;
    res += t * t;
  }
  return (res);
}

static void CreateDirectories(const std::string& path) {
  std::error_code ec;
  if (std::filesystem::exists(path)) {
    DINGO_LOG(INFO) << fmt::format("Directory already exists, path: {}", path);

    return;
  }

  if (!std::filesystem::create_directories(path, ec)) {
    DINGO_LOG(FATAL) << fmt::format("Create directory {} failed, error: {} {}", path, ec.value(), ec.message());
  }

  DINGO_LOG(INFO) << fmt::format("Create directory success, path: {}", path);
}

static int32_t GetCores() {
  int32_t cores = sysconf(_SC_NPROCESSORS_ONLN);
  CHECK(cores > 0) << "System not support cpu core count.";

  return cores;
}

// Next prefix
static std::string PrefixNext(const std::string& input) {
  std::string ret(input.size(), 0);
  int carry = 1;
  for (int i = input.size() - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(input[i]) == (uint8_t)0xFF && carry == 1) {
      ret[i] = 0;
    } else {
      ret[i] = (input[i] + carry);
      carry = 0;
    }
  }

  return (carry == 0) ? ret : input;
}

static std::string PrefixNext(const std::string_view& input) {
  std::string ret(input.size(), 0);
  int carry = 1;
  for (int i = input.size() - 1; i >= 0; --i) {
    if (static_cast<uint8_t>(input[i]) == (uint8_t)0xFF && carry == 1) {
      ret[i] = 0;
    } else {
      ret[i] = (input[i] + carry);
      carry = 0;
    }
  }

  return (carry == 0) ? ret : std::string(input.data(), input.size());
}

static std::vector<std::string> TraverseDirectory(const std::string& path, const std::string& prefix,
                                                  bool ignore_dir = false, bool ignore_file = false) {
  std::vector<std::string> filenames;
  try {
    if (std::filesystem::exists(path)) {
      for (const auto& fe : std::filesystem::directory_iterator(path)) {
        if (ignore_dir && fe.is_directory()) {
          continue;
        }

        if (ignore_file && fe.is_regular_file()) {
          continue;
        }

        if (prefix.empty()) {
          filenames.push_back(fe.path().filename().string());
        } else {
          // check if the filename start with prefix
          auto filename = fe.path().filename().string();
          if (filename.find(prefix) == 0L) {
            filenames.push_back(filename);
          }
        }
      }
    }
  } catch (std::filesystem::filesystem_error const& ex) {
    DINGO_LOG(ERROR) << fmt::format("directory_iterator failed, path: {} error: {}", path, ex.what());
  }

  return filenames;
}

static std::vector<std::string> TraverseDirectory(const std::string& path, bool ignore_dir = false,
                                                  bool ignore_file = false) {
  return TraverseDirectory(path, "", ignore_dir, ignore_file);
}

static int64_t TimestampNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

static int64_t TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

static int64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

static int64_t Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

static std::string FormatMsTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str()) << "." << timestamp % 1000;
  return ss.str();
}

static std::string NowTime() { return FormatMsTime(TimestampMs(), "%Y-%m-%d %H:%M:%S"); }

static int NowHour() {
  std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm* ptm = std::localtime(&tt);
  return ptm->tm_hour;
}

static std::string FormatTime(int64_t timestamp, const std::string& format) {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

static std::string PastDate(int64_t day) { return FormatTime(Timestamp() - day * 86400, "%Y-%m-%d"); }

static std::string FormatMsTime(int64_t timestamp) { return FormatMsTime(timestamp, "%Y-%m-%d %H:%M:%S"); }

static std::string GetNowFormatMsTime() {
  int64_t timestamp = TimestampMs();
  std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp(
      (std::chrono::milliseconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%dT%H:%M:%S.000Z");
  return ss.str();
}

static std::string ToUpper(const std::string& str) {
  std::string result;
  result.resize(str.size());
  std::transform(str.begin(), str.end(), result.begin(), ::toupper);
  return result;
}

static std::string ToLower(const std::string& str) {
  std::string result;
  result.resize(str.size());
  std::transform(str.begin(), str.end(), result.begin(), ::tolower);
  return result;
}

static bool SaveFile(const std::string& filepath, const std::string& data) {
  std::ofstream file(filepath);
  if (!file.is_open()) {
    return false;
  }

  file << data;
  file.close();

  return true;
}

static bool IsExistPath(const std::string& path) { return std::filesystem::exists(path); }

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_UTIL_H_
