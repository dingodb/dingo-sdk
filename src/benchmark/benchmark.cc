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

#include "benchmark/benchmark.h"

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "benchmark/color.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "sdk/client_stub.h"
#include "sdk/common/helper.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "util.h"

DEFINE_string(coordinator_addrs, "file://./coor_list", "coordinator addrs");
DEFINE_bool(show_version, false, "Show dingo-store version info");
DEFINE_string(prefix, "BENCH", "Region range prefix");

DEFINE_string(raw_engine, "LSM", "Raw engine type");
DEFINE_validator(raw_engine, [](const char*, const std::string& value) -> bool {
  auto raw_engine_type = dingodb::benchmark::ToUpper(value);
  return raw_engine_type == "LSM" || raw_engine_type == "BTREE" || raw_engine_type == "XDP";
});

DEFINE_uint32(region_num, 1, "Region number");
DEFINE_uint32(vector_index_num, 1, "Vector index number");
DEFINE_uint32(concurrency, 1, "Concurrency of request");

DEFINE_uint64(req_num, 10000, "Request number");
DEFINE_uint32(timelimit, 0, "Time limit in seconds");

DEFINE_uint32(delay, 2, "Interval in seconds between intermediate reports");

DEFINE_bool(is_single_region_txn, true, "Is single region txn");
DEFINE_uint32(replica, 3, "Replica number");

DEFINE_bool(is_clean_region, true, "Is clean region");

DEFINE_string(vector_index_type, "HNSW", "Vector index type");
DEFINE_validator(vector_index_type, [](const char*, const std::string& value) -> bool {
  auto vector_index_type = dingodb::benchmark::ToUpper(value);
  return vector_index_type == "HNSW" || vector_index_type == "FLAT" || vector_index_type == "IVF_FLAT" ||
         vector_index_type == "IVF_PQ" || vector_index_type == "BRUTE_FORCE" || vector_index_type == "DISKANN";
});

// vector
DEFINE_uint32(vector_dimension, 256, "Vector dimension");
DEFINE_string(vector_value_type, "FLOAT", "Vector value type");
DEFINE_validator(vector_value_type, [](const char*, const std::string& value) -> bool {
  auto value_type = dingodb::benchmark::ToUpper(value);
  return value_type == "FLOAT" || value_type == "UINT8";
});
DEFINE_uint32(vector_max_element_num, 100000, "Vector index contain max element number");
DEFINE_string(vector_metric_type, "L2", "Calcute vector distance method");
DEFINE_validator(vector_metric_type, [](const char*, const std::string& value) -> bool {
  auto metric_type = dingodb::benchmark::ToUpper(value);
  return metric_type == "NONE" || metric_type == "L2" || metric_type == "IP" || metric_type == "COSINE";
});
DEFINE_string(vector_partition_vector_ids, "", "Vector id used by partition");

DEFINE_uint32(hnsw_ef_construction, 500, "HNSW ef construction");
DEFINE_uint32(hnsw_nlink_num, 64, "HNSW nlink number");

DEFINE_uint32(ivf_ncentroids, 2048, "IVF ncentroids");
DEFINE_uint32(ivf_nsubvector, 64, "IVF nsubvector");
DEFINE_uint32(ivf_bucket_init_size, 1000, "IVF bucket init size");
DEFINE_uint32(ivf_bucket_max_size, 1280000, "IVF bucket max size");
DEFINE_uint32(ivf_nbits_per_idx, 8, "IVF nbits per idx");

// diskann
DEFINE_int32(diskann_max_degree, 64, "Diskann max degree");
DEFINE_int32(diskann_search_list_size, 100, "Diskann search list size");
DEFINE_uint32(diskann_search_beamwidth, 2, "Diskann search beam width");

// vector search
DEFINE_string(vector_dataset, "", "Open source dataset, like sift/gist/glove/mnist etc.., hdf5 format");
DEFINE_validator(vector_dataset, [](const char*, const std::string& value) -> bool {
  return value.empty() || dingodb::benchmark::IsExistPath(value);
});

DEFINE_uint32(vector_index_id, 0, "Vector index id");
DEFINE_string(vector_index_name, "", "Vector index name");

DEFINE_uint32(vector_search_topk, 10, "Vector search flag topk");
DEFINE_bool(with_vector_data, true, "Vector search flag with_vector_data");
DEFINE_bool(with_scalar_data, false, "Vector search flag with_scalar_data");
DEFINE_bool(with_table_data, false, "Vector search flag with_table_data");
DEFINE_bool(vector_search_use_brute_force, false, "Vector search flag use_brute_force");
DEFINE_bool(vector_search_enable_range_search, false, "Vector search flag enable_range_search");
DEFINE_double(vector_search_radius, 0.1, "Vector search flag radius");
DEFINE_string(vector_search_scalar_filter_radius, "",
              "Vector search scalar filter radius. such as 0.1, 0.2 0.5 10 1 20 2 50 5");

DECLARE_uint32(vector_put_batch_size);
DECLARE_uint32(vector_arrange_concurrency);
DECLARE_bool(vector_search_arrange_data);
DECLARE_uint32(vector_search_nprobe);
DECLARE_uint32(vector_search_ef);

DECLARE_string(benchmark);
DECLARE_uint32(key_size);
DECLARE_uint32(value_size);
DECLARE_uint32(batch_size);
DECLARE_bool(is_pessimistic_txn);
DECLARE_string(txn_isolation_level);

DECLARE_string(filter_field);

// monitor cpu memory usage
DEFINE_bool(enable_monitor_vector_performance_info, false, "Monitor performance information");
DEFINE_int32(index_store_id_1, 21001, "index store_id 1");
DEFINE_int32(index_store_id_2, 21002, "index store_id 2");
DEFINE_int32(index_store_id_3, 21003, "index store_id 3");

// auto balance region
DEFINE_bool(auto_balance_region, false, "auto balance region, default false");

namespace dingodb {
namespace benchmark {

static const std::string kClientRaw = "w";
static const std::string kClientTxn = "x";

static const std::string kNamePrefix = "Benchmark";

static std::string EncodeRawKey(const std::string& str) { return kClientRaw + str; }
static std::string EncodeTxnKey(const std::string& str) { return kClientTxn + str; }

sdk::EngineType GetRawEngineType() {
  auto raw_engine = dingodb::benchmark::ToUpper(FLAGS_raw_engine);
  if (raw_engine == "LSM") {
    return sdk::EngineType::kLSM;
  } else if (FLAGS_raw_engine == "BTREE") {
    return sdk::EngineType::kBTree;
  } else if (raw_engine == "XDP") {
    return sdk::EngineType::kXDPROCKS;
  }

  LOG(FATAL) << fmt::format("Not support raw_engine: {}", FLAGS_raw_engine);
  return sdk::EngineType::kLSM;
}

static bool IsTransactionBenchmark() {
  return (FLAGS_benchmark == "filltxnseq" || FLAGS_benchmark == "filltxnrandom" || FLAGS_benchmark == "readtxnseq" ||
          FLAGS_benchmark == "readtxnrandom" || FLAGS_benchmark == "readtxnmissing");
}

static bool IsVectorBenchmark() {
  return FLAGS_benchmark == "fillvectorseq" || FLAGS_benchmark == "fillvectorrandom" ||
         FLAGS_benchmark == "searchvector" || FLAGS_benchmark == "queryvector";
}

Stats::Stats() {
  latency_recorder_ = std::make_shared<bvar::LatencyRecorder>();
  recall_recorder_ = std::make_shared<bvar::LatencyRecorder>();
}

void Stats::Add(size_t duration, size_t write_bytes, size_t read_bytes) {
  ++req_num_;
  write_bytes_ += write_bytes;
  read_bytes_ += read_bytes;
  latency_min_ = (latency_min_ == 0) ? duration : std::min(latency_min_, duration);
  *latency_recorder_ << duration;
}

void Stats::Add(size_t duration, size_t write_bytes, size_t read_bytes, const std::vector<uint32_t>& recalls) {
  ++req_num_;
  write_bytes_ += write_bytes;
  read_bytes_ += read_bytes;
  *latency_recorder_ << duration;
  latency_min_ = (latency_min_ == 0) ? duration : std::min(latency_min_, duration);
  for (auto recall : recalls) {
    *recall_recorder_ << recall;
  }
}

void Stats::AddError() { ++error_count_; }

void Stats::Clear() {
  ++epoch_;
  req_num_ = 0;
  write_bytes_ = 0;
  read_bytes_ = 0;
  error_count_ = 0;
  latency_min_ = 0;
  latency_recorder_ = std::make_shared<bvar::LatencyRecorder>();
  recall_recorder_ = std::make_shared<bvar::LatencyRecorder>();
}

void Stats::Report(bool is_cumulative, size_t milliseconds,
                   const std::map<std::int64_t, sdk::StoreOwnMetics>& store_id_to_store_own_metrics) const {
  double seconds = milliseconds / static_cast<double>(1000);

  if (is_cumulative) {
    std::cout << COLOR_GREEN << fmt::format("Cumulative({}ms):", milliseconds) << COLOR_RESET << '\n';
  } else {
    if (epoch_ == 1) {
      std::cout << COLOR_GREEN << fmt::format("Interval({}ms):", FLAGS_delay * 1000) << COLOR_RESET << '\n';
    }
    if (epoch_ % 20 == 1) {
      std::cout << COLOR_GREEN << Header() << COLOR_RESET << '\n';
    }
  }

  if (FLAGS_vector_dataset.empty()) {
    std::cout << fmt::format("{:>8}{:>8}{:>8}{:>8.0f}{:>8.2f}{:>16}{:>12}{:>12}{:>12}{:>12}", epoch_, req_num_,
                             error_count_, (req_num_ / seconds), (write_bytes_ / seconds / 1048576),
                             latency_recorder_->latency(), latency_recorder_->max_latency(),
                             latency_recorder_->latency_percentile(0.5), latency_recorder_->latency_percentile(0.95),
                             latency_recorder_->latency_percentile(0.99))
              << '\n';
  } else {
    if (FLAGS_enable_monitor_vector_performance_info) {
      std::cout << fmt::format(
                       "{:>8}{:>8}{:>8}{:>8.0f}{:>8.2f}{:>16}{:>12}{:>12}{:>12}{:>12}{:>12}{:>16.2f}{:>20}{:>20}{:>20."
                       "2f}{:>20}"
                       "{:>20}{:>20.2f}{:>20}{:>20}{:>20.2f}",
                       epoch_, req_num_, error_count_, (req_num_ / seconds), (write_bytes_ / seconds / 1048576),
                       latency_recorder_->latency(), latency_recorder_->max_latency(), latency_min_,
                       latency_recorder_->latency_percentile(0.5), latency_recorder_->latency_percentile(0.95),
                       latency_recorder_->latency_percentile(0.99), recall_recorder_->latency() / 100.0,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_1).system_cpu_usage,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_1).process_used_memory / 1024 / 1024,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_1).system_capacity_usage,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_2).system_cpu_usage,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_2).process_used_memory / 1024 / 1024,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_2).system_capacity_usage,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_3).system_cpu_usage,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_3).process_used_memory / 1024 / 1024,
                       store_id_to_store_own_metrics.at(FLAGS_index_store_id_3).system_capacity_usage)
                << '\n';
    } else {
      std::cout << fmt::format("{:>8}{:>8}{:>8}{:>8.0f}{:>8.2f}{:>16}{:>12}{:>12}{:>12}{:>12}{:>16.2f}", epoch_,
                               req_num_, error_count_, (req_num_ / seconds), (write_bytes_ / seconds / 1048576),
                               latency_recorder_->latency(), latency_recorder_->max_latency(),
                               latency_recorder_->latency_percentile(0.5), latency_recorder_->latency_percentile(0.95),
                               latency_recorder_->latency_percentile(0.99), recall_recorder_->latency() / 100.0)
                << '\n';
    }
  }
}

std::string Stats::Header() {
  if (FLAGS_vector_dataset.empty()) {
    return fmt::format("{:>8}{:>8}{:>8}{:>8}{:>8}{:>16}{:>12}{:>12}{:>12}{:>12}", "EPOCH", "REQ_NUM", "ERRORS", "QPS",
                       "MB/s", "LATENCY AVG(us)", "MAX(us)", "P50(us)", "P95(us)", "P99(us)");
  } else {
    if (FLAGS_enable_monitor_vector_performance_info) {
      return fmt::format(
          "{:>8}{:>8}{:>8}{:>8}{:>8}{:>16}{:>12}{:>12}{:>12}{:>12}{:>12}{:>16}{:>20}{:>20}{:>20}{:>20}{:>20}{:>20}{:>"
          "20}{:>20}{:>20}",
          "EPOCH", "REQ_NUM", "ERRORS", "QPS", "MB/s", "LATENCY AVG(us)", "MAX(us)", "MIN(us)", "P50(us)", "P95(us)",
          "P99(us)", "RECALL AVG(%)", "INDEX_1_CPU(%)", "INDEX_1_MEMORY(GB)", "INDEX_1_DISK(%)", "INDEX_2_CPU(%)",
          "INDEX_2_MEMORY(GB)", "INDEX_2_DISK(%)", "INDEX_3_CPU(%)", "INDEX_3_MEMORY(GB)", "INDEX_3_DISK(%)");
    }
    return fmt::format("{:>8}{:>8}{:>8}{:>8}{:>8}{:>16}{:>12}{:>12}{:>12}{:>12}{:>16}", "EPOCH", "REQ_NUM", "ERRORS",
                       "QPS", "MB/s", "LATENCY AVG(us)", "MAX(us)", "P50(us)", "P95(us)", "P99(us)", "RECALL AVG(%)");
  }
}

Benchmark::Benchmark(std::shared_ptr<dingodb::sdk::ClientStub> client_stub, std::shared_ptr<sdk::Client> client)
    : client_stub_(client_stub), client_(client) {
  stats_interval_ = std::make_shared<Stats>();
  stats_cumulative_ = std::make_shared<Stats>();
}

std::shared_ptr<Benchmark> Benchmark::New(std::shared_ptr<dingodb::sdk::ClientStub> client_stub,
                                          std::shared_ptr<sdk::Client> client) {
  return std::make_shared<Benchmark>(client_stub, client);
}

void Benchmark::Stop() {
  for (auto& thread_entry : thread_entries_) {
    thread_entry->is_stop.store(true, std::memory_order_relaxed);
  }
}

bool Benchmark::Run() {
  if (!Arrange()) {
    Clean();
    return false;
  }

  Launch();

  size_t start_time = dingodb::benchmark::TimestampMs();

  while (true) {
    if (operation_->ReadyReport()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // Interval report
  IntervalReport();

  Wait();

  // Cumulative report
  Report(true, dingodb::benchmark::TimestampMs() - start_time);

  Clean();
  return true;
}

bool Benchmark::Arrange() {
  std::cout << COLOR_GREEN << "Arrange: " << COLOR_RESET << '\n';

  if (IsVectorBenchmark()) {
    if (!FLAGS_vector_dataset.empty()) {
      dataset_ = Dataset::New(FLAGS_vector_dataset);
      if (dataset_ == nullptr || !dataset_->Init()) {
        return false;
      }
      if (dataset_->GetDimension() > 0) {
        FLAGS_vector_dimension = dataset_->GetDimension();
      }
    }

    if (FLAGS_vector_index_id > 0 || !FLAGS_vector_index_name.empty()) {
      vector_index_entries_ = ArrangeExistVectorIndex(FLAGS_vector_index_id, FLAGS_vector_index_name);
    } else {
      vector_index_entries_ = ArrangeVectorIndex(FLAGS_vector_index_num);
      if (vector_index_entries_.size() != FLAGS_vector_index_num) {
        return false;
      }
    }
  } else {
    region_entries_ = ArrangeRegion(FLAGS_region_num);
    if (region_entries_.size() != FLAGS_region_num) {
      return false;
    }
  }

  if (!ArrangeOperation()) {
    DINGO_LOG(ERROR) << "Arrange operation failed";
    return false;
  }

  if (!ArrangeData()) {
    DINGO_LOG(ERROR) << "Arrange data failed";
    return false;
  }

  std::cout << '\n';
  return true;
}

std::vector<RegionEntryPtr> Benchmark::ArrangeRegion(int num) {
  std::mutex mutex;
  std::vector<RegionEntryPtr> region_entries;

  bool is_txn_region = IsTransactionBenchmark();

  std::vector<std::thread> threads;
  threads.reserve(num);
  for (int thread_no = 0; thread_no < num; ++thread_no) {
    threads.emplace_back([this, is_txn_region, thread_no, &region_entries, &mutex]() {
      auto name = fmt::format("{}_{}_{}", kNamePrefix, dingodb::benchmark::TimestampMs(), thread_no + 1);
      std::string prefix = fmt::format("{}{:06}", FLAGS_prefix, thread_no);
      int64_t region_id = 0;
      if (is_txn_region) {
        region_id = CreateTxnRegion(name, prefix, PrefixNext(prefix), GetRawEngineType(), FLAGS_replica);
      } else {
        region_id = CreateRawRegion(name, prefix, PrefixNext(prefix), GetRawEngineType(), FLAGS_replica);
      }
      if (region_id == 0) {
        LOG(ERROR) << fmt::format("create region failed, name: {}", name);
        return;
      }

      std::cout << fmt::format("create region name({}) id({}) prefix({}) done", name, region_id, prefix) << '\n';

      auto region_entry = std::make_shared<RegionEntry>();
      region_entry->prefix = prefix;
      region_entry->region_id = region_id;

      std::lock_guard lock(mutex);
      region_entries.push_back(region_entry);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return region_entries;
}

std::vector<VectorIndexEntryPtr> Benchmark::ArrangeVectorIndex(int num) {
  std::vector<VectorIndexEntryPtr> vector_index_entries;

  // test dimension is ready
  std::string wait_string = fmt::format("wait vector index dimension ready ");
  while (true) {
    if (dataset_->GetObtainDimension()) {
      LOG(INFO) << fmt::format("vector index dimension: {}", FLAGS_vector_dimension);
      std::cout << fmt::format("\nvector index dimension: {}", FLAGS_vector_dimension) << std::endl;
      break;
    }
    wait_string += ".";
    std::cout << '\r' << wait_string << std::flush;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  std::vector<std::thread> threads;
  threads.reserve(num);
  std::mutex mutex;
  for (int thread_no = 0; thread_no < num; ++thread_no) {
    threads.emplace_back([this, thread_no, &vector_index_entries, &mutex]() {
      std::string name = fmt::format("{}_{}_{}", kNamePrefix, dingodb::benchmark::TimestampMs(), thread_no + 1);
      auto index_id = CreateVectorIndex(name, FLAGS_vector_index_type);
      if (index_id == 0) {
        LOG(ERROR) << fmt::format("create vector index failed, name: {}", name);
        return;
      }

      std::cout << fmt::format("create vector index name({}) id({}) type({}) done", name, index_id,
                               FLAGS_vector_index_type)
                << '\n';

      auto entry = std::make_shared<VectorIndexEntry>();
      entry->index_id = index_id;

      std::lock_guard lock(mutex);
      vector_index_entries.push_back(entry);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  return vector_index_entries;
}

std::vector<VectorIndexEntryPtr> Benchmark::ArrangeExistVectorIndex(int64_t vector_index_id,
                                                                    const std::string& vector_index_name) {
  std::vector<VectorIndexEntryPtr> vector_index_entries;
  auto entry = std::make_shared<VectorIndexEntry>();
  if (vector_index_id > 0) {
    entry->index_id = vector_index_id;
    vector_index_entries.push_back(entry);
  } else if (!vector_index_name.empty()) {
    entry->index_id = GetVectorIndex(vector_index_name);
    if (entry->index_id > 0) {
      vector_index_entries.push_back(entry);
    }
  }

  return vector_index_entries;
}

bool Benchmark::ArrangeOperation() {
  operation_ = NewOperation(client_);

  return true;
}

bool Benchmark::ArrangeData() {
  for (auto& region_entry : region_entries_) {
    operation_->Arrange(region_entry);
  }
  // monitor cpu memoory disk usage before put data
  if (FLAGS_enable_monitor_vector_performance_info) {
    std::map<std::int64_t, sdk::StoreOwnMetics> store_id_to_store_own_metrics;
    std::vector<int64_t> store_ids;
    store_ids.push_back(FLAGS_index_store_id_1);
    store_ids.push_back(FLAGS_index_store_id_2);
    store_ids.push_back(FLAGS_index_store_id_3);

    sdk::Status s = client_->GetStoreOwnMetrics(store_ids, store_id_to_store_own_metrics);
    if (!s.ok()) {
      std::cerr << fmt::format("Get store own metrics failed, status: {}", s.ToString()) << '\n';
      store_id_to_store_own_metrics.clear();
      store_id_to_store_own_metrics[FLAGS_index_store_id_1] = sdk::StoreOwnMetics();
      store_id_to_store_own_metrics[FLAGS_index_store_id_2] = sdk::StoreOwnMetics();
      store_id_to_store_own_metrics[FLAGS_index_store_id_3] = sdk::StoreOwnMetics();
    }
    int index = 0;
    std::cout << "Monitor performance before put data \n";
    for (const auto& store_id_to_store_own_metric : store_id_to_store_own_metrics) {
      std::cout << fmt::format("INDEX_{} :  cpu_usage : {}%  memory_usage : {}GB  disk_usage : {}%", ++index,
                               store_id_to_store_own_metric.second.system_cpu_usage,
                               store_id_to_store_own_metric.second.process_used_memory / 1024 / 1024,
                               store_id_to_store_own_metric.second.system_capacity_usage)
                << "\n";
    }
  }

  for (auto& vector_index_entry : vector_index_entries_) {
    operation_->Arrange(vector_index_entry, dataset_);
  }

  // monitor cpu memoory disk usage after put data
  if (FLAGS_enable_monitor_vector_performance_info) {
    std::map<std::int64_t, sdk::StoreOwnMetics> store_id_to_store_own_metrics;
    std::vector<int64_t> store_ids;
    store_ids.push_back(FLAGS_index_store_id_1);
    store_ids.push_back(FLAGS_index_store_id_2);
    store_ids.push_back(FLAGS_index_store_id_3);

    sdk::Status s = client_->GetStoreOwnMetrics(store_ids, store_id_to_store_own_metrics);
    if (!s.ok()) {
      std::cerr << fmt::format("Get store own metrics failed, status: {}", s.ToString()) << '\n';
      store_id_to_store_own_metrics.clear();
      store_id_to_store_own_metrics[FLAGS_index_store_id_1] = sdk::StoreOwnMetics();
      store_id_to_store_own_metrics[FLAGS_index_store_id_2] = sdk::StoreOwnMetics();
      store_id_to_store_own_metrics[FLAGS_index_store_id_3] = sdk::StoreOwnMetics();
    }
    int index = 0;
    std::cout << "Monitor performance after put data \n";
    for (const auto& store_id_to_store_own_metric : store_id_to_store_own_metrics) {
      std::cout << fmt::format("INDEX_{} :  cpu_usage : {}%  memory_usage : {}GB  disk_usage : {}%", ++index,
                               store_id_to_store_own_metric.second.system_cpu_usage,
                               store_id_to_store_own_metric.second.process_used_memory / 1024 / 1024,
                               store_id_to_store_own_metric.second.system_capacity_usage)
                << "\n";
    }
  }

  return true;
}

void Benchmark::Launch() {
  // Create multiple thread run benchmark
  thread_entries_.reserve(FLAGS_concurrency);
  for (int i = 0; i < FLAGS_concurrency; ++i) {
    auto thread_entry = std::make_shared<ThreadEntry>();
    thread_entry->client = client_;
    thread_entry->region_entries = region_entries_;
    thread_entry->vector_index_entries = vector_index_entries_;

    thread_entry->thread =
        std::thread([this](ThreadEntryPtr thread_entry) mutable { ThreadRoutine(thread_entry); }, thread_entry);
    thread_entries_.push_back(thread_entry);
  }
}

void Benchmark::Wait() {
  for (auto& thread_entry : thread_entries_) {
    thread_entry->thread.join();
  }
}

void Benchmark::Clean() {
  if (FLAGS_is_clean_region) {
    // Drop region
    for (auto& region_entry : region_entries_) {
      DropRegion(region_entry->region_id);
    }

    // Drop vector index
    for (auto& vector_index_entry : vector_index_entries_) {
      DropVectorIndex(vector_index_entry->index_id);
    }
  }
}

int64_t Benchmark::CreateRawRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                                   sdk::EngineType engine_type, int replicas) {
  sdk::RegionCreator* tmp;
  auto status = client_->NewRegionCreator(&tmp);
  CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());
  std::shared_ptr<sdk::RegionCreator> creator(tmp);

  int64_t region_id;
  status = creator->SetRegionName(name)
               .SetEngineType(engine_type)
               .SetReplicaNum(replicas)
               .SetRange(EncodeRawKey(start_key), EncodeRawKey(end_key))
               .Create(region_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("create region failed, {}", status.ToString());
    return 0;
  }
  if (region_id == 0) {
    LOG(ERROR) << "region_id is 0, invalid";
  }

  return region_id;
}

int64_t Benchmark::CreateTxnRegion(const std::string& name, const std::string& start_key, const std::string& end_key,
                                   sdk::EngineType engine_type, int replicas) {
  sdk::RegionCreator* tmp;
  auto status = client_->NewRegionCreator(&tmp);
  CHECK(status.ok()) << fmt::format("new region creator failed, {}", status.ToString());
  std::shared_ptr<sdk::RegionCreator> creator(tmp);

  int64_t region_id;
  status = creator->SetRegionName(name)
               .SetEngineType(engine_type)
               .SetReplicaNum(replicas)
               .SetRange(EncodeTxnKey(start_key), EncodeTxnKey(end_key))
               .Create(region_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("create region failed, {}", status.ToString());
    return 0;
  }
  if (region_id == 0) {
    LOG(ERROR) << "region_id is 0, invalid";
  }

  return region_id;
}

bool Benchmark::IsStop() {
  bool all_stop = true;
  for (auto& thread_entry : thread_entries_) {
    if (!thread_entry->is_stop.load(std::memory_order_relaxed)) {
      all_stop = false;
    }
  }

  return all_stop;
}

void Benchmark::DropRegion(int64_t region_id) {
  CHECK(region_id != 0) << "region_id is invalid";
  auto status = client_->DropRegion(region_id);
  CHECK(status.IsOK()) << fmt::format("Drop region failed, {}", status.ToString());
}

sdk::MetricType GetMetricType(const std::string& metric_type) {
  auto upper_metric_type = dingodb::benchmark::ToUpper(metric_type);

  if (upper_metric_type == "L2") {
    return sdk::MetricType::kL2;
  } else if (upper_metric_type == "IP") {
    return sdk::MetricType::kInnerProduct;
  } else if (upper_metric_type == "COSINE") {
    return sdk::MetricType::kCosine;
  }

  return sdk::MetricType::kNoneMetricType;
}

sdk::ValueType GetValueType(const std::string& value_type) {
  auto upper_value_type = dingodb::benchmark::ToUpper(value_type);

  if (upper_value_type == "FLOAT") {
    return sdk::ValueType::kFloat;
  } else if (upper_value_type == "UINT8") {
    return sdk::ValueType::kUint8;
  } else if (upper_value_type == "INT8") {
    return sdk::ValueType::kInt8;
  }

  return sdk::ValueType::kNoneValueType;
}

sdk::FlatParam GenFlatParam() {
  sdk::FlatParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  return param;
}

sdk::IvfFlatParam GenIvfFlatParam() {
  sdk::IvfFlatParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  param.ncentroids = FLAGS_ivf_ncentroids;
  return param;
}

sdk::IvfPqParam GenIvfPqParam() {
  sdk::IvfPqParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  param.ncentroids = FLAGS_ivf_ncentroids;
  param.nsubvector = FLAGS_ivf_nsubvector;
  param.bucket_init_size = FLAGS_ivf_bucket_init_size;
  param.bucket_max_size = FLAGS_ivf_bucket_max_size;
  param.nbits_per_idx = FLAGS_ivf_nbits_per_idx;
  return param;
}

sdk::HnswParam GenHnswParam() {
  sdk::HnswParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type), FLAGS_vector_max_element_num);
  param.ef_construction = FLAGS_hnsw_ef_construction;
  param.nlinks = FLAGS_hnsw_nlink_num;

  return param;
}

sdk::BruteForceParam GenBruteForceParam() {
  sdk::BruteForceParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type));
  return param;
}

sdk::DiskAnnParam GenDiskANNParam() {
  sdk::DiskAnnParam param(FLAGS_vector_dimension, GetMetricType(FLAGS_vector_metric_type),
                          GetValueType(FLAGS_vector_value_type));
  param.max_degree = FLAGS_diskann_max_degree;
  param.search_list_size = FLAGS_diskann_search_list_size;
  return param;
}

static bool IsDigitString(const std::string& str) {
  for (const auto& c : str) {
    if (!std::isdigit(c)) {
      return false;
    }
  }

  return true;
}

// parse format: field1:int,field2:string
static std::vector<std::vector<std::string>> ParseFilterField(const std::string& value) {
  std::vector<std::vector<std::string>> result;

  std::vector<std::string> parts;
  SplitString(value, ',', parts);

  for (auto& part : parts) {
    std::vector<std::string> sub_parts;
    SplitString(part, ':', sub_parts);
    if (sub_parts.size() >= 2) {
      result.push_back(sub_parts);
    }
  }

  return result;
}

int64_t Benchmark::CreateVectorIndex(const std::string& name, const std::string& vector_index_type) {
  sdk::VectorIndexCreator* creator = nullptr;
  auto status = client_->NewVectorIndexCreator(&creator);
  CHECK(status.ok()) << fmt::format("new vector index creator failed, {}", status.ToString());

  int64_t vector_index_id = 0;
  std::vector<int64_t> separator_id;
  if (!FLAGS_vector_partition_vector_ids.empty()) {
    SplitString(FLAGS_vector_partition_vector_ids, ',', separator_id);
  }

  creator->SetName(name)
      .SetSchemaId(pb::meta::ReservedSchemaIds::DINGO_SCHEMA)
      .SetRangePartitions(separator_id)
      .SetReplicaNum(FLAGS_replica);

  if (vector_index_type == "HNSW") {
    creator->SetHnswParam(GenHnswParam());
  } else if (vector_index_type == "BRUTE_FORCE") {
    creator->SetBruteForceParam(GenBruteForceParam());
  } else if (vector_index_type == "FLAT") {
    creator->SetFlatParam(GenFlatParam());
  } else if (vector_index_type == "IVF_FLAT") {
    creator->SetIvfFlatParam(GenIvfFlatParam());
  } else if (vector_index_type == "IVF_PQ") {
    creator->SetIvfPqParam(GenIvfPqParam());
  } else if (vector_index_type == "DISKANN") {
    creator->SetDiskAnnParam(GenDiskANNParam());
  } else {
    LOG(ERROR) << fmt::format("Not support vector index type {}", vector_index_type);
    return 0;
  }

  if (!FLAGS_filter_field.empty()) {
    sdk::VectorScalarSchema scalar_schema;
    auto filter_fields = ParseFilterField(FLAGS_filter_field);
    for (auto& filter_field : filter_fields) {
      const auto& field_name = filter_field[0];
      const std::string& filed_type = filter_field[1];

      if (filed_type == "int" || filed_type == "int32" || filed_type == "int64" || filed_type == "uint" ||
          filed_type == "uint32" || filed_type == "uint64") {
        sdk::VectorScalarColumnSchema column_schema(field_name, sdk::Type::kINT64, true);
        scalar_schema.cols.push_back(column_schema);

      } else if (filed_type == "string") {
        sdk::VectorScalarColumnSchema column_schema(field_name, sdk::Type::kSTRING, true);
        scalar_schema.cols.push_back(column_schema);
      }
    }

    if (!scalar_schema.cols.empty()) {
      creator->SetScalarSchema(scalar_schema);
    }
  }

  status = creator->Create(vector_index_id);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("create vector index failed, {}", status.ToString());
    return 0;
  }
  if (vector_index_id == 0) {
    LOG(ERROR) << "vector_index_id is 0, invalid";
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));

  // auto balance region
  if (FLAGS_auto_balance_region && FLAGS_replica > 1) {
    std::cout << fmt::format("enable auto balance region for vector index {}", vector_index_id) << std::endl;
    LOG(INFO) << fmt::format("enable auto balance region for vector index {}", vector_index_id);
    AutoBalanceRegion(vector_index_id);
  }

  return vector_index_id;
}

void Benchmark::DropVectorIndex(int64_t vector_index_id) {
  CHECK(vector_index_id != 0) << "vector_index_id is invalid";
  auto status = client_->DropVectorIndexById(vector_index_id);
  CHECK(status.IsOK()) << fmt::format("drop vector index failed, {}", status.ToString());
}

int64_t Benchmark::GetVectorIndex(const std::string& name) {
  int64_t vector_index_id = 0;
  auto status = client_->GetVectorIndexId(pb::meta::ReservedSchemaIds::DINGO_SCHEMA, name, vector_index_id);
  CHECK(status.ok()) << fmt::format("get vector index failed, {}", status.ToString());

  return vector_index_id;
}

static std::vector<std::string> ExtractPrefixs(const std::vector<RegionEntryPtr>& region_entries) {
  std::vector<std::string> prefixes;
  prefixes.reserve(region_entries.size());
  for (const auto& region_entry : region_entries) {
    prefixes.push_back(region_entry->prefix);
  }

  return prefixes;
}

void Benchmark::ThreadRoutine(ThreadEntryPtr thread_entry) {
  // Set signal
  sigset_t sig_set;
  if (sigemptyset(&sig_set) || sigaddset(&sig_set, SIGINT) || pthread_sigmask(SIG_BLOCK, &sig_set, nullptr)) {
    std::cerr << "Cannot block signal" << '\n';
    exit(1);
  }

  if (IsTransactionBenchmark()) {
    if (FLAGS_is_single_region_txn) {
      ExecutePerRegion(thread_entry);
    } else {
      ExecuteMultiRegion(thread_entry);
    }

  } else if (IsVectorBenchmark()) {
    ExecutePerVectorIndex(thread_entry);
  } else {
    ExecutePerRegion(thread_entry);
  }

  thread_entry->is_stop.store(true, std::memory_order_relaxed);
}

void Benchmark::ExecutePerRegion(ThreadEntryPtr thread_entry) {
  auto region_entries = thread_entry->region_entries;

  int64_t req_num_per_thread = static_cast<int64_t>(FLAGS_req_num / (FLAGS_concurrency * FLAGS_region_num));
  for (int64_t i = 0; i < req_num_per_thread; ++i) {
    if (thread_entry->is_stop.load(std::memory_order_relaxed)) {
      break;
    }

    for (const auto& region_entry : region_entries) {
      size_t eplased_time;
      auto result = operation_->Execute(region_entry);
      {
        std::lock_guard lock(mutex_);
        if (result.status.ok()) {
          stats_interval_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
          stats_cumulative_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
        } else {
          stats_interval_->AddError();
          stats_cumulative_->AddError();
        }
      }
    }
  }
}

void Benchmark::ExecuteMultiRegion(ThreadEntryPtr thread_entry) {
  auto region_entries = thread_entry->region_entries;

  int64_t req_num_per_thread = static_cast<int64_t>(FLAGS_req_num / FLAGS_concurrency);

  for (int64_t i = 0; i < req_num_per_thread; ++i) {
    if (thread_entry->is_stop.load(std::memory_order_relaxed)) {
      break;
    }

    auto result = operation_->Execute(region_entries);
    {
      std::lock_guard lock(mutex_);
      if (result.status.ok()) {
        stats_interval_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
        stats_cumulative_->Add(result.eplased_time, result.write_bytes, result.read_bytes);
      } else {
        stats_interval_->AddError();
        stats_cumulative_->AddError();
      }
    }
  }
}

void Benchmark::ExecutePerVectorIndex(ThreadEntryPtr thread_entry) {
  auto vector_index_entries = thread_entry->vector_index_entries;

  int64_t req_num_per_thread = static_cast<int64_t>(FLAGS_req_num / (FLAGS_concurrency * FLAGS_vector_index_num));
  for (int64_t i = 0; i < req_num_per_thread; ++i) {
    if (thread_entry->is_stop.load(std::memory_order_relaxed)) {
      break;
    }

    for (const auto& vector_index_entry : vector_index_entries) {
      size_t eplased_time;
      auto result = operation_->Execute(vector_index_entry);
      {
        std::lock_guard lock(mutex_);
        if (result.status.ok()) {
          stats_interval_->Add(result.eplased_time, result.write_bytes, result.read_bytes, result.recalls);
          stats_cumulative_->Add(result.eplased_time, result.write_bytes, result.read_bytes, result.recalls);
        } else {
          stats_interval_->AddError();
          stats_cumulative_->AddError();
        }
      }
    }
  }
}

void Benchmark::IntervalReport() {
  size_t delay_ms = FLAGS_delay * 1000;
  size_t start_time = dingodb::benchmark::TimestampMs();
  size_t cumulative_start_time = dingodb::benchmark::TimestampMs();

  for (;;) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    size_t milliseconds = dingodb::benchmark::TimestampMs() - start_time;
    if (milliseconds > delay_ms) {
      Report(false, milliseconds);
      start_time = dingodb::benchmark::TimestampMs();
    }

    // Check time limit
    if (FLAGS_timelimit > 0 && dingodb::benchmark::TimestampMs() - cumulative_start_time > FLAGS_timelimit * 1000) {
      Stop();
    }

    if (IsStop()) {
      break;
    }
  }
}

void Benchmark::Report(bool is_cumulative, size_t milliseconds) {
  std::map<std::int64_t, sdk::StoreOwnMetics> store_id_to_store_own_metrics;
  if (FLAGS_enable_monitor_vector_performance_info) {
    // monitor cpu memory disk usage
    std::vector<int64_t> store_ids;
    store_ids.push_back(FLAGS_index_store_id_1);
    store_ids.push_back(FLAGS_index_store_id_2);
    store_ids.push_back(FLAGS_index_store_id_3);

    sdk::Status s = client_->GetStoreOwnMetrics(store_ids, store_id_to_store_own_metrics);
    if (!s.ok()) {
      std::cerr << fmt::format("Get store own metrics failed, status: {}", s.ToString()) << '\n';
      store_id_to_store_own_metrics.clear();
      store_id_to_store_own_metrics[FLAGS_index_store_id_1] = sdk::StoreOwnMetics();
      store_id_to_store_own_metrics[FLAGS_index_store_id_2] = sdk::StoreOwnMetics();
      store_id_to_store_own_metrics[FLAGS_index_store_id_3] = sdk::StoreOwnMetics();
    }
  }

  std::lock_guard lock(mutex_);

  if (is_cumulative) {
    stats_cumulative_->Report(true, milliseconds, store_id_to_store_own_metrics);
    stats_interval_->Clear();
  } else {
    stats_interval_->Report(false, milliseconds, store_id_to_store_own_metrics);
    stats_interval_->Clear();
  }
}

void Benchmark::AutoBalanceRegion(int64_t vector_index_id) {
  std::shared_ptr<sdk::VectorIndex> out_vector_index;
  sdk::Status status;
  status = client_->GetVectorIndexById(vector_index_id, out_vector_index);
  CHECK(status.ok()) << fmt::format("Get vector index failed, status: {}", status.ToString());

  // collect all partition ranges
  std::vector<pb::common::Range> ranges;
  for (const auto& part_id : out_vector_index->GetPartitionIds()) {
    pb::common::Range range = out_vector_index->GetPartitionRange(part_id);
    ranges.emplace_back(std::move(range));
  }

  // collect all region ids
  std::vector<int64_t> region_ids;
  for (const auto& range : ranges) {
    std::vector<int64_t> single_region_ids;
    status = client_->ScanRegions(range.start_key(), range.end_key(), 0, single_region_ids);
    CHECK(status.ok()) << fmt::format("Scan regions failed, status: {}", status.ToString());
    region_ids.insert(region_ids.end(), single_region_ids.begin(), single_region_ids.end());
  }

  if (region_ids.empty()) {
    LOG(ERROR) << fmt::format("No regions found for vector index {}", vector_index_id);
    return;
  }

  // get region map
  std::shared_ptr<dingodb::pb::common::RegionMap> regionmap;
  int64_t tenant_id = -1;
  status = client_->GetRegionMap(tenant_id, regionmap);
  CHECK(status.ok()) << fmt::format("Get region map failed, status: {}", status.ToString());

  // get store map
  dingodb::pb::common::StoreType store_type = dingodb::pb::common::StoreType::NODE_TYPE_INDEX;
  dingodb::pb::common::StoreMap storemap;
  status = client_->GetStoreMap({store_type}, storemap);
  CHECK(status.ok()) << fmt::format("get store map failed, status: {}", status.ToString());

  std::vector<int64_t> store_ids;
  for (const auto& store : storemap.stores()) {
    store_ids.push_back(store.id());
  }

  if (store_ids.empty()) {
    LOG(ERROR) << fmt::format("No stores found for vector index {}", vector_index_id);
    return;
  }

  // sort store ids
  std::sort(store_ids.begin(), store_ids.end());

  std::string store_ids_str = fmt::format("store ids ({}): ", store_ids.size());

  for (const auto store_id : store_ids) {
    store_ids_str += std::to_string(store_id) + " ";
  }

  std::cout << store_ids_str << std::endl;
  LOG(INFO) << store_ids_str;

  // sort region ids
  std::sort(region_ids.begin(), region_ids.end());
  std::string region_ids_str = fmt::format("region ids ({}): ", region_ids.size());
  for (const auto region_id : region_ids) {
    region_ids_str += std::to_string(region_id) + " ";
  }
  std::cout << region_ids_str << std::endl;
  LOG(INFO) << region_ids_str;

  // allocate region ids to store ids in a round-robin manner
  std::map<int64_t, int64_t> region_id_to_store_id_map;

  int64_t i = 0;
  for (const auto region_id : region_ids) {
    auto store_id = store_ids[i];
    // map region id to store id
    region_id_to_store_id_map[region_id] = store_id;
    i = (++i) % store_ids.size();  // round-robin assign store ids
  }

  std::string region_id_to_store_id_map_str =
      fmt::format("region_id_to_store_id_map_str ({}): ", region_id_to_store_id_map.size());
  for (const auto& [region_id, store_id] : region_id_to_store_id_map) {
    region_id_to_store_id_map_str += fmt::format("{{{}: {}}} ", region_id, store_id);
  }
  std::cout << region_id_to_store_id_map_str << std::endl;
  LOG(INFO) << region_id_to_store_id_map_str;

  for (const auto& [region_id, store_id] : region_id_to_store_id_map) {
    auto iter = regionmap->regions().begin();
    for (; iter != regionmap->regions().end(); ++iter) {
      if (iter->id() == region_id) {
        break;
      }
    }

    CHECK(iter != regionmap->regions().end()) << fmt::format("Region id {} not found in region map", region_id);
    auto leader_store_id = iter->leader_store_id();
    if (store_id != leader_store_id) {
      // change leader store id
      status = client_->TransferLeaderRegion(region_id, store_id, true);
      CHECK(status.ok()) << fmt::format("Change region {} leader failed, status: {}", region_id, status.ToString());
    }
  }

  std::this_thread::sleep_for(std::chrono::seconds(10));
  
  // double check get region map again to ensure the changes are applied
  status = client_->GetRegionMap(tenant_id, regionmap);
  CHECK(status.ok()) << fmt::format("Get region map failed, status: {}", status.ToString());

  for (const auto& [region_id, store_id] : region_id_to_store_id_map) {
    auto iter = regionmap->regions().begin();
    for (; iter != regionmap->regions().end(); ++iter) {
      if (iter->id() == region_id) {
        break;
      }
    }

    CHECK(iter != regionmap->regions().end()) << fmt::format("Region id {} not found in region map", region_id);
    auto leader_store_id = iter->leader_store_id();

    CHECK(store_id == leader_store_id) << fmt::format("Region {} leader store id is {}, but expected {}", region_id,
                                                      leader_store_id, store_id);
  }
}

Environment& Environment::GetInstance() {
  static Environment instance;
  return instance;
}

bool Environment::Init() {
  if (!IsSupportBenchmarkType(FLAGS_benchmark)) {
    std::cerr << fmt::format("Not support benchmark {}, just support: {}", FLAGS_benchmark, GetSupportBenchmarkType())
              << '\n';
    return false;
  }

  DINGO_LOG(INFO) << "using coordinator_addrs: " << FLAGS_coordinator_addrs;

  std::vector<sdk::EndPoint> endpoints = sdk::IsServiceUrlValid(FLAGS_coordinator_addrs)
                                             ? sdk::FileNamingServiceUrlEndpoints(FLAGS_coordinator_addrs)
                                             : sdk::StringToEndpoints(FLAGS_coordinator_addrs);
  if (endpoints.empty()) {
    std::cerr << fmt::format("coordinator_addrs({}) is invalid, maybe coor_list not exist!", FLAGS_coordinator_addrs)
              << std::endl;
    return false;
  }

  client_stub_ = std::make_shared<dingodb::sdk::ClientStub>();
  auto s = client_stub_->Open(endpoints);
  CHECK(s.ok()) << "Open client stub failed, please check parameter --coordinator_addrs="
                << sdk::EndPointToString(endpoints);

  sdk::Client* tmp;
  auto status = sdk::Client::BuildFromEndPoint(endpoints, &tmp);
  CHECK(status.ok()) << fmt::format("Build sdk client failed, error: {}", status.ToString());
  client_.reset(tmp);

  PrintParam();

  if (FLAGS_show_version) {
    PrintVersionInfo();
  }

  return true;
}

void Environment::AddBenchmark(BenchmarkPtr benchmark) { benchmarks_.push_back(benchmark); }

void Environment::Stop() {
  for (auto& benchmark : benchmarks_) {
    benchmark->Stop();
  }
}

void Environment::PrintVersionInfo() {
  sdk::coordinator::HelloRpc rpc;
  rpc.MutableRequest()->set_is_just_version_info(true);

  LOG(INFO) << "Hello request: " << rpc.Request()->ShortDebugString();

  auto status = client_stub_->GetCoordinatorRpcController()->SyncCall(rpc);
  CHECK(status.ok()) << fmt::format("Hello failed, status: {}", status.ToString());

  const auto& version_info = rpc.Response()->version_info();

  std::cout << COLOR_GREEN << "Version(dingo-store):" << COLOR_RESET << '\n';

  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_hash", version_info.git_commit_hash()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_tag_name", version_info.git_tag_name()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_user", version_info.git_commit_user()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_mail", version_info.git_commit_mail()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "git_commit_time", version_info.git_commit_time()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "major_version", version_info.major_version()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "minor_version", version_info.minor_version()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "dingo_build_type", version_info.dingo_build_type()) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "dingo_contrib_build_type", version_info.dingo_contrib_build_type())
            << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_mkl", (version_info.use_mkl() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_openblas", (version_info.use_openblas() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_tcmalloc", (version_info.use_tcmalloc() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_profiler", (version_info.use_profiler() ? "true" : "false")) << '\n';
  std::cout << fmt::format("{:<24}: {:>64}", "use_sanitizer", (version_info.use_sanitizer() ? "true" : "false"))
            << '\n';

  std::cout << '\n';
}

void Environment::PrintParam() {
  std::cout << COLOR_GREEN << "Parameter:" << COLOR_RESET << '\n';

  std::cout << fmt::format("{:<34}: {:>32}", "benchmark", FLAGS_benchmark) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "region_num", FLAGS_region_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "prefix", FLAGS_prefix) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "raw_engine", FLAGS_raw_engine) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "region_num", FLAGS_region_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_index_num", FLAGS_vector_index_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "is_clean_region", FLAGS_is_clean_region ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "concurrency", FLAGS_concurrency) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "req_num", FLAGS_req_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "delay(s)", FLAGS_delay) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "timelimit(s)", FLAGS_timelimit) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "key_size(byte)", FLAGS_key_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "value_size(byte)", FLAGS_value_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "batch_size", FLAGS_batch_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "is_single_region_txn", FLAGS_is_single_region_txn ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "is_pessimistic_txn", FLAGS_is_pessimistic_txn ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "txn_isolation_level", FLAGS_vector_search_topk) << '\n';

  std::cout << fmt::format("{:<34}: {:>32}", "vector_dimension", FLAGS_vector_dimension) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_value_type", FLAGS_vector_value_type) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_max_element_num", FLAGS_vector_max_element_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_metric_type", FLAGS_vector_metric_type) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_partition_vector_ids", FLAGS_vector_partition_vector_ids) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_dataset", FLAGS_vector_dataset) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_arrange_concurrency", FLAGS_vector_arrange_concurrency) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_put_batch_size", FLAGS_vector_put_batch_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "hnsw_ef_construction", FLAGS_hnsw_ef_construction) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "hnsw_nlink_num", FLAGS_hnsw_nlink_num) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_ncentroids", FLAGS_ivf_ncentroids) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_nsubvector", FLAGS_ivf_nsubvector) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_bucket_init_size", FLAGS_ivf_bucket_init_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_bucket_max_size", FLAGS_ivf_bucket_max_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "ivf_nbits_per_idx", FLAGS_ivf_nbits_per_idx) << '\n';

  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_topk", FLAGS_vector_search_topk) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "with_vector_data", FLAGS_with_vector_data ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "with_scalar_data", FLAGS_with_scalar_data ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "with_table_data", FLAGS_with_table_data ? "true" : "false") << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_use_brute_force",
                           FLAGS_vector_search_use_brute_force ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_enable_range_search",
                           FLAGS_vector_search_enable_range_search ? "true" : "false")
            << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_radius", FLAGS_vector_search_radius) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_nprobe", FLAGS_vector_search_nprobe) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "vector_search_ef", FLAGS_vector_search_ef) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "diskann_max_degree", FLAGS_diskann_max_degree) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "diskann_search_list_size", FLAGS_diskann_search_list_size) << '\n';
  std::cout << fmt::format("{:<34}: {:>32}", "diskann_search_beamwidth", FLAGS_diskann_search_beamwidth) << '\n';
  std::cout << '\n';
}

}  // namespace benchmark
}  // namespace dingodb
