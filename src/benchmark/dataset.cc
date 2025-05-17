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

#include "benchmark/dataset.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "H5Cpp.h"
#include "H5PredType.h"
#include "common/logging.h"
#include "dingosdk/vector.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "util.h"

DECLARE_uint32(vector_put_batch_size);
DECLARE_uint32(vector_search_topk);
DECLARE_uint32(vector_dimension);
DECLARE_bool(vector_search_arrange_data);

DEFINE_uint32(load_vector_dataset_concurrency, 4, "load vector dataset concurrency");

DEFINE_uint32(batch_vector_entry_cache_size, 1024, "batch vector entry cache");

DEFINE_string(vector_search_filter, "", "vector search filter,e.g. key=value;key=value");

DEFINE_int64(arrange_data_start_offset, 0, "arrange data start offset");

DECLARE_string(vector_search_filter_source);

DECLARE_string(vector_search_scalar_filter_radius);

namespace dingodb {
namespace benchmark {

std::shared_ptr<Dataset> Dataset::New(std::string filepath) {
  if (filepath.find("sift") != std::string::npos) {
    return std::make_shared<SiftDataset>(filepath);

  } else if (filepath.find("glove") != std::string::npos) {
    return std::make_shared<GloveDataset>(filepath);

  } else if (filepath.find("gist") != std::string::npos) {
    return std::make_shared<GistDataset>(filepath);

  } else if (filepath.find("kosarak") != std::string::npos) {
  } else if (filepath.find("lastfm") != std::string::npos) {
  } else if (filepath.find("mnist") != std::string::npos) {
    return std::make_shared<MnistDataset>(filepath);

  } else if (filepath.find("movielens10m") != std::string::npos) {
  } else if (filepath.find("wikipedia") != std::string::npos) {
    return std::make_shared<Wikipedia2212Dataset>(filepath);
  } else if (filepath.find("beir-bioasq") != std::string::npos) {
    return std::make_shared<BeirBioasqDataset>(filepath);
  } else if (filepath.find("miracl") != std::string::npos) {
    return std::make_shared<MiraclDataset>(filepath);
  } else if (filepath.find("laion") != std::string::npos) {
    return std::make_shared<LaionDataset>(filepath);
  } else if (filepath.find("bioasq_medium") != std::string::npos) {
    return std::make_shared<BioasqMediumDataset>(filepath);
  } else if (filepath.find("openai_large") != std::string::npos) {
    return std::make_shared<OpenaiLargeDataset>(filepath);
  }

  std::cout << "Not support dataset, path: " << filepath << std::endl;

  return nullptr;
}

BaseDataset::BaseDataset(std::string filepath) : filepath_(filepath) {}

BaseDataset::~BaseDataset() { h5file_->close(); }

bool BaseDataset::Init() {
  std::lock_guard lock(mutex_);

  try {
    h5file_ = std::make_shared<H5::H5File>(filepath_, H5F_ACC_RDONLY);
    {
      H5::DataSet dataset = h5file_->openDataSet("train");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      train_row_count_ = dims_out[0];
      dimension_ = dims_out[1];
      std::cout << fmt::format("dataset train data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    {
      H5::DataSet dataset = h5file_->openDataSet("test");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      test_row_count_ = dims_out[0];
      std::cout << fmt::format("dataset test data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    {
      H5::DataSet dataset = h5file_->openDataSet("neighbors");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      std::cout << fmt::format("dataset neighbors data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    {
      H5::DataSet dataset = h5file_->openDataSet("distances");
      H5::DataSpace dataspace = dataset.getSpace();

      hsize_t dims_out[2] = {0};
      dataspace.getSimpleExtentDims(dims_out, nullptr);
      std::cout << fmt::format("dataset distances data_type({}) rank({}) dimensions({}x{})",
                               static_cast<int>(dataset.getTypeClass()), dataspace.getSimpleExtentNdims(), dims_out[0],
                               dims_out[1])
                << std::endl;
    }

    return true;
  } catch (H5::FileIException& error) {
    error.printErrorStack();
  } catch (H5::DataSetIException& error) {
    error.printErrorStack();
  } catch (H5::DataSpaceIException& error) {
    error.printErrorStack();
  } catch (H5::DataTypeIException& error) {
    error.printErrorStack();
  } catch (std::exception& e) {
    std::cerr << "dataset init failed, " << e.what();
  }

  return false;
}

uint32_t BaseDataset::GetDimension() const { return dimension_; }
uint32_t BaseDataset::GetTrainDataCount() const { return train_row_count_; }
uint32_t BaseDataset::GetTestDataCount() const { return test_row_count_; }

void BaseDataset::GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) {
  std::lock_guard lock(mutex_);

  uint64_t start_time = dingodb::benchmark::TimestampUs();

  is_eof = false;
  H5::DataSet dataset = h5file_->openDataSet("train");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];
  uint32_t row_offset = batch_num * FLAGS_vector_put_batch_size;
  if (row_offset >= row_count) {
    dataspace.close();
    dataset.close();
    is_eof = true;
    return;
  }

  uint32_t batch_size =
      (row_offset + FLAGS_vector_put_batch_size) <= row_count ? FLAGS_vector_put_batch_size : (row_count - row_offset);
  is_eof = batch_size < FLAGS_vector_put_batch_size;

  hsize_t file_offset[2] = {row_offset, 0};
  hsize_t file_count[2] = {batch_size, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {batch_size, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<float> buf;
  buf.resize(batch_size * dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);

  // generate VectorWithId
  for (int i = 0; i < batch_size; ++i) {
    sdk::VectorWithId vector_with_id;
    vector_with_id.id = row_offset + i + 1;
    vector_with_id.vector.dimension = dimension;
    vector_with_id.vector.value_type = sdk::ValueType::kFloat;

    std::vector<float> vec;
    vec.resize(dimension);
    memcpy(vec.data(), buf.data() + i * dimension, dimension * sizeof(float));
    vector_with_id.vector.float_values.swap(vec);

    vector_with_ids.push_back(std::move(vector_with_id));
  }

  memspace.close();
  dataspace.close();
  dataset.close();

  std::cout << fmt::format("elapsed time: {} us", dingodb::benchmark::TimestampUs() - start_time) << std::endl;
}

template <typename T>
static void PrintVector(const std::vector<T>& vec) {
  for (const auto& v : vec) {
    std::cout << v << " ";
  }

  std::cout << std::endl;
}

std::vector<BaseDataset::TestEntryPtr> BaseDataset::GetTestData() {
  std::lock_guard lock(mutex_);

  H5::DataSet dataset = h5file_->openDataSet("test");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];

  hsize_t file_offset[2] = {0, 0};
  hsize_t file_count[2] = {row_count, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {row_count, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<float> buf;
  buf.resize(row_count * dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);

  // gernerate test entries
  std::vector<BaseDataset::TestEntryPtr> test_entries;
  for (int i = 0; i < row_count; ++i) {
    auto test_entry = std::make_shared<BaseDataset::TestEntry>();
    test_entry->vector_with_id.id = 0;
    test_entry->vector_with_id.vector.dimension = dimension;
    test_entry->vector_with_id.vector.value_type = sdk::ValueType::kFloat;

    std::vector<float> vec;
    vec.resize(dimension);
    memcpy(vec.data(), buf.data() + i * dimension, dimension * sizeof(float));
    test_entry->vector_with_id.vector.float_values.swap(vec);

    test_entry->neighbors = GetTestVectorNeighbors(i);

    test_entries.push_back(test_entry);
  }

  memspace.close();
  dataspace.close();
  dataset.close();

  return test_entries;
}

std::unordered_map<int64_t, float> BaseDataset::GetTestVectorNeighbors(uint32_t index) {
  auto vector_ids = GetNeighbors(index);
  auto distances = GetDistances(index);
  assert(vector_ids.size() == distances.size());

  uint32_t size = std::min(static_cast<uint32_t>(vector_ids.size()), FLAGS_vector_search_topk);
  std::unordered_map<int64_t, float> neighbors;
  for (uint32_t i = 0; i < size; ++i) {
    neighbors.insert(std::make_pair(vector_ids[i] + 1, distances[i]));
  }

  return neighbors;
}

std::vector<int> BaseDataset::GetNeighbors(uint32_t index) {
  H5::DataSet dataset = h5file_->openDataSet("neighbors");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];

  hsize_t file_offset[2] = {index, 0};
  hsize_t file_count[2] = {1, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {1, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<int> buf;
  buf.resize(dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_INT32, memspace, dataspace);

  memspace.close();
  dataspace.close();
  dataset.close();

  return buf;
}

std::vector<float> BaseDataset::GetDistances(uint32_t index) {
  H5::DataSet dataset = h5file_->openDataSet("distances");
  H5::DataSpace dataspace = dataset.getSpace();

  int rank = dataspace.getSimpleExtentNdims();
  hsize_t dims_out[2] = {0};
  dataspace.getSimpleExtentDims(dims_out, nullptr);
  uint32_t row_count = dims_out[0];
  uint32_t dimension = dims_out[1];

  hsize_t file_offset[2] = {index, 0};
  hsize_t file_count[2] = {1, dimension};
  dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

  H5::DataSpace memspace(rank, dims_out);
  hsize_t mem_offset[2] = {0, 0};
  hsize_t mem_count[2] = {1, dimension};
  memspace.selectHyperslab(H5S_SELECT_SET, mem_count, mem_offset);

  std::vector<float> buf;
  buf.resize(dimension);
  dataset.read(buf.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);

  memspace.close();
  dataspace.close();
  dataset.close();

  return buf;
}

bool JsonDataset::Init() {
  std::lock_guard lock(mutex_);

  // find scalar_labels.json and  neighbors_labels_label*.json
  // if enable  scalar filter
  if (!FLAGS_vector_search_filter.empty()) {
    if (!HandleScalarAndNeighborsJson()) {
      LOG(ERROR) << "use vector_search_filter, but scalar_labels.json or neighbors_labels_label*.json not exist";
      return false;
    }
  }

  auto lambda_ends_with_function = [](const std::string& str, const std::string& suffix) {
    if (suffix.size() > str.size()) return false;
    return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
  };

  if (FLAGS_vector_search_arrange_data) {
    auto train_filenames = dingodb::benchmark::TraverseDirectory(dirpath_, std::string("train"));
    for (auto& filename : train_filenames) {
      if (lambda_ends_with_function(filename, ".json")) {
        train_filepaths_.push_back(fmt::format("{}/{}", dirpath_, filename));
      } else {
        LOG(WARNING) << "ignore train file. not .json end_with " << filename;
      }
    }

    if (train_filepaths_.empty()) {
      LOG(ERROR) << "train file not exist, please check";
      return false;
    }

    batch_vector_entry_cache_.resize(FLAGS_batch_vector_entry_cache_size);
    auto self = GetSelf();
    train_thread_ = std::thread([self] { self->ParallelLoadTrainData(self->train_filepaths_); });
    train_thread_.detach();
  }

  auto test_filenames = dingodb::benchmark::TraverseDirectory(dirpath_, std::string("test"));
  for (auto& filename : test_filenames) {
    if (lambda_ends_with_function(filename, ".json")) {
      test_filepaths_.push_back(fmt::format("{}/{}", dirpath_, filename));
    } else {
      LOG(WARNING) << "ignore test file. not .json end_with " << filename;
    }
  }

  if (test_filepaths_.empty()) {
    LOG(ERROR) << "test file not exist, please check";
    return false;
  }

  return !test_filepaths_.empty();
}

uint32_t JsonDataset::GetDimension() const { return FLAGS_vector_dimension; }

uint32_t JsonDataset::GetTrainDataCount() const { return 0; }

uint32_t JsonDataset::GetTestDataCount() const { return test_row_count_; }

void JsonDataset::ParallelLoadTrainData(const std::vector<std::string>& filepaths) {
  uint64_t start_time = dingodb::benchmark::TimestampMs();

  std::atomic_int curr_file_pos = 0;
  std::vector<std::thread> threads;
  for (size_t thread_id = 0; thread_id < FLAGS_load_vector_dataset_concurrency; ++thread_id) {
    auto self = GetSelf();
    threads.push_back(std::thread([self, &curr_file_pos, filepaths, thread_id] {
      for (;;) {
        int file_pos = curr_file_pos.fetch_add(1);
        if (file_pos >= filepaths.size()) {
          return;
        }

        std::ifstream ifs(filepaths[file_pos]);
        rapidjson::IStreamWrapper isw(ifs);
        auto doc = std::make_shared<rapidjson::Document>();
        doc->ParseStream(isw);
        LOG_IF(ERROR, doc->HasParseError()) << fmt::format("parse json file {} failed, error: {}", filepaths[file_pos],
                                                           static_cast<int>(doc->GetParseError()));

        uint32_t offset = 0;
        int64_t batch_vector_count = 0;
        int64_t vector_count = 0;
        for (;;) {
          ++batch_vector_count;
          auto batch_vector_entry = std::make_shared<BatchVectorEntry>();
          batch_vector_entry->vector_with_ids.reserve(FLAGS_vector_put_batch_size);
          offset = self->LoadTrainData(doc, offset, FLAGS_vector_put_batch_size, batch_vector_entry->vector_with_ids);
          vector_count += batch_vector_entry->vector_with_ids.size();
          if (batch_vector_entry->vector_with_ids.empty()) {
            break;
          }

          for (;;) {
            bool is_full = false;
            {
              std::lock_guard lock(self->mutex_);
              // check is full
              if ((self->head_pos_ + 1) % FLAGS_batch_vector_entry_cache_size == self->tail_pos_) {
                is_full = true;
              } else {
                self->batch_vector_entry_cache_[self->tail_pos_] = batch_vector_entry;
                self->tail_pos_ = (self->tail_pos_ + 1) % FLAGS_batch_vector_entry_cache_size;
                break;
              }
            }

            if (is_full) {
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
          }
        }

        LOG(INFO) << fmt::format("filepath: {} file_pos: {} batch_vector_count: {} vector_count: {}",
                                 filepaths[file_pos], file_pos, batch_vector_count, vector_count);
      }
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  train_load_finish_.store(true);

  LOG(INFO) << fmt::format("Parallel load train data elapsed time: {} ms",
                           dingodb::benchmark::TimestampMs() - start_time);
}

uint32_t JsonDataset::LoadTrainData(std::shared_ptr<rapidjson::Document> doc, uint32_t offset, uint32_t size,
                                    std::vector<sdk::VectorWithId>& vector_with_ids) const {
  CHECK(doc != nullptr);
  CHECK(doc->IsArray());

  const auto& array = doc->GetArray();
  while (offset < array.Size()) {
    sdk::VectorWithId vector_with_id;
    if (!ParseTrainData(array[offset++], vector_with_id)) {
      continue;
    }

    vector_with_ids.push_back(vector_with_id);
    if (vector_with_ids.size() >= size) {
      break;
    }
  }

  return offset;
}

bool JsonDataset::HandleScalarAndNeighborsJson() {
  // find scalar_labels.json and  neighbors_labels_label*.json
  // if enable  scalar filter
  std::string type = GetType();

  if ((type == "BioasqMediumDataset" || type == "OpenaiLargeDataset")) {
    std::vector<std::string> scalar_labels =
        dingodb::benchmark::TraverseDirectory(dirpath_, std::string("scalar_labels"));

    if (scalar_labels.empty()) {
      LOG(ERROR) << "use vector_search_filter, but type : " << type << " scalar_labels.json not exist";
      return false;
    }

    std::string target_scalar_labels;

    for (const auto& scalar_label : scalar_labels) {
      if (scalar_label.find("scalar_labels.json") != std::string::npos) {
        target_scalar_labels = scalar_label;
        break;
      }
    }

    if (target_scalar_labels.empty()) {
      LOG(ERROR) << "use vector_search_filter, but type : " << type << " scalar_labels.json not exist";
      return false;
    }

    if (!ParseScalarLabelsJson(target_scalar_labels)) {
      LOG(ERROR) << "parse scalar_labels.json failed";
      return false;
    }

    std::cout << "scalar_labels file : " << target_scalar_labels << std::endl;
    LOG(INFO) << "scalar_labels file : " << target_scalar_labels;

    std::cout << "scalar_labels size : " << scalar_labels_map->size() << std::endl;
    LOG(INFO) << "scalar_labels size : " << scalar_labels_map->size();

    std::vector<std::string> neighbors_labels =
        dingodb::benchmark::TraverseDirectory(dirpath_, std::string("neighbors_labels_label"));
    if ((type == "BioasqMediumDataset" || type == "OpenaiLargeDataset") && neighbors_labels.empty()) {
      LOG(ERROR) << "use vector_search_filter, but type : " << type << " neighbors_labels not exist";
      return false;
    }

    if (neighbors_labels.empty()) {
      LOG(ERROR) << "use vector_search_filter, but type : " << type << " neighbors_labels_label*.json not exist";
      return false;
    }

    // find assign neighbors_labels_label
    std::string target_neighbors_label;
    std::string part_file = "neighbors_labels_label_" + FLAGS_vector_search_scalar_filter_radius + "p" + ".json";

    std::cout << "vector_search_scalar_filter_radius file : " << part_file << std::endl;
    LOG(INFO) << "vector_search_scalar_filter_radius file : " << part_file;

    std::cout << "vector_search_scalar_filter_radius : " << FLAGS_vector_search_scalar_filter_radius << std::endl;
    LOG(INFO) << "vector_search_scalar_filter_radius : " << FLAGS_vector_search_scalar_filter_radius;

    for (const auto& neighbors_label : neighbors_labels) {
      if (neighbors_label.find(part_file) != std::string::npos) {
        target_neighbors_label = neighbors_label;
        break;
      }
    }

    if (target_neighbors_label.empty()) {
      LOG(ERROR) << "neighbors_labels*.json not exist";
      return false;
    }

    if (!ParseNeighborsLabelsJson(target_neighbors_label)) {
      LOG(ERROR) << "parse neighbors_labels.json failed";
      return false;
    }

    std::cout << "neighbors_id size : " << neighbors_id_map->size() << std::endl;
    LOG(INFO) << "neighbors_id size : " << neighbors_id_map->size();

    // double check
    if (scalar_labels_map->empty()) {
      LOG(ERROR) << "scalar_labels_map is empty";
      return false;
    }

    if (neighbors_id_map->empty()) {
      LOG(ERROR) << "neighbors_id_map is empty";
      return false;
    }

    std::cout << "neighbors label : " << "label_" + FLAGS_vector_search_scalar_filter_radius + "p" << std::endl;
    LOG(INFO) << "neighbors label : " << "label_" + FLAGS_vector_search_scalar_filter_radius + "p";

  }  //  if ((type == "BioasqMediumDataset" || type == "OpenaiLargeDataset")) {

  return true;
}

bool JsonDataset::ParseScalarLabelsJson(const std::string& json_file) {
  std::ifstream ifs(fmt::format("{}/{}", dirpath_, json_file));
  rapidjson::IStreamWrapper isw(ifs);
  auto doc = std::make_shared<rapidjson::Document>();
  doc->ParseStream(isw);
  LOG_IF(ERROR, doc->HasParseError()) << fmt::format("parse json file {} failed, error: {}", json_file,
                                                     static_cast<int>(doc->GetParseError()));

  scalar_labels_map = std::make_shared<std::unordered_map<int64_t, std::string>>();

  const auto& array = doc->GetArray();
  for (size_t i = 0; i < array.Size(); ++i) {
    auto& obj = array[i];
    if (!obj.IsObject()) {
      LOG(ERROR) << "scalar_labels.json is not object";
      return false;
    }
    if (!obj.HasMember("id") || !obj.HasMember("labels")) {
      LOG(ERROR) << "scalar_labels.json id or labels not exist";
      return false;
    }

    int64_t id = obj["id"].GetInt64();
    std::string labels = obj["labels"].GetString();
    if (id < 0 || labels.empty()) {
      LOG(ERROR) << fmt ::format("scalar_labels.json id:{} < 0 or value:{} is empty", id, labels);
      return false;
    }
    scalar_labels_map->insert(std::make_pair(id, labels));
  }

  return true;
}

bool JsonDataset::ParseNeighborsLabelsJson(const std::string& json_file) {
  std::ifstream ifs(fmt::format("{}/{}", dirpath_, json_file));
  rapidjson::IStreamWrapper isw(ifs);
  auto doc = std::make_shared<rapidjson::Document>();
  doc->ParseStream(isw);
  LOG_IF(ERROR, doc->HasParseError()) << fmt::format("parse json file {} failed, error: {}", json_file,
                                                     static_cast<int>(doc->GetParseError()));

  neighbors_id_map = std::make_shared<std::unordered_map<int64_t, std::vector<int64_t>>>();

  const auto& array = doc->GetArray();
  for (size_t i = 0; i < array.Size(); ++i) {
    auto& obj = array[i];
    if (!obj.IsObject()) {
      LOG(ERROR) << "neighbors_labels_label*.json is not object";
      return false;
    }
    if (!obj.HasMember("id") || !obj.HasMember("neighbors_id")) {
      LOG(ERROR) << "neighbors_labels_label*.json id or neighbors_id not exist";
      return false;
    }

    int64_t id = obj["id"].GetInt64();
    const auto& neighbors_id_array = obj["neighbors_id"].GetArray();
    if (id < 0 || neighbors_id_array.Size() == 0) {
      LOG(ERROR) << fmt ::format("neighbors_labels_label*.json id:{} < 0 or neighbors_id_array:{} is empty", id,
                                 neighbors_id_array.Size());
      return false;
    }
    std::vector<int64_t> neighbors;
    for (size_t j = 0; j < neighbors_id_array.Size(); ++j) {
      int64_t neighbor_id = neighbors_id_array[j].GetInt64();
      if (neighbor_id < 0) {
        LOG(ERROR) << fmt::format("neighbors_labels_label*.json id:{} < 0", neighbor_id);
        return false;
      }
      neighbors.push_back(neighbor_id);
    }
    neighbors_id_map->insert(std::make_pair(id, std::move(neighbors)));
  }

  return true;
}

void JsonDataset::GetBatchTrainData(uint32_t, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) {
  is_eof = false;
  if (train_filepaths_.empty()) {
    return;
  }

  for (;;) {
    bool is_empty = false;

    {
      std::lock_guard lock(mutex_);

      if (head_pos_ == tail_pos_) {
        is_empty = true;
        is_eof = train_load_finish_.load();
        if (is_eof) {
          return;
        }
      } else {
        auto batch_vector_entry = batch_vector_entry_cache_[head_pos_];
        head_pos_ = (head_pos_ + 1) % FLAGS_batch_vector_entry_cache_size;

        for (auto& vector_with_id : batch_vector_entry->vector_with_ids) {
          vector_with_ids.push_back(std::move(vector_with_id));
        }
        train_data_count_ += batch_vector_entry->vector_with_ids.size();
        if (train_data_count_ < FLAGS_arrange_data_start_offset) {
          vector_with_ids.clear();
        }
        return;
      }
    }

    if (is_empty) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
}

std::vector<Dataset::TestEntryPtr> JsonDataset::GetTestData() {
  std::vector<Dataset::TestEntryPtr> test_entries;
  for (auto& test_filepath : test_filepaths_) {
    std::ifstream ifs(test_filepath);
    rapidjson::IStreamWrapper isw(ifs);
    rapidjson::Document doc;
    doc.ParseStream(isw);
    if (doc.HasParseError()) {
      DINGO_LOG(ERROR) << fmt::format("parse json file {} failed, error: {}", test_filepath,
                                      static_cast<int>(doc.GetParseError()));
      continue;
    }

    const auto& array = doc.GetArray();
    test_row_count_ += array.Size();
    for (int i = 0; i < array.Size(); ++i) {
      auto entry = ParseTestData(array[i]);
      if (entry != nullptr) {
        test_entries.push_back(entry);
      }
    }
  }

  return test_entries;
}

bool Wikipedia2212Dataset::ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const {
  const auto& item = obj.GetObject();
  if (!item.HasMember("id") || !item.HasMember("title") || !item.HasMember("text") || !item.HasMember("url") ||
      !item.HasMember("wiki_id") || !item.HasMember("views") || !item.HasMember("paragraph_id") ||
      !item.HasMember("langs") || !item.HasMember("emb")) {
    return false;
  }

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    if (!obtain_dimension.load()) {
      FLAGS_vector_dimension = dimension;  // force set
      obtain_dimension.store(true);
    }
    // CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.",
    // dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.id = item["id"].GetInt64() + 1;

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kINT64;
    sdk::ScalarField field;
    field.long_data = item["id"].GetInt64() + 1;
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["id"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kINT64;
    sdk::ScalarField field;
    field.long_data = item["wiki_id"].GetInt64();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["wiki_id"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kINT64;
    sdk::ScalarField field;
    field.long_data = item["paragraph_id"].GetInt64();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["paragraph_id"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kINT64;
    sdk::ScalarField field;
    field.long_data = item["langs"].GetInt64();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["langs"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["title"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["title"] = scalar_value;
  }
  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["url"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["url"] = scalar_value;
  }
  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["text"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["text"] = scalar_value;
  }
  {
    if (item.HasMember("filter_id")) {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::Type::kINT64;
      sdk::ScalarField field;
      field.long_data = item["filter_id"].GetInt64();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["filter_id"] = scalar_value;
    }
  }

  return true;
}

// parse format: field1:int:1:eq,field2:string:hello:ge
// op: eq(==)/ne(!=)/lt(<)/lte(<=)/gt(>)/gte(>=)
static std::vector<std::vector<std::string>> ParseFilterFieldV2(const std::string& value) {
  std::vector<std::vector<std::string>> result;

  std::vector<std::string> parts;
  SplitString(value, ',', parts);

  for (auto& part : parts) {
    std::vector<std::string> sub_parts;
    SplitString(part, ':', sub_parts);
    if (sub_parts.size() == 4) {
      result.push_back(sub_parts);
    }
  }

  return result;
}

std::string GenFilterJson(const std::string& filter_content) {
  auto filter_fields = ParseFilterFieldV2(filter_content);

  rapidjson::Document out_doc;
  out_doc.SetObject();
  rapidjson::Document::AllocatorType& allocator = out_doc.GetAllocator();

  if (filter_fields.size() == 1) {
    auto& filter_field = filter_fields[0];
    const auto& field_name = filter_field[0];
    const auto& field_type = filter_field[1];
    const auto& field_value = filter_field[2];
    const auto& op = filter_field[3];

    out_doc.AddMember(rapidjson::StringRef("type"), rapidjson::StringRef("comparator"), allocator);
    out_doc.AddMember(rapidjson::StringRef("comparator"), rapidjson::StringRef(op.c_str()), allocator);
    out_doc.AddMember(rapidjson::StringRef("attribute"), rapidjson::StringRef(field_name.c_str()), allocator);

    if (field_type == "int" || field_type == "int32" || field_type == "int64" || field_type == "uint" ||
        field_type == "uint32" || field_type == "uint64") {
      out_doc.AddMember(rapidjson::StringRef("value_type"), rapidjson::StringRef("INT64"), allocator);

      int64_t value = std::strtoll(field_value.c_str(), nullptr, 10);
      out_doc.AddMember(rapidjson::StringRef("value"), value, allocator);

    } else if (field_type == "float" || field_type == "double") {
      out_doc.AddMember(rapidjson::StringRef("value_type"), rapidjson::StringRef("DOUBLE"), allocator);
      double value = std::strtod(field_value.c_str(), nullptr);
      out_doc.AddMember(rapidjson::StringRef("value"), value, allocator);

    } else {
      out_doc.AddMember(rapidjson::StringRef("value_type"), rapidjson::StringRef("STRING"), allocator);
      out_doc.AddMember(rapidjson::StringRef("value"), rapidjson::StringRef(field_value.c_str()), allocator);
    }

  } else {
    out_doc.AddMember(rapidjson::StringRef("type"), rapidjson::StringRef("operator"), allocator);
    out_doc.AddMember(rapidjson::StringRef("operator"), rapidjson::StringRef("and"), allocator);

    rapidjson::Value arguments(rapidjson::kArrayType);

    for (auto& filter_field : filter_fields) {
      const auto& field_name = filter_field[0];
      const auto& field_type = filter_field[1];
      const auto& field_value = filter_field[2];
      const auto& op = filter_field[3];

      rapidjson::Value argument(rapidjson::kObjectType);

      argument.AddMember(rapidjson::StringRef("type"), rapidjson::StringRef("comparator"), allocator);
      argument.AddMember(rapidjson::StringRef("comparator"), rapidjson::StringRef(op.c_str()), allocator);
      argument.AddMember(rapidjson::StringRef("attribute"), rapidjson::StringRef(field_name.c_str()), allocator);

      if (field_type == "int" || field_type == "int32" || field_type == "int64" || field_type == "uint" ||
          field_type == "uint32" || field_type == "uint64") {
        argument.AddMember(rapidjson::StringRef("value_type"), rapidjson::StringRef("INT64"), allocator);

        int64_t value = std::strtoll(field_value.c_str(), nullptr, 10);
        argument.AddMember(rapidjson::StringRef("value"), value, allocator);

      } else if (field_type == "float" || field_type == "double") {
        argument.AddMember(rapidjson::StringRef("value_type"), rapidjson::StringRef("DOUBLE"), allocator);
        double value = std::strtod(field_value.c_str(), nullptr);
        argument.AddMember(rapidjson::StringRef("value"), value, allocator);

      } else {
        argument.AddMember(rapidjson::StringRef("value_type"), rapidjson::StringRef("STRING"), allocator);
        argument.AddMember(rapidjson::StringRef("value"), rapidjson::StringRef(field_value.c_str()), allocator);
      }

      arguments.PushBack(argument, allocator);
    }

    out_doc.AddMember(rapidjson::StringRef("arguments"), arguments, allocator);
  }

  rapidjson::StringBuffer str_buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(str_buf);
  out_doc.Accept(writer);

  return str_buf.GetString();
}

Dataset::TestEntryPtr Wikipedia2212Dataset::ParseTestData(const rapidjson::Value& obj) const {
  const auto& item = obj.GetObject();

  sdk::VectorWithId vector_with_id;
  vector_with_id.id = item["id"].GetInt64() + 1;

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.", dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  if (!FLAGS_vector_search_filter.empty()) {
    std::vector<std::string> kv_strs;
    SplitString(FLAGS_vector_search_filter, ';', kv_strs);

    for (auto& kv_str : kv_strs) {
      std::vector<std::string> kv;
      SplitString(kv_str, '=', kv);
      CHECK(kv.size() >= 2) << fmt::format("filter string({}) invalid.", kv_str);

      const auto& key = kv[0];
      const auto& value = kv[1];
      if (key == "title") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kSTRING;
        sdk::ScalarField field;
        field.string_data = value;
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["title"] = scalar_value;
      } else if (key == "text") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kSTRING;
        sdk::ScalarField field;
        field.string_data = value;
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["text"] = scalar_value;
      } else if (key == "langs") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kINT64;
        sdk::ScalarField field;
        field.long_data = std::stoll(value);
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["langs"] = scalar_value;
      } else if (key == "paragraph_id") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kINT64;
        sdk::ScalarField field;
        field.long_data = std::stoll(value);
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["paragraph_id"] = scalar_value;
      } else if (key == "wiki_id") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kINT64;
        sdk::ScalarField field;
        field.long_data = std::stoll(value);
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["wiki_id"] = scalar_value;
      }
    }
  }

  Dataset::TestEntryPtr entry = std::make_shared<Dataset::TestEntry>();
  entry->vector_with_id = vector_with_id;

  if (item.HasMember("filter")) {
    std::string filter_value = item["filter"].GetString();
    entry->filter_json = GenFilterJson(filter_value);
  }

  // set neighbors
  CHECK(item.HasMember("neighbors")) << "missing neighbors";
  const auto& neighbors = item["neighbors"].GetArray();
  uint32_t size = std::min(static_cast<uint32_t>(neighbors.Size()), FLAGS_vector_search_topk);
  for (uint32_t i = 0; i < size; ++i) {
    const auto& neighbor_obj = neighbors[i].GetObject();

    CHECK(neighbor_obj["id"].IsInt64()) << "id type is not int64_t.";
    CHECK(neighbor_obj["distance"].IsFloat()) << "distance type is not float.";
    entry->neighbors[neighbor_obj["id"].GetInt64() + 1] = neighbor_obj["distance"].GetFloat();
  }

  // set filter_vector_ids
  if (FLAGS_vector_search_filter_source == "VECTOR_ID" && item.HasMember("filter_vector_ids")) {
    const auto& filter_vector_ids = item["filter_vector_ids"].GetArray();
    for (uint32_t i = 0; i < filter_vector_ids.Size(); ++i) {
      entry->filter_vector_ids.push_back(filter_vector_ids[i].GetInt64() + 1);
    }
  }
  return entry;
}

bool BeirBioasqDataset::ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const {
  const auto& item = obj.GetObject();
  if (!item.HasMember("_id") || !item.HasMember("title") || !item.HasMember("text") || !item.HasMember("emb")) {
    return true;
  }

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    if (!obtain_dimension.load()) {
      FLAGS_vector_dimension = dimension;  // force set
      obtain_dimension.store(true);
    }
    // CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.",
    // dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.id = std::stoll(item["_id"].GetString()) + 1;

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kINT64;
    sdk::ScalarField field;
    field.long_data = std::stoll(item["_id"].GetString()) + 1;
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["id"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["title"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["title"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["text"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["text"] = scalar_value;
  }

  {
    if (item.HasMember("filter_id")) {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::Type::kINT64;
      sdk::ScalarField field;
      field.long_data = item["filter_id"].GetInt64();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["filter_id"] = scalar_value;
    }
  }

  return true;
}

Dataset::TestEntryPtr BeirBioasqDataset::ParseTestData(const rapidjson::Value& obj) const {
  const auto& item = obj.GetObject();

  sdk::VectorWithId vector_with_id;
  vector_with_id.id = std::stoll(item["_id"].GetString()) + 1;

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.", dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  if (!FLAGS_vector_search_filter.empty()) {
    std::vector<std::string> kv_strs;
    SplitString(FLAGS_vector_search_filter, ';', kv_strs);

    for (auto& kv_str : kv_strs) {
      std::vector<std::string> kv;
      SplitString(kv_str, '=', kv);
      CHECK(kv.size() >= 2) << fmt::format("filter string({}) invalid.", kv_str);

      const auto& key = kv[0];
      const auto& value = kv[1];
      if (key == "title") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kSTRING;
        sdk::ScalarField field;
        field.string_data = value;
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["title"] = scalar_value;
      } else if (key == "text") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kSTRING;
        sdk::ScalarField field;
        field.string_data = value;
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["text"] = scalar_value;
      }
    }
  }

  Dataset::TestEntryPtr entry = std::make_shared<Dataset::TestEntry>();
  entry->vector_with_id = vector_with_id;

  // set filter_json
  if (item.HasMember("filter")) {
    std::string filter_value = item["filter"].GetString();
    entry->filter_json = GenFilterJson(filter_value);
  }

  // set nuighbors
  CHECK(item.HasMember("neighbors")) << "missing neighbors";
  const auto& neighbors = item["neighbors"].GetArray();
  CHECK(!neighbors.Empty()) << "neighbor size is empty.";
  uint32_t size = std::min(static_cast<uint32_t>(neighbors.Size()), FLAGS_vector_search_topk);
  for (uint32_t i = 0; i < size; ++i) {
    const auto& neighbor_obj = neighbors[i].GetObject();

    CHECK(neighbor_obj["id"].IsInt64()) << "id type is not int64_t.";
    CHECK(neighbor_obj["distance"].IsFloat()) << "distance type is not float.";
    entry->neighbors[neighbor_obj["id"].GetInt64() + 1] = neighbor_obj["distance"].GetFloat();
  }

  // set filter_vector_ids
  if (FLAGS_vector_search_filter_source == "VECTOR_ID" && item.HasMember("filter_vector_ids")) {
    const auto& filter_vector_ids = item["filter_vector_ids"].GetArray();
    for (uint32_t i = 0; i < filter_vector_ids.Size(); ++i) {
      entry->filter_vector_ids.push_back(filter_vector_ids[i].GetInt64() + 1);
    }
  }

  return entry;
}

static int64_t GetMiraclVectorId(const rapidjson::Value& obj) {
  std::string id(obj["docid"].GetString());
  std::vector<std::string> sub_parts;
  SplitString(id, '#', sub_parts);
  CHECK(sub_parts.size() == 2) << fmt::format("id({}) is invalid", id);

  return std::stoll(fmt::format("{}{:0>4}", sub_parts[0], sub_parts[1]));
};

bool MiraclDataset::ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const {
  const auto& item = obj.GetObject();
  if (!item.HasMember("docid") || !item.HasMember("title") || !item.HasMember("text") || !item.HasMember("emb")) {
    return true;
  }

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    if (!obtain_dimension.load()) {
      FLAGS_vector_dimension = dimension;  // force set
      obtain_dimension.store(true);
    }
    // CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.",
    // dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  int64_t id = GetMiraclVectorId(item) + 1;
  vector_with_id.id = id;

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kINT64;
    sdk::ScalarField field;
    field.long_data = id;
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["id"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["title"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["title"] = scalar_value;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = item["text"].GetString();
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["text"] = scalar_value;
  }

  {
    if (item.HasMember("filter_id")) {
      sdk::ScalarValue scalar_value;
      scalar_value.type = sdk::Type::kINT64;
      sdk::ScalarField field;
      field.long_data = item["filter_id"].GetInt64();
      scalar_value.fields.push_back(field);
      vector_with_id.scalar_data["filter_id"] = scalar_value;
    }
  }

  return true;
}

Dataset::TestEntryPtr MiraclDataset::ParseTestData(const rapidjson::Value& obj) const {
  const auto& item = obj.GetObject();

  sdk::VectorWithId vector_with_id;
  vector_with_id.id = GetMiraclVectorId(item) + 1;

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.", dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  if (!FLAGS_vector_search_filter.empty()) {
    std::vector<std::string> kv_strs;
    SplitString(FLAGS_vector_search_filter, ';', kv_strs);

    for (auto& kv_str : kv_strs) {
      std::vector<std::string> kv;
      SplitString(kv_str, '=', kv);
      CHECK(kv.size() >= 2) << fmt::format("filter string({}) invalid.", kv_str);

      const auto& key = kv[0];
      const auto& value = kv[1];
      if (key == "title") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kSTRING;
        sdk::ScalarField field;
        field.string_data = value;
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["title"] = scalar_value;
      } else if (key == "text") {
        sdk::ScalarValue scalar_value;
        scalar_value.type = sdk::Type::kSTRING;
        sdk::ScalarField field;
        field.string_data = value;
        scalar_value.fields.push_back(field);
        vector_with_id.scalar_data["text"] = scalar_value;
      }
    }
  }

  Dataset::TestEntryPtr entry = std::make_shared<Dataset::TestEntry>();
  entry->vector_with_id = vector_with_id;

  // set filter
  if (item.HasMember("filter")) {
    std::string filter_value = item["filter"].GetString();
    entry->filter_json = GenFilterJson(filter_value);
  }

  // set neighbors
  CHECK(item.HasMember("neighbors")) << "missing neighbors";
  const auto& neighbors = item["neighbors"].GetArray();
  CHECK(!neighbors.Empty()) << "neighbor size is empty.";
  uint32_t size = std::min(static_cast<uint32_t>(neighbors.Size()), FLAGS_vector_search_topk);
  for (uint32_t i = 0; i < size; ++i) {
    const auto& neighbor_obj = neighbors[i].GetObject();

    CHECK(neighbor_obj["id"].IsInt64()) << "id type is not int64_t.";
    CHECK(neighbor_obj["distance"].IsFloat()) << "distance type is not float.";
    entry->neighbors[neighbor_obj["id"].GetInt64() + 1] = neighbor_obj["distance"].GetFloat();
  }

  // set filter_vector_ids
  if (FLAGS_vector_search_filter_source == "VECTOR_ID" && item.HasMember("filter_vector_ids")) {
    const auto& filter_vector_ids = item["filter_vector_ids"].GetArray();
    for (uint32_t i = 0; i < filter_vector_ids.Size(); ++i) {
      entry->filter_vector_ids.push_back(filter_vector_ids[i].GetInt64() + 1);
    }
  }

  return entry;
}

bool BioasqMediumDataset::ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const {
  const auto& item = obj.GetObject();
  if (!item.HasMember("id") || !item.HasMember("emb")) {
    return false;
  }

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    if (!obtain_dimension.load()) {
      FLAGS_vector_dimension = dimension;  // force set
      obtain_dimension.store(true);
    }
    // CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.",
    // dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.id = item["id"].GetInt64() + 1;

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  int64_t id = item["id"].GetInt64();

  auto iter = scalar_labels_map->find(id);
  if (iter == scalar_labels_map->end()) {
    LOG(ERROR) << fmt::format("id({}) not found in scalar_labels_map", id);
    return false;
  }

  {
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = iter->second;
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["labels"] = scalar_value;
  }

  return true;
}

Dataset::TestEntryPtr BioasqMediumDataset::ParseTestData(const rapidjson::Value& obj) const {
  const auto& item = obj.GetObject();

  // set neighbors
  int64_t id = item["id"].GetInt64();
  auto iter = neighbors_id_map->find(id);
  if (iter == neighbors_id_map->end()) {
    LOG(FATAL) << fmt::format("id({}) not found in neighbors_id_map", id);
    return nullptr;
  }

  sdk::VectorWithId vector_with_id;
  vector_with_id.id = item["id"].GetInt64() + 1;

  std::vector<float> embedding;
  if (item["emb"].IsArray()) {
    uint32_t dimension = item["emb"].GetArray().Size();
    CHECK(dimension == FLAGS_vector_dimension) << fmt::format("dataset dimension({}) is not uniformity.", dimension);
    embedding.reserve(dimension);
    for (const auto& f : item["emb"].GetArray()) {
      embedding.push_back(f.GetFloat());
    }
  }

  vector_with_id.vector.value_type = sdk::ValueType::kFloat;
  vector_with_id.vector.float_values.swap(embedding);
  vector_with_id.vector.dimension = FLAGS_vector_dimension;

  if (!FLAGS_vector_search_filter.empty()) {
    std::string value;

    if (neighbors_id_map->empty()) {
      LOG(FATAL) << "neighbors_id_map is empty";
      return nullptr;
    }

    if (!already_set_label_name_) {
      label_name_ = "label_" + FLAGS_vector_search_scalar_filter_radius + "p";
      already_set_label_name_.store(true);
    }

    value = label_name_;
    sdk::ScalarValue scalar_value;
    scalar_value.type = sdk::Type::kSTRING;
    sdk::ScalarField field;
    field.string_data = value;
    scalar_value.fields.push_back(field);
    vector_with_id.scalar_data["labels"] = scalar_value;
  }

  Dataset::TestEntryPtr entry = std::make_shared<Dataset::TestEntry>();
  entry->vector_with_id = vector_with_id;

  // ignore filter json
  // entry->filter_json = GenFilterJson(filter_value);

  uint32_t size = std::min(static_cast<uint32_t>(iter->second.size()), FLAGS_vector_search_topk);
  for (uint32_t i = 0; i < size; ++i) {
    const auto& neighbors_id = iter->second[i];
    entry->neighbors[neighbors_id + 1] = 0.0f;
  }

  return entry;
}

bool OpenaiLargeDataset::ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const {
  return BioasqMediumDataset::ParseTrainData(obj, vector_with_id);
}

Dataset::TestEntryPtr OpenaiLargeDataset::ParseTestData(const rapidjson::Value& obj) const {
  return BioasqMediumDataset::ParseTestData(obj);
}

}  // namespace benchmark
}  // namespace dingodb
