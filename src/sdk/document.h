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

#ifndef DINGODB_SDK_DOCUMENT_H_
#define DINGODB_SDK_DOCUMENT_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "sdk/status.h"
#include "sdk/types.h"

namespace dingodb {
namespace sdk {

class ClientStub;
class DocumentTranslater;

struct DocumentColumn {
  std::string key;
  Type type;

  DocumentColumn(const std::string& key, Type type) : key(key), type(type) {}
};

struct DocumentSchema {
  void AddColumn(const DocumentColumn& col) { cols.push_back(col); }

  std::vector<DocumentColumn> cols;
};

class DocumentIndexCreator {
 public:
  ~DocumentIndexCreator();

  DocumentIndexCreator& SetSchemaId(int64_t schema_id);

  DocumentIndexCreator& SetName(const std::string& name);

  DocumentIndexCreator& SetRangePartitions(std::vector<int64_t> separator_id);

  DocumentIndexCreator& SetReplicaNum(int64_t num);

  // when start_id greater than 0, index is enable auto_increment
  DocumentIndexCreator& SetAutoIncrementStart(int64_t start_id);

  DocumentIndexCreator& SetSchema(const DocumentSchema& schema);

  DocumentIndexCreator& SetJsonParams(std::string& json_params);

  Status Create(int64_t& out_index_id);

 private:
  friend class Client;

  // own
  class Data;
  Data* data_;
  explicit DocumentIndexCreator(Data* data);
};

class DocValue {
 public:
  ~DocValue();

  DocValue(const DocValue&);
  DocValue& operator=(const DocValue&);
  DocValue(DocValue&& other) noexcept;
  DocValue& operator=(DocValue&& other) noexcept;

  static DocValue FromInt(int64_t val);
  static DocValue FromDouble(double val);
  static DocValue FromString(const std::string& val);
  static DocValue FromBytes(const std::string& val);

  Type GetType() const;
  int64_t IntValue() const;
  double DoubleValue() const;
  std::string StringValue() const;
  std::string BytesValue() const;

  std::string ToString() const;

 private:
  friend class DocumentTranslater;
  class Data;
  std::unique_ptr<Data> data_;

  explicit DocValue();
};

class Document {
 public:
  void AddField(const std::string& key, const DocValue& value);

  std::unordered_map<std::string, DocValue> GetFields() const;

  std::string ToString() const;

 private:
  friend class DocumentTranslater;
  std::unordered_map<std::string, DocValue> fields_;
};

struct DocWithId {
  int64_t id;
  Document doc;

  explicit DocWithId() : id(0) {}

  explicit DocWithId(int64_t p_id, Document p_doc) : id(p_id), doc(std::move(p_doc)) {}

  explicit DocWithId(Document p_doc) : id(0), doc(std::move(p_doc)) {}

  std::string ToString() const;
};

struct DocQueryParam {
  std::vector<int64_t> doc_ids;
  // if true, response with scalar data
  bool with_scalar_data{false};
  // If with_scalar_data is true, selected_keys is used to select scalar data, and if this parameter is null, all scalar
  // data will be returned.
  std::vector<std::string> selected_keys;
};

struct DocQueryResult {
  std::vector<DocWithId> docs;

  std::string ToString() const;
};

struct DocSearchParam {
  int32_t top_n{0};
  std::string query_string;
  bool use_id_filter{false};
  std::vector<int64_t> doc_ids;
  std::vector<std::string> column_names;
  bool with_scalar_data{false};
  std::vector<std::string> selected_keys;
};

struct DocWithStore {
  DocWithId doc_with_id;
  float score{0.0};

  std::string ToString() const;
};

struct DocSearchResult {
  std::vector<DocWithStore> doc_sores;
  std::string ToString() const;
};

struct DocDeleteResult {
  int64_t doc_id;
  bool deleted;

  std::string ToString() const;
};

struct DocScanQueryParam {
  int64_t doc_id_start;
  // the end id of scan
  // if is_reverse is true, doc_id_end must be less than doc_id_start
  // if is_reverse is false, doc_id_end must be greater than doc_id_start
  // the real range is [start, end], include start and end
  // if doc_id_end == 0, scan to the end of the region
  int64_t doc_id_end{0};
  bool is_reverse{false};
  int64_t max_scan_count{0};

  bool with_scalar_data{true};
  // If with_scalar_data is true, selected_keys is used to select scalar data, and if this parameter is null, all scalar
  // data will be returned.
  std::vector<std::string> selected_keys;
};

struct DocScanQueryResult {
  std::vector<DocWithId> docs;

  std::string ToString() const;
};

struct DocIndexMetricsResult {
  int64_t total_num_docs{0};
  int64_t total_num_tokens{0};
  int64_t max_doc_id{0};
  int64_t min_doc_id{0};
  std::string meta_json;
  std::string json_parameter;

  std::string ToString() const;
};

class DocumentClient {
 public:
  DocumentClient(const DocumentClient&) = delete;
  const DocumentClient& operator=(const DocumentClient&) = delete;

  ~DocumentClient() = default;

  Status AddByIndexId(int64_t index_id, std::vector<DocWithId>& docs);
  Status AddByIndexName(int64_t schema_id, const std::string& index_name, std::vector<DocWithId>& docs);

  Status UpdateByIndexId(int64_t index_id, std::vector<DocWithId>& docs);
  Status UpdateByIndexName(int64_t schema_id, const std::string& index_name, std::vector<DocWithId>& docs);

  Status SearchByIndexId(int64_t index_id, const DocSearchParam& search_param, DocSearchResult& out_result);
  Status SearchByIndexName(int64_t schema_id, const std::string& index_name, const DocSearchParam& search_param,
                           DocSearchResult& out_result);

  Status DeleteByIndexId(int64_t index_id, const std::vector<int64_t>& doc_ids,
                         std::vector<DocDeleteResult>& out_result);
  Status DeleteByIndexName(int64_t schema_id, const std::string& index_name, const std::vector<int64_t>& doc_ids,
                           std::vector<DocDeleteResult>& out_result);

  Status BatchQueryByIndexId(int64_t index_id, const DocQueryParam& query_param, DocQueryResult& out_result);
  Status BatchQueryByIndexName(int64_t schema_id, const std::string& index_name, const DocQueryParam& query_param,
                               DocQueryResult& out_result);

  Status GetBorderByIndexId(int64_t index_id, bool is_max, int64_t& out_doc_id);
  Status GetBorderByIndexName(int64_t schema_id, const std::string& index_name, bool is_max, int64_t& out_doc_id);

  Status ScanQueryByIndexId(int64_t index_id, const DocScanQueryParam& query_param, DocScanQueryResult& out_result);
  Status ScanQueryByIndexName(int64_t schema_id, const std::string& index_name, const DocScanQueryParam& query_param,
                              DocScanQueryResult& out_result);

  Status GetIndexMetricsByIndexId(int64_t index_id, DocIndexMetricsResult& out_result);
  Status GetIndexMetricsByIndexName(int64_t schema_id, const std::string& index_name,
                                    DocIndexMetricsResult& out_result);

  Status CountAllByIndexId(int64_t index_id, int64_t& out_count);
  Status CountallByIndexName(int64_t schema_id, const std::string& index_name, int64_t& out_count);

  Status CountByIndexId(int64_t index_id, int64_t start_doc_id, int64_t end_doc_id, int64_t& out_count);
  Status CountByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_doc_id, int64_t end_doc_id,
                          int64_t& out_count);

  Status GetAutoIncrementIdByIndexId(int64_t index_id, int64_t& start_id);
  Status GetAutoIncrementIdByIndexName(int64_t schema_id, const std::string& index_name, int64_t& start_id);

  Status UpdateAutoIncrementIdByIndexId(int64_t index_id, int64_t start_id);
  Status UpdateAutoIncrementIdByIndexName(int64_t schema_id, const std::string& index_name, int64_t& start_id);

 private:
  friend class Client;

  const ClientStub& stub_;

  explicit DocumentClient(const ClientStub& stub);
};

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_DOCUMENT_H_