
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

#ifndef DINGODB_SDK_DOCUMENT_PARAMS_H_
#define DINGODB_SDK_DOCUMENT_PARAMS_H_

#include <memory>
#include <sstream>

#include "sdk/document.h"
#include "sdk/document/document_doc_value_internal.h"
#include "sdk/types.h"

namespace dingodb {
namespace sdk {

DocValue::DocValue(const DocValue& other) : data_(new Data(*other.data_)) {}

DocValue& DocValue::operator=(const DocValue& other) {
  if (this != &other) {
    data_ = std::make_unique<Data>(*other.data_);
  }
  return *this;
}

DocValue::DocValue() { data_ = std::make_unique<DocValue::Data>(); }

DocValue::~DocValue() = default;

DocValue::DocValue(DocValue&& other) noexcept : data_(std::move(other.data_)) {}

DocValue& DocValue::operator=(DocValue&& other) noexcept {
  if (this != &other) {
    data_ = std::move(other.data_);
  }
  return *this;
}

DocValue DocValue::FromInt(int64_t val) {
  DocValue value;
  value.data_->type = Type::kINT64;
  value.data_->int_val = val;
  return value;
}

DocValue DocValue::FromDouble(double val) {
  DocValue value;
  value.data_->type = Type::kDOUBLE;
  value.data_->double_val = val;
  return value;
}

DocValue DocValue::FromString(const std::string& val) {
  DocValue value;
  value.data_->type = Type::kSTRING;
  value.data_->string_val = val;
  return value;
}

DocValue DocValue::FromBytes(const std::string& val) {
  DocValue value;
  value.data_->type = Type::kBYTES;
  value.data_->string_val = val;
  return value;
}

Type DocValue::GetType() const { return data_->type; }

int64_t DocValue::IntValue() const { return data_->int_val; }

double DocValue::DoubleValue() const { return data_->double_val; }

std::string DocValue::StringValue() const { return data_->string_val; }

std::string DocValue::ToString() const {
  std::stringstream ss;
  ss << "DocValue { type: " << TypeToString(data_->type) << ", value: ";

  switch (data_->type) {
    case Type::kINT64:
      ss << std::to_string(data_->int_val);
    case Type::kDOUBLE:
      ss << std::to_string(data_->double_val);
    case Type::kSTRING:
      ss << data_->string_val;
    default:
      ss << "";
  }

  ss << " }";

  return ss.str();
}

void Document::AddField(const std::string& key, const DocValue& value) { fields_.emplace(key, value); }

std::string Document::ToString() const {
  std::string result = "Document {";
  for (auto it = fields_.begin(); it != fields_.end();) {
    result += it->first + ": ( " + it->second.ToString() + " )";
    if (++it != fields_.end()) {
      result += ", ";
    }
  }
  result += "}";
  return result;
}

std::string DocWithId::ToString() const {
  return "DocWithId{id: " + std::to_string(id) + ", doc: " + doc.ToString() + "}";
}

std::string DocQueryResult::ToString() const {
  std::string result = "DocQueryResult { docs: [";
  for (auto it = docs.begin(); it != docs.end();) {
    result += it->ToString();
    if (++it != docs.end()) {
      result += ", ";
    }
  }
  result += "] }";
  return result;
}

std::string DocWithStore::ToString() const {
  std::ostringstream oss;
  oss << "DocWithStore{doc_with_id: " << doc_with_id.ToString() << ", score: " << score << "}";
  return oss.str();
}

std::string DocSearchResult::ToString() const {
  std::ostringstream oss;
  oss << "DocSearchResult{doc_scores: [";
  for (auto it = doc_sores.begin(); it != doc_sores.end(); ++it) {
    if (it != doc_sores.begin()) {
      oss << ", ";
    }
    oss << it->ToString();
  }
  oss << "]}";
  return oss.str();
}

std::string DocDeleteResult::ToString() const {
  std::ostringstream oss;
  oss << "DocDeleteResult{doc_id: " << doc_id << ", deleted: " << (deleted ? "true" : "false") << "}";
  return oss.str();
}

std::string DocScanQueryResult::ToString() const {
  std::ostringstream oss;
  oss << "DocScanQueryResult { docs: [";
  for (auto it = docs.begin(); it != docs.end();) {
    oss << it->ToString();
    if (++it != docs.end()) {
      oss << ", ";
    }
  }
  oss << "] }";
  return oss.str();
}

std::string DocIndexMetricsResult::ToString() const {
  std::ostringstream oss;
  oss << "DocIndexMetricsResult {"
      << " total_num_docs: " << total_num_docs << ", total_num_tokens: " << total_num_tokens
      << ", max_doc_id: " << max_doc_id << ", min_doc_id: " << min_doc_id << ", meta_json: " << meta_json
      << ", json_parameter: " << json_parameter << " }";
  return oss.str();
}

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_DOCUMENT_PARAMS_H_
