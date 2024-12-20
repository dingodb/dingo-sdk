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

#ifndef DINGODB_SDK_DOCUMENT_CREATOR_DATA_H_
#define DINGODB_SDK_DOCUMENT_CREATOR_DATA_H_

#include <optional>
#include <utility>

#include "sdk/client_stub.h"
#include "dingosdk/document.h"
#include "sdk/document/document_translater.h"

namespace dingodb {
namespace sdk {

class DocumentIndexCreator::Data {
 public:
  Data(const Data&) = delete;
  const Data& operator=(const Data&) = delete;

  explicit Data(const ClientStub& stub) : stub(stub) {}

  ~Data() = default;

  void BuildIndexParameter(pb::common::IndexParameter* index_parameter) {
    index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_DOCUMENT);
    auto* parameter = index_parameter->mutable_document_index_parameter();

    CHECK(schema.has_value());
    DocumentSchema& s = schema.value();
    DocumentTranslater::FillScalarSchema(parameter->mutable_scalar_schema(), s);

    if (json_params.has_value()) {
      parameter->set_json_parameter(json_params.value());
    }
  }

  const ClientStub& stub;
  int64_t schema_id{-1};
  std::string doc_name;
  std::vector<int64_t> range_partition_seperator_ids;
  int64_t replica_num{3};

  std::optional<int64_t> auto_incr_start;
  std::optional<DocumentSchema> schema;
  std::optional<std::string> json_params;

  bool wait{true};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_DOCUMENT_CREATOR_DATA_H_