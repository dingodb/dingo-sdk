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

#ifndef DINGODB_SDK_DOCUMENT_TRANSLATER_H_
#define DINGODB_SDK_DOCUMENT_TRANSLATER_H_

#include <cstdint>

#include "dingosdk/document.h"
#include "dingosdk/types.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "sdk/codec/document_codec.h"
#include "sdk/document/document_doc_value_internal.h"
#include "sdk/types_util.h"

namespace dingodb {
namespace sdk {

class DocumentTranslater {
 public:
  static void FillRangePartitionRule(pb::meta::PartitionRule* partition_rule, const std::vector<int64_t>& seperator_ids,
                                     const std::vector<int64_t>& index_and_part_ids) {
    auto part_count = seperator_ids.size() + 1;
    CHECK(part_count == index_and_part_ids.size() - 1);

    int64_t new_index_id = index_and_part_ids[0];

    for (int i = 0; i < part_count; i++) {
      auto* part = partition_rule->add_partitions();
      int64_t part_id = index_and_part_ids[i + 1];  // 1st use for index id
      part->mutable_id()->set_entity_id(part_id);
      part->mutable_id()->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_PART);
      part->mutable_id()->set_parent_entity_id(new_index_id);
      std::string start;
      if (i == 0) {
        document_codec::EncodeDocumentKey(Constant::kClientRaw, part_id, start);
      } else {
        document_codec::EncodeDocumentKey(Constant::kClientRaw, part_id, seperator_ids[i - 1], start);
      }
      part->mutable_range()->set_start_key(start);
      std::string end;
      document_codec::EncodeDocumentKey(Constant::kClientRaw, part_id + 1, end);
      part->mutable_range()->set_end_key(end);
    }
  }  // namespace sdk

  static void FillScalarSchemaItem(pb::common::ScalarSchemaItem* pb, const DocumentColumn& schema) {
    pb->set_key(schema.key);
    pb->set_field_type(Type2InternalScalarFieldTypePB(schema.type));
  }

  static void FillScalarSchema(pb::common::ScalarSchema* pb, const DocumentSchema& schema) {
    for (const auto& col : schema.cols) {
      FillScalarSchemaItem(pb->add_fields(), col);
    }
  }

  static pb::common::DocumentValue DocValue2InternalDocumentValuePB(const DocValue& doc_value) {
    pb::common::DocumentValue result;
    result.set_field_type(Type2InternalScalarFieldTypePB(doc_value.data_->type));

    auto* pb_field = result.mutable_field_value();
    switch (doc_value.data_->type) {
      case kINT64:
        pb_field->set_long_data(doc_value.data_->int_val);
        break;
      case kDOUBLE:
        pb_field->set_double_data(doc_value.data_->double_val);
        break;
      case kSTRING:
        pb_field->set_string_data(doc_value.data_->string_val);
        break;
      case kBYTES:
        pb_field->set_bytes_data(doc_value.data_->string_val);
        break;
      case kBOOL:
        pb_field->set_bool_data(doc_value.data_->bool_val);
        break;
      case kDATETIME:
        pb_field->set_datetime_data(doc_value.data_->string_val);
        break;
      default:
        CHECK(false) << "unsupported doc value type:" << TypeToString(doc_value.data_->type);
    }

    return result;
  }

  static DocValue InternalDocumentValuePb2DocValue(const pb::common::DocumentValue& pb_doc_value) {
    switch (pb_doc_value.field_type()) {
      case pb::common::ScalarFieldType::INT64:
        return DocValue::FromInt(pb_doc_value.field_value().long_data());
      case pb::common::ScalarFieldType::DOUBLE:
        return DocValue::FromDouble(pb_doc_value.field_value().double_data());
      case pb::common::ScalarFieldType::STRING:
        return DocValue::FromString(pb_doc_value.field_value().string_data());
      case pb::common::ScalarFieldType::BYTES:
        return DocValue::FromBytes(pb_doc_value.field_value().bytes_data());
      case pb::common::ScalarFieldType::BOOL:
        return DocValue::FromBool(pb_doc_value.field_value().bool_data());
      case pb::common::ScalarFieldType::DATETIME:
        return DocValue::FromDatetime(pb_doc_value.field_value().datetime_data());
      default:
        CHECK(false) << "unsupported DocumentValue field_type:"
                     << pb::common::ScalarFieldType_Name(pb_doc_value.field_type());
    }
  }

  static void FillDocumentWithIdPB(pb::common::DocumentWithId* pb, const DocWithId& doc_with_id, bool with_id = true) {
    if (with_id) {
      pb->set_id(doc_with_id.id);
    }

    auto* doc_pb = pb->mutable_document();

    const auto& doc = doc_with_id.doc;
    for (const auto& [key, doc_value] : doc.fields_) {
      doc_pb->mutable_document_data()->emplace(key, DocValue2InternalDocumentValuePB(doc_value));
    }
  }

  static DocWithId InternalDocumentWithIdPB2DocWithId(const pb::common::DocumentWithId& pb) {
    DocWithId to_return;
    to_return.id = pb.id();

    const auto& doc_pb = pb.document();
    for (const auto& [key, doc_value_pb] : doc_pb.document_data()) {
      to_return.doc.AddField(key, InternalDocumentValuePb2DocValue(doc_value_pb));
    }

    return std::move(to_return);
  }

  static DocWithStore InternalDocumentWithScore2DocWithStore(const pb::common::DocumentWithScore& pb) {
    DocWithStore to_return;
    to_return.doc_with_id = InternalDocumentWithIdPB2DocWithId(pb.document_with_id());
    to_return.score = pb.score();
    return std::move(to_return);
  }

  static void FillInternalDocSearchParams(pb::common::DocumentSearchParameter* pb, const DocSearchParam& param) {
    pb->set_top_n(param.top_n);
    pb->set_query_string(param.query_string);
    pb->set_use_id_filter(param.use_id_filter);
    if (param.use_id_filter) {
      for (const auto& id : param.doc_ids) {
        pb->add_document_ids(id);
      }
    }

    for (const std::string& col : param.column_names) {
      pb->add_column_names(col);
    }

    pb->set_without_scalar_data(!param.with_scalar_data);

    for (const auto& key : param.selected_keys) {
      pb->add_selected_keys(key);
    }
  }

  static void FillInternalDocSearchAllParams(pb::common::DocumentSearchParameter* pb, const DocSearchParam& param) {
    pb->set_query_string(param.query_string);
    pb->set_use_id_filter(param.use_id_filter);
    pb->set_query_unlimited(true);
    if (param.use_id_filter) {
      for (const auto& id : param.doc_ids) {
        pb->add_document_ids(id);
      }
    }

    for (const std::string& col : param.column_names) {
      pb->add_column_names(col);
    }

    pb->set_without_scalar_data(!param.with_scalar_data);

    for (const auto& key : param.selected_keys) {
      pb->add_selected_keys(key);
    }
  }
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_DOCUMENT_TRANSLATER_H_