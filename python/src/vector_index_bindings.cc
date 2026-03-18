
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

#include "vector_index_bindings.h"

#include <nanobind/nanobind.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <tuple>

#include "sdk/vector/vector_index.h"

void DefineVectorIndexBindings(nanobind::module_& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = nanobind;

  py::class_<VectorIndex>(m, "VectorIndex")
      .def("GetId", &VectorIndex::GetId)
      .def("GetSchemaId", &VectorIndex::GetSchemaId)
      .def("GetName", &VectorIndex::GetName)
      .def("GetVectorIndexType", &VectorIndex::GetVectorIndexType)
      .def("GetPartitionId", &VectorIndex::GetPartitionId)
      .def("GetPartitionIds", &VectorIndex::GetPartitionIds)
      .def("IsStale", &VectorIndex::IsStale)
      .def("HasAutoIncrement", &VectorIndex::HasAutoIncrement)
      .def("GetIncrementStartId", &VectorIndex::GetIncrementStartId)
      .def("HasScalarSchema", &VectorIndex::HasScalarSchema)
      .def("GetSchema", &VectorIndex::GetScalarSchema)
      .def("ToString", &VectorIndex::ToString);
}
