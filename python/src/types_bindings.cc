
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

#include "types_bindings.h"

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <tuple>

#include "sdk/types.h"

void DefineTypesBindings(pybind11::module& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = pybind11;

  py::enum_<Type>(m, "Type")
      .value("kBOOL", Type::kBOOL)
      .value("kINT64", Type::kINT64)
      .value("kDOUBLE", Type::kDOUBLE)
      .value("kSTRING", Type::kSTRING)
      .value("kTypeEnd", Type::kTypeEnd);

  m.def("TypeToString", &TypeToString, "description: TypeToString");

}