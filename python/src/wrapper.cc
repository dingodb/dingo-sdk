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

#include <pybind11/pybind11.h>

#include "client_bindings.h"
#include "status_bindings.h"
#include "vector_bindings.h"
#include "types_bindings.h"

PYBIND11_MODULE(dingosdk, m) {
  m.doc() = "python sdk for dingo store, from https://github.com/dingodb/dingo-sdk";

  DefineStatusBindings(m);
  DefineVectorBindings(m);
  DefineClientBindings(m);
  DefineTypesBindings(m);
}