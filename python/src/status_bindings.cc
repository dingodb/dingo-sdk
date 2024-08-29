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

#include "status_bindings.h"

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <tuple>

#include "sdk/status.h"

void DefineStatusBindings(pybind11::module& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = pybind11;

  py::class_<Status>(m, "Status")
      .def(py::init<>())
      .def("ok", &Status::ok)
      .def_static("OK", py::overload_cast<>(&Status::OK))
      .def("IsOK", &Status::IsOK)
      .def("IsNotFound", &Status::IsNotFound)
      .def("IsCorruption", &Status::IsCorruption)
      .def("IsNotSupported", &Status::IsNotSupported)
      .def("IsInvalidArgument", &Status::IsInvalidArgument)
      .def("IsIOError", &Status::IsIOError)
      .def("IsAlreadyPresent", &Status::IsAlreadyPresent)
      .def("IsRuntimeError", &Status::IsRuntimeError)
      .def("IsNetworkError", &Status::IsNetworkError)
      .def("IsIllegalState", &Status::IsIllegalState)
      .def("IsNotAuthorized", &Status::IsNotAuthorized)
      .def("IsAborted", &Status::IsAborted)
      .def("IsRemoteError", &Status::IsRemoteError)
      .def("IsServiceUnavailable", &Status::IsServiceUnavailable)
      .def("IsTimedOut", &Status::IsTimedOut)
      .def("IsUninitialized", &Status::IsUninitialized)
      .def("IsConfigurationError", &Status::IsConfigurationError)
      .def("IsIncomplete", &Status::IsIncomplete)
      .def("IsNotLeader", &Status::IsNotLeader)
      .def("IsTxnLockConflict", &Status::IsTxnLockConflict)
      .def("IsTxnWriteConflict", &Status::IsTxnWriteConflict)
      .def("IsTxnNotFound", &Status::IsTxnNotFound)
      .def("IsTxnPrimaryMismatch", &Status::IsTxnPrimaryMismatch)
      .def("IsTxnRolledBack", &Status::IsTxnRolledBack)
      .def("IsNoLeader", &Status::IsNoLeader)
      .def("IsBuildFailed", &Status::IsBuildFailed)
      .def("IsLoadFailed", &Status::IsLoadFailed)
      .def("IsResetFailed", &Status::IsResetFailed)
      .def("ToString", &Status::ToString)
      .def("Errno", &Status::Errno);
}
