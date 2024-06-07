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
      .def_static("OK", py::overload_cast<const Slice&, const Slice&>(&Status::OK))
      .def_static("OK", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::OK))
      .def("IsOK", &Status::IsOK)
      .def_static("NotFound", py::overload_cast<const Slice&, const Slice&>(&Status::NotFound))
      .def_static("NotFound", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::NotFound))
      .def("IsNotFound", &Status::IsNotFound)
      .def_static("Corruption", py::overload_cast<const Slice&, const Slice&>(&Status::Corruption))
      .def_static("Corruption", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::Corruption))
      .def("IsCorruption", &Status::IsCorruption)
      .def_static("NotSupported", py::overload_cast<const Slice&, const Slice&>(&Status::NotSupported))
      .def_static("NotSupported", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::NotSupported))
      .def("IsNotSupported", &Status::IsNotSupported)
      .def_static("InvalidArgument", py::overload_cast<const Slice&, const Slice&>(&Status::InvalidArgument))
      .def_static("InvalidArgument", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::InvalidArgument))
      .def("IsInvalidArgument", &Status::IsInvalidArgument)
      .def_static("IOError", py::overload_cast<const Slice&, const Slice&>(&Status::IOError))
      .def_static("IOError", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::IOError))
      .def("IsIOError", &Status::IsIOError)
      .def_static("AlreadyPresent", py::overload_cast<const Slice&, const Slice&>(&Status::AlreadyPresent))
      .def_static("AlreadyPresent", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::AlreadyPresent))
      .def("IsAlreadyPresent", &Status::IsAlreadyPresent)
      .def_static("RuntimeError", py::overload_cast<const Slice&, const Slice&>(&Status::RuntimeError))
      .def_static("RuntimeError", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::RuntimeError))
      .def("IsRuntimeError", &Status::IsRuntimeError)
      .def_static("NetworkError", py::overload_cast<const Slice&, const Slice&>(&Status::NetworkError))
      .def_static("NetworkError", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::NetworkError))
      .def("IsNetworkError", &Status::IsNetworkError)
      .def_static("IllegalState", py::overload_cast<const Slice&, const Slice&>(&Status::IllegalState))
      .def_static("IllegalState", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::IllegalState))
      .def("IsIllegalState", &Status::IsIllegalState)
      .def_static("NotAuthorized", py::overload_cast<const Slice&, const Slice&>(&Status::NotAuthorized))
      .def_static("NotAuthorized", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::NotAuthorized))
      .def("IsNotAuthorized", &Status::IsNotAuthorized)
      .def_static("Aborted", py::overload_cast<const Slice&, const Slice&>(&Status::Aborted))
      .def_static("Aborted", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::Aborted))
      .def("IsAborted", &Status::IsAborted)
      .def_static("RemoteError", py::overload_cast<const Slice&, const Slice&>(&Status::RemoteError))
      .def_static("RemoteError", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::RemoteError))
      .def("IsRemoteError", &Status::IsRemoteError)
      .def_static("ServiceUnavailable", py::overload_cast<const Slice&, const Slice&>(&Status::ServiceUnavailable))
      .def_static("ServiceUnavailable",
                  py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::ServiceUnavailable))
      .def("IsServiceUnavailable", &Status::IsServiceUnavailable)
      .def_static("TimedOut", py::overload_cast<const Slice&, const Slice&>(&Status::TimedOut))
      .def_static("TimedOut", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::TimedOut))
      .def("IsTimedOut", &Status::IsTimedOut)
      .def_static("Uninitialized", py::overload_cast<const Slice&, const Slice&>(&Status::Uninitialized))
      .def_static("Uninitialized", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::Uninitialized))
      .def("IsUninitialized", &Status::IsUninitialized)
      .def_static("ConfigurationError", py::overload_cast<const Slice&, const Slice&>(&Status::ConfigurationError))
      .def_static("ConfigurationError",
                  py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::ConfigurationError))
      .def("IsConfigurationError", &Status::IsConfigurationError)
      .def_static("Incomplete", py::overload_cast<const Slice&, const Slice&>(&Status::Incomplete))
      .def_static("Incomplete", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::Incomplete))
      .def("IsIncomplete", &Status::IsIncomplete)
      .def_static("NotLeader", py::overload_cast<const Slice&, const Slice&>(&Status::NotLeader))
      .def_static("NotLeader", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::NotLeader))
      .def("IsNotLeader", &Status::IsNotLeader)
      .def_static("TxnLockConflict", py::overload_cast<const Slice&, const Slice&>(&Status::TxnLockConflict))
      .def_static("TxnLockConflict", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::TxnLockConflict))
      .def("IsTxnLockConflict", &Status::IsTxnLockConflict)
      .def_static("TxnWriteConflict", py::overload_cast<const Slice&, const Slice&>(&Status::TxnWriteConflict))
      .def_static("TxnWriteConflict", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::TxnWriteConflict))
      .def("IsTxnWriteConflict", &Status::IsTxnWriteConflict)
      .def_static("TxnNotFound", py::overload_cast<const Slice&, const Slice&>(&Status::TxnNotFound))
      .def_static("TxnNotFound", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::TxnNotFound))
      .def("IsTxnNotFound", &Status::IsTxnNotFound)
      .def_static("TxnPrimaryMismatch", py::overload_cast<const Slice&, const Slice&>(&Status::TxnPrimaryMismatch))
      .def_static("TxnPrimaryMismatch",
                  py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::TxnPrimaryMismatch))
      .def("IsTxnPrimaryMismatch", &Status::IsTxnPrimaryMismatch)
      .def_static("TxnRolledBack", py::overload_cast<const Slice&, const Slice&>(&Status::TxnRolledBack))
      .def_static("TxnRolledBack", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::TxnRolledBack))
      .def("IsTxnRolledBack", &Status::IsTxnRolledBack)
      .def_static("NoLeader", py::overload_cast<const Slice&, const Slice&>(&Status::NoLeader))
      .def_static("NoLeader", py::overload_cast<int32_t, const Slice&, const Slice&>(&Status::NoLeader))
      .def("IsNoLeader", &Status::IsNoLeader)
      .def("ToString", &Status::ToString)
      .def("Errno", &Status::Errno);
}
