# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.23.1 FATAL_ERROR)
project(pysdk)

find_package(SWIG REQUIRED)

if(SWIG_FOUND)
  message("SWIG found: ${SWIG_EXECUTABLE}, dir: ${SWIG_DIR}, version: ${SWIG_VERSION}")
endif()

# Find Python 3
find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

include(UseSWIG)

set(CMAKE_SWIG_FLAGS)
set(SWIG_SOURCE_FILE_EXTENSIONS swg)

list(APPEND CMAKE_SWIG_FLAGS "-DSWIGWORDSIZE64" "-c++")

set_property(SOURCE dingosdk.swg PROPERTY CPLUSPLUS ON)

swig_add_library(dingosdk 
  TYPE SHARED 
  LANGUAGE python 
  SOURCES dingosdk.swg
)

target_include_directories(dingosdk 
  PRIVATE 
  ${Python3_INCLUDE_DIRS}
)

target_link_libraries(dingosdk
  PRIVATE
  sdk
)

configure_file(setup.py setup.py COPYONLY)
configure_file(__init__.py __init__.py COPYONLY)
configure_file(loader.py loader.py COPYONLY)

configure_file(examples/pysdk_rawkv_example.py pysdk_rawkv_example.py COPYONLY)
configure_file(examples/pysdk_vector_example.py pysdk_vector_example.py COPYONLY)

