#!/usr/bin/env python

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

import os

import setuptools
import shutil
import platform
import glob

# make the diongsdk python package dir
shutil.rmtree("dingosdk", ignore_errors=True)
os.mkdir("dingosdk")
shutil.copyfile("__init__.py", "dingosdk/__init__.py")
ext = ".pyd" if platform.system() == "Windows" else ".so"
store_lib = f"dingo_store{ext}"
shutil.copyfile(store_lib, f"dingosdk/{store_lib}")

long_description = """
dingosdk is dingo-store python sdk for efficient interact with the dingo-store cluster. 
It contains RawKV, Txn,  Vector Api.
NOTE: only support LINUX now.
"""
setuptools.setup(
    name="dingosdk",
    version="0.1rc11-12",
    description="dingo-store python sdk",
    license="Apache License 2.0",
    long_description=long_description,
    url="https://www.dingodb.com/",
    project_urls={
        "Homepage": "https://www.dingodb.com/",
        "Documentation": "https://dingodb.readthedocs.io/en/latest/",
    },
    author="DingoDB",
    author_email="dingodb@zetyun.com",
    keywords="dingodb dingo-store",
    python_requires=">=3.7",
    packages=["dingosdk"],
    package_data={
        "dingosdk": ["*.so", "*.pyd"],
    },
    zip_safe=False,
)
