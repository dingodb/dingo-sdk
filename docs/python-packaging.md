# dingosdk Python 打包指南

## 概述

`dingosdk` 是一个基于 pybind11 的 C++ 扩展包，通过 `setuptools` + `CMake` 进行构建。支持两种场景：

- **本地开发**：editable 模式编译，代码修改无需重复安装
- **CI 发布**：通过 cibuildwheel 构建 manylinux wheel，自动发布到 PyPI

---

## 项目结构

```
dingo-sdk/
├── setup.py                  # 构建入口，定义 CMake 调用逻辑
├── pyproject.toml            # 构建系统声明 + cibuildwheel 配置
├── CMakeLists.txt            # 顶层 CMake，BUILD_PYTHON_SDK=ON 时启用 python/
├── python/
│   ├── CMakeLists.txt        # Python 扩展的 CMake 配置
│   └── src/                  # pybind11 绑定源码（.cc 文件）
└── .github/workflows/
    ├── wheels.yml            # CI 构建 wheel
    └── upload_package.yml    # CI 发布到 PyPI
```

---

## 依赖说明

### 构建工具（pyproject.toml 声明）

| 依赖 | 版本 | 用途 |
|------|------|------|
| setuptools | >=42 | Python 包构建后端 |
| cmake | >=3.30.1 | C++ 项目构建 |
| ninja | - | 并行编译加速 |
| wheel | - | 生成 .whl 文件 |

### C++ 依赖（THIRD_PARTY_INSTALL_PATH）

所有 C++ 依赖（pybind11、gRPC、protobuf、gflags 等）统一安装在 `THIRD_PARTY_INSTALL_PATH` 目录下。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `THIRD_PARTY_INSTALL_PATH` | `$HOME/.local/dingo-eureka` | C++ 依赖安装前缀 |

> pybind11 使用系统安装版本（位于 `$THIRD_PARTY_INSTALL_PATH/share/cmake/pybind11`），
> 不再依赖 git submodule。

---

## 本地开发构建

### 前置条件

1. 已安装 cmake >= 3.23、ninja、gcc/clang（支持 C++17）
2. 已安装 dingo-eureka 依赖包到 `$HOME/.local/dingo-eureka`（或自定义路径）
3. Python >= 3.9

### 安装命令

```bash
# 使用默认 THIRD_PARTY_INSTALL_PATH（$HOME/.local/dingo-eureka）
pip install --no-build-isolation -e .

# 自定义依赖路径
THIRD_PARTY_INSTALL_PATH=/custom/path pip install --no-build-isolation -e .
```

> **必须加 `--no-build-isolation`**
> 否则 pip 会创建隔离的临时虚拟环境，找不到 `THIRD_PARTY_INSTALL_PATH` 里的 C++ 依赖。

### 验证安装

```bash
python3 -c "import dingosdk; print(dingosdk.__file__)"
# 输出类似：/path/to/dingo-sdk/dingosdk.cpython-310-x86_64-linux-gnu.so
```

### Debug 模式编译

```bash
DEBUG=1 pip install --no-build-isolation -e .
```

### 并行编译加速

```bash
CMAKE_BUILD_PARALLEL_LEVEL=8 pip install --no-build-isolation -e .
```

---

## 构建流程详解

```
pip install --no-build-isolation -e .
        │
        ▼
   pyproject.toml          读取 build-system.requires
        │                  安装 setuptools/cmake/ninja（当前环境）
        ▼
     setup.py              CMakeExtension("dingosdk") + CMakeBuild
        │
        ▼
   cmake <sourcedir>       传入以下参数：
        │                    -DBUILD_PYTHON_SDK=ON
        │                    -DSDK_ENABLE_GRPC=OFF
        │                    -DBUILD_BENCHMARK=OFF
        │                    -DBUILD_INTEGRATION_TESTS=OFF
        │                    -DBUILD_UNIT_TESTS=OFF
        │                    -DTHIRD_PARTY_INSTALL_PATH=...（若设置了环境变量）
        ▼
 python/CMakeLists.txt     find_package(pybind11 REQUIRED)
        │                    搜索路径：THIRD_PARTY_INSTALL_PATH/share/cmake/pybind11
        │                    默认：$HOME/.local/dingo-eureka
        ▼
  cmake --build .           编译 src/*.cc → dingosdk.cpython-*.so
```

---

## CI 打包发布

### 触发条件

- push 到 `main` 分支 → 构建 wheel + 发布到 PyPI
- PR 到 `main` 分支 → 仅构建 wheel（不发布）

### 构建流程（wheels.yml）

```yaml
- actions/checkout          检出代码
- cibuildwheel@v2.19.1      在自定义 manylinux 镜像内构建
- upload-artifact            保存 .whl 文件
```

cibuildwheel 配置（`pyproject.toml`）：

```toml
[tool.cibuildwheel]
before-build = "rm -rf {project}/build"           # 清理上次构建残留
build = "*-manylinux*"                            # 只构建 manylinux 格式
skip = "*-musllinux*"                             # 跳过 musl libc
manylinux-x86_64-image = "dingodatabase/dingo-eureka:manylinux_2_34-v1.0"

[tool.cibuildwheel.linux]
archs = ["x86_64"]
```

### 关键：自定义 manylinux 镜像

`dingodatabase/dingo-eureka:manylinux_2_34-v1.0` 是基于官方 manylinux 镜像定制的，
预装了所有 C++ 依赖（pybind11、gRPC 等），对应本地的 `dingo-eureka` 依赖包。
因此 CI 内无需额外设置 `THIRD_PARTY_INSTALL_PATH`。

### 发布流程（upload_package.yml）

```
Build_wheel 成功 + 来自 main 分支的 push
        │
        ▼
  下载 .whl artifact
        │
        ▼
  twine upload *.whl  →  PyPI
        │
  认证：TWINE_USERNAME=__token__
        TWINE_PASSWORD=${{ secrets.PYPI_API_TOKEN }}
```

---

## 环境变量参考

| 变量 | 场景 | 说明 |
|------|------|------|
| `THIRD_PARTY_INSTALL_PATH` | 本地 / CI | C++ 依赖路径，默认 `$HOME/.local/dingo-eureka` |
| `DEBUG` | 本地 | 设为 `1` 时使用 Debug 模式编译 |
| `CMAKE_GENERATOR` | 本地 | 覆盖 CMake 生成器，默认自动选择 Ninja |
| `CMAKE_ARGS` | 本地 / CI | 追加额外的 CMake 参数 |
| `CMAKE_BUILD_PARALLEL_LEVEL` | 本地 | 并行编译线程数 |

---

## 常见问题

**Q: `find_package(pybind11) failed` 报错**

检查 `THIRD_PARTY_INSTALL_PATH` 是否正确，且目录下存在：
```
$THIRD_PARTY_INSTALL_PATH/share/cmake/pybind11/pybind11Config.cmake
```

**Q: 编译后 import 报符号找不到**

确认编译时的 Python 版本与运行时一致：
```bash
python3 --version
ls dingosdk.cpython-*.so   # 文件名中的版本号应匹配
```

**Q: 修改了 C++ 代码后如何重新编译**

editable 模式下，修改 C++ 代码需要重新执行安装命令：
```bash
pip install --no-build-isolation -e .
```
修改纯 Python 代码则无需重新编译。
