# dingosdk Python 打包指南

## 概述

`dingosdk` 是一个基于 nanobind 的 C++ 扩展包，通过 `scikit-build-core` + `CMake` 进行构建。支持两种场景：

- **本地开发**：editable 模式编译，代码修改无需重复安装
- **CI 发布**：通过 cibuildwheel 构建 manylinux wheel，自动发布到 PyPI

---

## 项目结构

```
dingo-sdk/
├── pyproject.toml            # 构建系统声明 + scikit-build-core 配置 + cibuildwheel 配置
├── CMakeLists.txt            # 顶层 CMake，BUILD_PYTHON_SDK=ON 时启用 python/
├── python/
│   ├── CMakeLists.txt        # Python 扩展的 CMake 配置
│   └── src/                  # nanobind 绑定源码（.cc 文件）
└── .github/workflows/
    ├── wheels.yml            # CI 构建 wheel
    └── upload_package.yml    # CI 发布到 PyPI
```

---

## 依赖说明

### 构建工具

| 依赖 | 用途 |
|------|------|
| scikit-build-core | Python 包构建后端，直接驱动 CMake |
| cmake >= 3.23 | C++ 项目构建 |
| ninja | 并行编译加速（可选，自动检测） |

> scikit-build-core 会自动处理 CMake 调用、输出目录、wheel 打包，无需 `setup.py`。

### C++ 依赖（THIRD_PARTY_INSTALL_PATH）

所有 C++ 依赖（nanobind、brpc、protobuf、gflags 等）统一安装在 `THIRD_PARTY_INSTALL_PATH` 目录下。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `THIRD_PARTY_INSTALL_PATH` | `$HOME/.local/dingo-eureka` | C++ 依赖安装前缀 |

> nanobind 使用 `THIRD_PARTY_INSTALL_PATH` 中的系统安装版本，不再依赖 git submodule。

---

## 本地开发构建

### 前置条件

1. 已安装 cmake >= 3.23、gcc/clang（支持 C++17）
2. 已安装 dingo-eureka 依赖包到 `$HOME/.local/dingo-eureka`（或自定义路径）
3. Python >= 3.9
4. 已安装 scikit-build-core：`pip install scikit-build-core`

### 安装命令

```bash
# 使用默认 THIRD_PARTY_INSTALL_PATH（$HOME/.local/dingo-eureka）
pip install --no-build-isolation -e .

# 自定义依赖路径
THIRD_PARTY_INSTALL_PATH=/custom/path pip install --no-build-isolation -e .
```

> **必须加 `--no-build-isolation`**
> 否则 pip 会创建隔离的临时虚拟环境，导致 `THIRD_PARTY_INSTALL_PATH` 中的 C++ 依赖无法被找到。

### 验证安装

```bash
python3 -c "import dingosdk; print(dingosdk.__file__)"
# 输出类似：/path/to/dingo-sdk/dingosdk.cpython-310-x86_64-linux-gnu.so
```

### Debug 模式编译

```bash
pip install --no-build-isolation -e . --config-settings=cmake.build-type=Debug
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
   pyproject.toml          读取 build-backend = scikit_build_core.build
        │                  读取 [tool.scikit-build.cmake.define] 中的 CMake 参数
        ▼
 scikit-build-core         自动调用 cmake，传入以下参数：
        │                    -DBUILD_PYTHON_SDK=ON
        │                    -DSDK_ENABLE_GRPC=OFF
        │                    -DBUILD_BENCHMARK=OFF
        │                    -DBUILD_INTEGRATION_TESTS=OFF
        │                    -DBUILD_UNIT_TESTS=OFF
        ▼
  根 CMakeLists.txt        编译 sdk 静态库
        │                  add_subdirectory(python)（BUILD_PYTHON_SDK=ON）
        ▼
 python/CMakeLists.txt     查找 THIRD_PARTY_INSTALL_PATH（默认 $HOME/.local/dingo-eureka）
        │                  find_package(nanobind CONFIG REQUIRED)
        ▼
  cmake --build .          编译 src/*.cc → dingosdk.cpython-*.so
        │
        ▼
  install(TARGETS ...)     scikit-build-core 将 .so 打包进 wheel
                           排除 lib/**、include/**（C++ 开发文件，Python 用户不需要）
```

---

## CI 打包发布

### 触发条件

- push 到 `main` 分支 → 构建 wheel + 发布到 PyPI
- PR 到 `main` 分支 → 仅构建 wheel（不发布）

### 构建流程（wheels.yml）

```yaml
- actions/checkout          检出代码
- git submodule update       仅拉取 store-proto 和 serial（nanobind 已改用系统库）
- cibuildwheel@v2.19.1      在自定义 manylinux 镜像内构建
- upload-artifact            保存 .whl 文件
```

cibuildwheel 配置（`pyproject.toml`）：

```toml
[tool.cibuildwheel]
before-build = "rm -rf {project}/build"       # 清理上次构建残留
build = "*-manylinux*"                        # 只构建 manylinux 格式
skip = "*-musllinux*"                         # 跳过 musl libc
manylinux-x86_64-image = "dingodatabase/dingo-eureka:manylinux_2_34"

[tool.cibuildwheel.linux]
archs = ["x86_64"]
```

### 关键：自定义 manylinux 镜像

`dingodatabase/dingo-eureka:manylinux_2_34` 是基于官方 manylinux 镜像定制的，
预装了所有 C++ 依赖（nanobind、brpc、protobuf 等），对应本地的 `dingo-eureka` 依赖包。
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
| `CMAKE_BUILD_PARALLEL_LEVEL` | 本地 | 并行编译线程数 |

> scikit-build-core 额外支持通过 `--config-settings` 传递 CMake 参数，例如：
> ```bash
> pip install --no-build-isolation -e . \
>   --config-settings=cmake.define.THIRD_PARTY_INSTALL_PATH=/custom/path
> ```

---

## 常见问题

**Q: `find_package(nanobind) failed` 报错**

检查 `THIRD_PARTY_INSTALL_PATH` 是否正确，且目录下存在：
```
$THIRD_PARTY_INSTALL_PATH/lib/cmake/nanobind/nanobindConfig.cmake
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
纯 Python 代码修改则无需重新编译。
