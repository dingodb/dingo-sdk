# HDF5 to JSON 转换工具

这个工具集用于将HDF5格式的向量数据集转换为JSON格式，并生成标量过滤数据集。



## 环境要求

### Python版本

- Python 3.8 或更高版本

### 依赖包安装

```bash
# 使用pip安装
pip install h5py numpy faiss-cpu tqdm pathlib

# 或者使用conda安装
conda install h5py numpy faiss-cpu tqdm
conda install -c conda-forge pathlib
```



## 快速开始

### 文件目录

```
hdf5ToJson/ 
├── HDF5ToJson.py                  # HDF5 转换为 JSON
├── TrainAndTestToScalarFilter.py  # 生成标量数据集 
├── TrainAndTestToIdFilter.py      # 生成 id 过滤数据集
```

#### HDF5ToJson.py 参数说明

```
python HDF5ToJson.py <hdf5_file> [选项]

必选参数:
  hdf5_file             输入的HDF5文件路径

可选参数:
  -d, --datasets        要转换的数据集名称，多个用逗号分隔 (默认: test,train,neighbors)
  -o, --outputdir       输出目录路径 (默认: ".")
  --max-neighbors       最大邻居数量 (默认: 100)
  --preview             预览HDF5文件中的数据集信息 (默认: False)
  --pretty              格式化输出JSON (默认: False)
  -c, --chunk-size      分块大小，控制内存使用 (默认: 10000)
```

#### TrainAndTestToScalarFilter.py 参数说明

```
python TrainAndTestToScalarFilter.py [选项]

可选参数:
  --data-dir                 输入数据目录，包含train.json和test.json (默认: ".")
  --output-dir               输出目录 (默认: 与输入目录 data-dir 相同)
  --max-neighbors            邻居数量 (默认: 100)
  --only-base-neighbors      是否只计算基础 neighbor，不计算 scalar (默认: False)
  --compute-base-neighbors   是否在计算 scalar 的同时计算基础 neighbors (默认: False)
  --percentages              scalar 百分比数组，用逗号分隔，总和应 <= 100 (默认: 10,90）
```

#### TrainAndTestToIdFilter.py 参数说明

```
python TrainAndTestToIdFilter.py [选项]

可选参数:
  --data-dir           输入数据目录，包含train.json和test.json (默认: ".")
  --output-dir         输出目录 (默认: 与输入目录 data-dir 相同)
  --max-neighbors      邻居数量 (默认: 100)
  --id-threshold       过滤 id 的阈值（默认: 0）   
```



### 流程示例

#### 1. 首先预览HDF5文件，了解包含哪些数据集

```
$ python HDF5ToJson.py your_dataset.hdf5 --preview

输出示例：
预览HDF5文件中的数据集...
文件: your_dataset.hdf5
数据集列表:
  distances: (10000, 5000) float32 (190.7 MB)
  neighbors: (10000, 5000) uint32 (190.7 MB)
  test: (10000, 768) float32 (29.3 MB)
  train: (1000000, 768) float32 (2929.7 MB)
```

#### 2. 批量转换数据集

```
$ python HDF5ToJson.py your_dataset.hdf5 --datasets test,train,neighbors --outputdir "."

# 生成的文件
$ ls -l
-rw-r--r--. 1 root root   6.9M Aug 28 11:09 neighbors.json
-rw-r--r--. 1 root root   140M Aug 28 11:02 test.json
-rw-r--r--. 1 root root    14G Aug 28 11:07 train.json
```

#### 3. 如果 hdf5 中不含有 neighbors 且不需要生成 scalar 数据，通过计算生成 neighbors（可选）

```
$ python TrainAndTestToScalarFilter.py --only-base-neighbors
```

#### 4. 生成标量过滤数据集（可选）

```
$ python TrainAndTestToScalarFilter.py --percentages 5,15,30,50 --data-dir "."

# 生成的文件
$  ls -l
-rw-r--r--. 1 zetyun zetyun 6.9M Aug 28 14:43 neighbors_labels_label_15p.json
-rw-r--r--. 1 zetyun zetyun 6.9M Aug 28 14:46 neighbors_labels_label_30p.json
-rw-r--r--. 1 zetyun zetyun 6.9M Aug 28 14:50 neighbors_labels_label_50p.json
-rw-r--r--. 1 zetyun zetyun 6.9M Aug 28 14:42 neighbors_labels_label_5p.json
-rw-r--r--. 1 zetyun zetyun  34M Aug 28 14:41 scalar_labels.json
-rw-r--r--. 1 zetyun zetyun 140M Aug 28 11:02 test.json
-rw-r--r--. 1 zetyun zetyun  14G Aug 28 11:07 train.json
```

#### 5. 生成 id 过滤的数据集（可选）

```
$ python TrainAndTestToIdFilter.py --id-threshold 90 --data-dir "."

# 生成的文件
$  ls -l
-rw-r--r--. 1 zetyun zetyun 7.0M Aug 28 15:00 neighbors_int_90.json
-rw-r--r--. 1 zetyun zetyun 140M Aug 28 11:02 test.json
-rw-r--r--. 1 zetyun zetyun  14G Aug 28 11:07 train.json
```