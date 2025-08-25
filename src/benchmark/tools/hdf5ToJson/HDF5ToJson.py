# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
"""
向量数据专用的HDF5到JSON转换器
将向量数据转换为id+emb格式的JSON文件
emb列存储为向量数组，与C++基准测试框架兼容
"""

import h5py
import json
import numpy as np
import time
import argparse
from pathlib import Path
import gc
from tqdm import tqdm


def convert_vector_dataset_to_json(
    hdf5_path: str,
    dataset_name: str,
    output_path: str,
    chunk_size: int = 10000,
    pretty_print: bool = False,
):
    """
    将向量数据集转换为id+emb格式的JSON文件

    Args:
        hdf5_path: HDF5文件路径
        dataset_name: 数据集名称 (如 'train', 'test')
        output_path: 输出JSON文件路径
        chunk_size: 分块大小
        pretty_print: 是否格式化输出JSON
    """
    print(f"开始转换向量数据集: {dataset_name}")
    print(f"输入文件: {hdf5_path}")
    print(f"输出文件: {output_path}")
    print(f"分块大小: {chunk_size}")

    start_time = time.time()

    try:
        with h5py.File(hdf5_path, "r") as f:
            # 检查数据集是否存在
            if dataset_name not in f:
                print(f"错误: HDF5文件中没有找到'{dataset_name}'数据集")
                available_datasets = list(f.keys())
                print(f"可用的数据集: {available_datasets}")
                return False

            dataset = f[dataset_name]
            print(f"{dataset_name}数据集信息:")
            print(f"  形状: {dataset.shape}")
            print(f"  数据类型: {dataset.dtype}")
            print(f"  总大小: {dataset.nbytes / 1024 / 1024:.1f} MB")

            # 检查数据集维度 - 应该是2D (样本数, 向量维度)
            if len(dataset.shape) != 2:
                print(f"错误: 期望2D向量数据集，但得到形状: {dataset.shape}")
                return False

            total_rows, vector_dim = dataset.shape
            print(f"总样本数: {total_rows}, 向量维度: {vector_dim}")

            # 创建输出目录
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            # 如果数据集较小，直接转换
            if total_rows <= chunk_size:
                print("数据集较小，使用直接转换...")
                vectors = dataset[...]

                # 创建JSON数据
                json_data = []
                for i, vector in enumerate(tqdm(vectors, desc="转换向量")):
                    json_obj = {
                        "id": int(i),  # 从0开始的ID
                        "emb": vector.astype(float).tolist(),  # 转换为Python列表
                    }
                    json_data.append(json_obj)

                # 写入JSON文件
                with open(output_path, "w", encoding="utf-8") as json_file:
                    if pretty_print:
                        json.dump(json_data, json_file, indent=2, ensure_ascii=False)
                    else:
                        json.dump(
                            json_data,
                            json_file,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )

            else:
                print(f"使用分块处理，总行数: {total_rows}, 分块大小: {chunk_size}")

                # 计算分块数量
                num_chunks = (total_rows + chunk_size - 1) // chunk_size
                print(f"总分块数: {num_chunks}")

                # 流式写入JSON文件
                with open(output_path, "w", encoding="utf-8") as json_file:
                    json_file.write("[\n")  # 开始JSON数组

                    for chunk_idx in range(num_chunks):
                        start_row = chunk_idx * chunk_size
                        end_row = min(start_row + chunk_size, total_rows)

                        print(
                            f"处理分块 {chunk_idx + 1}/{num_chunks}: 行 {start_row}-{end_row-1}"
                        )

                        # 读取当前分块
                        chunk_vectors = dataset[start_row:end_row, :]

                        # 处理当前分块的向量
                        for i, vector in enumerate(chunk_vectors):
                            vector_id = start_row + i
                            json_obj = {
                                "id": int(vector_id),
                                "emb": vector.astype(float).tolist(),
                            }

                            # 写入JSON对象
                            if pretty_print:
                                json_line = json.dumps(
                                    json_obj, indent=2, ensure_ascii=False
                                )
                                # 调整缩进以适应数组格式
                                json_line = "\n".join(
                                    "  " + line for line in json_line.split("\n")
                                )
                            else:
                                json_line = json.dumps(
                                    json_obj, separators=(",", ":"), ensure_ascii=False
                                )
                                json_line = f"  {json_line}"

                            # 除了最后一个元素，都要加逗号
                            if vector_id < total_rows - 1:
                                json_line += ","

                            json_file.write(json_line + "\n")

                        # 定期垃圾回收
                        if chunk_idx % 10 == 0:
                            gc.collect()

                    json_file.write("]")  # 结束JSON数组

        # 转换完成
        total_time = time.time() - start_time
        print(f"\n✅ 转换成功完成!")
        print(f"总用时: {total_time:.1f}秒")

        # 验证输出文件
        try:
            # 显示文件大小
            file_size = Path(output_path).stat().st_size / 1024 / 1024
            print(f"输出文件大小: {file_size:.1f} MB")

            # 读取并验证JSON格式
            with open(output_path, "r", encoding="utf-8") as f:
                # 只读取前1KB来验证格式
                sample = f.read(1024)
                if (
                    sample.strip().startswith("[")
                    and "id" in sample
                    and "emb" in sample
                ):
                    print("JSON格式验证: ✅ 格式正确")
                else:
                    print("JSON格式验证: ❌ 格式可能有问题")

            # 尝试加载完整JSON来验证
            print("验证JSON完整性...")
            with open(output_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                print(f"验证结果: {len(data)} 条记录")

                if len(data) > 0:
                    print(f"\n数据预览 (前2条):")
                    for i in range(min(2, len(data))):
                        item = data[i]
                        print(
                            f"  记录 {i}: ID={item['id']}, 向量维度={len(item['emb'])}"
                        )
                        print(f"    向量前10个元素: {item['emb'][:10]}")

        except Exception as e:
            print(f"验证输出文件时出错: {e}")

        return True

    except Exception as e:
        print(f"转换过程中出错: {e}")
        return False


def convert_neighbors_to_json(
    hdf5_path: str,
    output_path: str = "neighbors.json",
    chunk_size: int = 1000,
    pretty_print: bool = False,
    max_neighbors: int = 100,
):  # 添加邻居数量限制
    """
    专门转换neighbors数据集到JSON格式
    """
    print(f"开始转换neighbors数据集...")
    print(f"输入文件: {hdf5_path}")
    print(f"输出文件: {output_path}")
    print(f"分块大小: {chunk_size}")
    print(f"最大邻居数: {max_neighbors}")  # 新增日志

    start_time = time.time()

    try:
        with h5py.File(hdf5_path, "r") as f:
            if "neighbors" not in f:
                print("错误: HDF5文件中没有找到'neighbors'数据集")
                return False

            neighbors_dataset = f["neighbors"]
            print(f"neighbors数据集信息:")
            print(f"  形状: {neighbors_dataset.shape}")
            print(f"  数据类型: {neighbors_dataset.dtype}")

            total_rows, total_cols = neighbors_dataset.shape
            if total_cols < max_neighbors:
                max_neighbors = total_cols
                print(
                    f"原始邻居数: {total_cols} < max_neighbors: {max_neighbors} ，继续使用原始邻居数: {total_cols}"
                )
            else:
                print(f"原始邻居数: {total_cols}, 限制为: {max_neighbors}")

            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            if total_rows <= chunk_size:
                # 小数据集直接转换
                data = neighbors_dataset[...]

                json_data = []
                for i, neighbors_row in enumerate(tqdm(data, desc="转换neighbors")):
                    # 限制邻居数量
                    limited_neighbors = (
                        neighbors_row[:max_neighbors].astype(int).tolist()
                    )
                    json_obj = {
                        "id": int(i),
                        "neighbors_id": limited_neighbors,  # 使用限制后的邻居列表
                    }
                    json_data.append(json_obj)

                # 写入JSON文件
                with open(output_path, "w", encoding="utf-8") as json_file:
                    if pretty_print:
                        json.dump(json_data, json_file, indent=2, ensure_ascii=False)
                    else:
                        json.dump(
                            json_data,
                            json_file,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )

            else:
                # 大数据集分块处理
                num_chunks = (total_rows + chunk_size - 1) // chunk_size
                print(f"总分块数: {num_chunks}")

                with open(output_path, "w", encoding="utf-8") as json_file:
                    json_file.write("[\n")  # 开始JSON数组

                    for chunk_idx in range(num_chunks):
                        start_row = chunk_idx * chunk_size
                        end_row = min(start_row + chunk_size, total_rows)

                        print(
                            f"处理分块 {chunk_idx + 1}/{num_chunks}: 行 {start_row}-{end_row-1}"
                        )

                        chunk_data = neighbors_dataset[start_row:end_row, :]

                        for i, neighbors_row in enumerate(chunk_data):
                            query_id = start_row + i
                            # 限制邻居数量
                            limited_neighbors = (
                                neighbors_row[:max_neighbors].astype(int).tolist()
                            )
                            json_obj = {
                                "id": int(query_id),
                                "neighbors": limited_neighbors,  # 使用限制后的邻居列表
                            }

                            # 写入JSON对象
                            if pretty_print:
                                json_line = json.dumps(
                                    json_obj, indent=2, ensure_ascii=False
                                )
                                json_line = "\n".join(
                                    "  " + line for line in json_line.split("\n")
                                )
                            else:
                                json_line = json.dumps(
                                    json_obj, separators=(",", ":"), ensure_ascii=False
                                )
                                json_line = f"  {json_line}"

                            # 除了最后一个元素，都要加逗号
                            if query_id < total_rows - 1:
                                json_line += ","

                            json_file.write(json_line + "\n")

                    json_file.write("]")  # 结束JSON数组

        total_time = time.time() - start_time
        print(f"✅ neighbors转换完成! 用时: {total_time:.1f}秒")
        return True

    except Exception as e:
        print(f"转换neighbors时出错: {e}")
        return False


def parse_datasets(datasets_str: str) -> list:
    """解析数据集字符串"""
    if not datasets_str:
        return []
    return [d.strip() for d in datasets_str.split(",") if d.strip()]


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="向量数据专用HDF5到JSON转换器")
    parser.add_argument("hdf5_file", help="输入的HDF5文件路径")
    parser.add_argument(
        "-d",
        "--datasets",
        default="train,test,neighbors",
        help="要转换的多个数据集名称 (默认: train,test,neighbors)",
    )
    parser.add_argument(
        "-o", "--outputdir", default=".", help="输出JSON文件路径 (默认: .)"
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        type=int,
        default=10000,
        help="分块大小，控制内存使用 (默认: 10000)",
    )
    parser.add_argument(
        "--max-neighbors", type=int, default=100, help="最大邻居数量 (默认: 100)"
    )
    parser.add_argument(
        "--pretty", action="store_true", default=False, help="格式化输出JSON"
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        default=False,
        help="预览HDF5文件中的数据集信息",
    )

    args = parser.parse_args()

    # 检查输入文件
    if not Path(args.hdf5_file).exists():
        print(f"错误: 文件不存在 - {args.hdf5_file}")
        return 1

    # 如果需要预览
    if args.preview:
        print("预览HDF5文件中的数据集...")
        try:
            with h5py.File(args.hdf5_file, "r") as f:
                print(f"文件: {args.hdf5_file}")
                print("数据集列表:")
                for name, dataset in f.items():
                    if isinstance(dataset, h5py.Dataset):
                        size_mb = dataset.nbytes / 1024 / 1024
                        print(
                            f"  {name}: {dataset.shape} {dataset.dtype} ({size_mb:.1f} MB)"
                        )
                print()
                return 0
        except Exception as e:
            print(f"读取HDF5文件时出错: {e}")
            return 1

    # 解析数据集列表
    datasets = parse_datasets(args.datasets)
    if not datasets:
        print("错误: 未指定有效的数据集")
        return 1

    all_success = True
    for dataset in datasets:
        dataset_output = Path(args.outputdir) / f"{dataset}.json"
        print(f"转换数据集 {dataset} 到: {dataset_output}")

        if dataset == "neighbors":
            success = convert_neighbors_to_json(
                args.hdf5_file,
                dataset_output,
                args.chunk_size,
                args.pretty,
                args.max_neighbors,
            )
        else:
            success = convert_vector_dataset_to_json(
                args.hdf5_file, dataset, dataset_output, args.chunk_size, args.pretty
            )

        if success:
            print(f"\n🎉 转换完成! 输出文件: {dataset_output}")
        else:
            print(f"数据集 {dataset} 转换失败")
            all_success = False
            return 1

    if all_success:
        print(f"\n🎉 所有转换完成! 输出目录: {args.outputdir}")

    return 0 if all_success else 1


if __name__ == "__main__":
    exit(main())
