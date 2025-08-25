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
标量数据集生成器 - JSON版本
从JSON格式的向量数据生成标量过滤数据集和对应的邻居数据集
与C++基准测试框架兼容
"""

import json
import numpy as np
import faiss
from tqdm import tqdm
import logging
from pathlib import Path
from typing import List, Dict, Tuple
import gc

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ScalarDatasetGenerator:
    def __init__(self, data_dir: str = ".", output_dir: str = None):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir) if output_dir else self.data_dir
        self.train_data = None
        self.test_data = None

        # 创建输出目录
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Output directory: {self.output_dir}")

    def load_data(self):
        """加载JSON数据"""
        logger.info("Loading data...")

        # 读取训练和测试数据
        train_path = self.data_dir / "train.json"
        test_path = self.data_dir / "test.json"

        if not train_path.exists():
            raise FileNotFoundError(f"Training data file not found: {train_path}")
        if not test_path.exists():
            raise FileNotFoundError(f"Testing data file not found: {test_path}")

        with open(train_path, "r", encoding="utf-8") as f:
            self.train_data = json.load(f)

        with open(test_path, "r", encoding="utf-8") as f:
            self.test_data = json.load(f)

        logger.info(f"Training data: {len(self.train_data)} records")
        logger.info(f"Testing data: {len(self.test_data)} records")
        logger.info(
            f"Training data keys: {list(self.train_data[0].keys()) if self.train_data else []}"
        )
        logger.info(
            f"Vector dimension: {len(self.train_data[0]['emb']) if self.train_data else 0}"
        )

    def validate_percentages(self, percentages: List[int]) -> bool:
        """验证百分比数组"""
        total = sum(percentages)
        if total > 100:
            logger.error(f"Percentages sum ({total}) exceeds 100%")
            return False
        if total < 100:
            logger.warning(
                f"Percentages sum ({total}) is less than 100%, remaining {100-total}% will be unlabeled"
            )
        if any(p <= 0 for p in percentages):
            logger.error("All percentages must be positive")
            return False
        return True

    def generate_scalar_datasets(self, percentages: List[int]):
        """生成标量过滤数据集"""
        logger.info(f"Generating scalar datasets with percentages: {percentages}")

        if not self.validate_percentages(percentages):
            raise ValueError("Invalid percentages")

        train_ids = [item["id"] for item in self.train_data]
        total_count = len(train_ids)
        np.random.seed(42)  # 确保可重现性

        # 创建标签映射
        labels = ["label_foo"] * total_count  # 默认标签
        used_indices = set()

        # 按照百分比分配标签
        label_info = {}
        for i, percentage in enumerate(percentages):
            label_name = f"label_{percentage}p"
            count = int(total_count * percentage / 100)

            # 从未使用的索引中随机选择
            available_indices = [
                idx for idx in range(total_count) if idx not in used_indices
            ]
            if len(available_indices) < count:
                count = len(available_indices)
                logger.warning(
                    f"Not enough available data for {percentage}%, using {count} samples"
                )

            selected_indices = np.random.choice(
                available_indices, size=count, replace=False
            )

            # 更新标签和已使用索引
            for idx in selected_indices:
                labels[idx] = label_name
                used_indices.add(idx)

            label_info[label_name] = count
            logger.info(f"Assigned {count} samples to {label_name}")

        # 创建scalar数据
        scalar_data = []
        for i, train_id in enumerate(train_ids):
            scalar_data.append({"id": train_id, "labels": labels[i]})

        # 生成文件名
        # percentages_str = "_".join([f"{p}p" for p in percentages])
        scalar_filename = f"scalar_labels.json"

        # 保存文件到输出目录
        scalar_path = self.output_dir / scalar_filename
        with open(scalar_path, "w", encoding="utf-8") as f:
            json.dump(scalar_data, f, separators=(",", ":"), ensure_ascii=False)
        logger.info(f"Saved {scalar_filename} to: {scalar_path}")

        # 显示分配统计
        logger.info("Label distribution:")
        for label_name, count in label_info.items():
            logger.info(
                f"  {label_name}: {count} samples ({count/total_count*100:.1f}%)"
            )

        unlabeled_count = total_count - sum(label_info.values())
        if unlabeled_count > 0:
            logger.info(
                f"  unlabeled: {unlabeled_count} samples ({unlabeled_count/total_count*100:.1f}%)"
            )

        return scalar_data, percentages

    def normalize_vectors(self, vectors: np.ndarray) -> np.ndarray:
        """向量归一化用于余弦相似度"""
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        # 避免除零
        norms = np.where(norms == 0, 1, norms)
        return vectors / norms

    def build_faiss_index(
        self, vectors: np.ndarray, use_gpu: bool = False
    ) -> faiss.Index:
        """构建FAISS索引进行向量搜索"""
        vectors = vectors.astype(np.float32)

        # L2归一化用于余弦距离
        vectors_normalized = self.normalize_vectors(vectors)

        d = vectors.shape[1]

        # 使用Flat索引进行精确搜索
        index = faiss.IndexFlatIP(d)  # 内积用于余弦相似度

        index.add(vectors_normalized)
        return index

    def compute_neighbors(
        self,
        query_vectors: np.ndarray,
        database_vectors: np.ndarray,
        max_neighbors: int = 100,
    ) -> np.ndarray:
        """计算最近邻"""
        logger.info(
            f"Computing nearest neighbors: query={len(query_vectors)}, database={len(database_vectors)}, max_neighbors={max_neighbors}"
        )

        # 检查边界条件
        if len(database_vectors) == 0:
            raise ValueError("Database vectors are empty, cannot compute neighbors")

        # 调整k值到数据库大小
        actual_k = min(max_neighbors, len(database_vectors))
        if actual_k < max_neighbors:
            logger.warning(
                f"Database size({len(database_vectors)}) smaller than max_neighbors({max_neighbors}), using max_neighbors={actual_k}"
            )

        # 构建索引
        index = self.build_faiss_index(database_vectors)

        # 归一化查询向量
        query_vectors_normalized = self.normalize_vectors(
            query_vectors.astype(np.float32)
        )

        # 搜索
        distances, indices = index.search(query_vectors_normalized, actual_k)

        # 验证索引有效性
        max_index = np.max(indices)
        if max_index >= len(database_vectors):
            logger.error(
                f"FAISS returned invalid index: max_index={max_index}, database_size={len(database_vectors)}"
            )
            raise ValueError(f"FAISS returned index out of range")

        return indices

    def generate_neighbors_datasets(
        self,
        scalar_data: List[Dict],
        percentages: List[int],
        max_neighbors=100,
        compute_base_neighbors=False,
    ):
        """生成带标量过滤的邻居数据集"""
        logger.info("Generating neighbor datasets...")

        # 转换embedding向量为numpy数组
        train_embeddings = np.array([item["emb"] for item in self.train_data])
        test_embeddings = np.array([item["emb"] for item in self.test_data])
        test_ids = [item["id"] for item in self.test_data]
        train_ids = [item["id"] for item in self.train_data]

        # 1. 生成基础 neighbors.json（使用所有训练数据）
        if compute_base_neighbors:
            logger.info("Computing base neighbors...")
            base_neighbors_indices = self.compute_neighbors(
                test_embeddings, train_embeddings, max_neighbors=max_neighbors
            )
            base_neighbors_ids = np.array(train_ids)[base_neighbors_indices]

            neighbors_data = []
            for i, test_id in enumerate(test_ids):
                neighbors_data.append(
                    {"id": test_id, "neighbors": base_neighbors_ids[i].tolist()}
                )

            neighbors_path = self.output_dir / "neighbors.json"
            with open(neighbors_path, "w", encoding="utf-8") as f:
                json.dump(neighbors_data, f, separators=(",", ":"), ensure_ascii=False)
            logger.info(
                f"Saved neighbors.json to: {neighbors_path} ({len(neighbors_data)} records)"
            )

        # 创建ID到索引的映射
        train_id_to_idx = {train_id: idx for idx, train_id in enumerate(train_ids)}

        # 为每个百分比生成对应的neighbors文件
        for percentage in percentages:
            label_name = f"label_{percentage}p"
            logger.info(f"Computing {percentage}% scalar filtered neighbors...")

            # 过滤指定标签的训练数据
            filtered_train_ids = [
                item["id"] for item in scalar_data if item["labels"] == label_name
            ]

            if len(filtered_train_ids) == 0:
                logger.warning(f"No training data found for {label_name}, skipping...")
                continue

            logger.info(
                f"{percentage}% filter result: {len(filtered_train_ids)} training vectors"
            )

            filtered_indices = [
                train_id_to_idx[train_id] for train_id in filtered_train_ids
            ]
            filtered_embeddings = train_embeddings[filtered_indices]

            neighbors_indices = self.compute_neighbors(
                test_embeddings, filtered_embeddings, max_neighbors=max_neighbors
            )
            neighbors_ids = np.array(filtered_train_ids)[neighbors_indices]

            neighbors_data_filtered = []
            for i, test_id in enumerate(test_ids):
                neighbors_data_filtered.append(
                    {"id": test_id, "neighbors_id": neighbors_ids[i].tolist()}
                )

            neighbors_filename = f"neighbors_labels_label_{percentage}p.json"
            neighbors_path = self.output_dir / neighbors_filename
            with open(neighbors_path, "w", encoding="utf-8") as f:
                json.dump(
                    neighbors_data_filtered,
                    f,
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
            logger.info(
                f"Saved {neighbors_filename} to: {neighbors_path} ({len(neighbors_data_filtered)} records)"
            )

        # 清理内存
        del train_embeddings, test_embeddings
        gc.collect()

    def run(
        self,
        percentages: List[int],
        max_neighbors=100,
        compute_base_neighbors=False,
        only_base_neighbors=False,
    ):
        """主执行流程"""
        # 加载数据
        self.load_data()

        if only_base_neighbors:
            # 仅生成基础neighbors模式
            logger.info("Starting base neighbors only generation...")

            # 转换embedding向量为numpy数组
            train_embeddings = np.array([item["emb"] for item in self.train_data])
            test_embeddings = np.array([item["emb"] for item in self.test_data])
            test_ids = [item["id"] for item in self.test_data]
            train_ids = [item["id"] for item in self.train_data]

            # 生成基础 neighbors.json
            logger.info("Computing base neighbors...")
            base_neighbors_indices = self.compute_neighbors(
                test_embeddings, train_embeddings, max_neighbors=max_neighbors
            )
            base_neighbors_ids = np.array(train_ids)[base_neighbors_indices]

            neighbors_data = []
            for i, test_id in enumerate(test_ids):
                neighbors_data.append(
                    {"id": test_id, "neighbors_id": base_neighbors_ids[i].tolist()}
                )

            neighbors_path = self.output_dir / "neighbors.json"
            with open(neighbors_path, "w", encoding="utf-8") as f:
                json.dump(neighbors_data, f, separators=(",", ":"), ensure_ascii=False)
            logger.info(
                f"Saved neighbors.json to: {neighbors_path} ({len(neighbors_data)} records)"
            )

            # 清理内存
            del train_embeddings, test_embeddings
            gc.collect()

            logger.info("Base neighbors generation completed!")

            # 显示生成的文件
            output_files = ["neighbors.json"]
        else:
            # 正常模式：生成scalar数据集和对应的neighbors
            logger.info(
                f"Starting scalar dataset generation with percentages: {percentages}"
            )

            # 生成标量数据集
            scalar_data, percentages = self.generate_scalar_datasets(percentages)

            # 生成邻居数据集
            self.generate_neighbors_datasets(
                scalar_data,
                percentages,
                max_neighbors=max_neighbors,
                compute_base_neighbors=compute_base_neighbors,
            )

            logger.info("All datasets generation completed!")

            # 显示生成的文件
            # percentages_str = "_".join([f"{p}p" for p in percentages])
            output_files = [f"scalar_labels.json"]
            if compute_base_neighbors:
                output_files.append("neighbors.json")
            output_files.extend(
                [f"neighbors_labels_label_{p}p.json" for p in percentages]
            )

        logger.info("Generated files:")
        total_size = 0
        for file in output_files:
            file_path = self.output_dir / file
            if file_path.exists():
                file_size = file_path.stat().st_size / 1024 / 1024
                total_size += file_size
                logger.info(f"  ✓ {file} ({file_size:.1f} MB)")
            else:
                logger.info(f"  ✗ {file} (not generated)")

        logger.info(f"Total file size: {total_size:.1f} MB")
        logger.info(f"All files saved to: {self.output_dir}")


def parse_percentages(percentages_str: str) -> List[int]:
    """解析百分比字符串"""
    try:
        percentages = [int(p.strip()) for p in percentages_str.split(",")]
        return percentages
    except ValueError as e:
        raise ValueError(
            f"Invalid percentages format: {percentages_str}. Use comma-separated integers like '10,20,70'"
        )


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="标量数据集生成器 - JSON版本")
    parser.add_argument(
        "--data-dir",
        default=".",
        help="输入数据目录，包含train.json和test.json (默认: 当前目录)",
    )
    parser.add_argument(
        "--output-dir", default=None, help="输出目录 (默认: 与输入目录相同)"
    )
    parser.add_argument(
        "--max-neighbors", type=int, default=100, help="邻居数量 (默认: 100)"
    )
    parser.add_argument(
        "--compute-base-neighbors",
        action="store_true",
        default=False,
        help="是否计算基础 neighbor(默认:false)",
    )
    parser.add_argument(
        "--only-base-neighbors",
        action="store_true",
        default=False,
        help="仅生成基础neighbors，不生成scalar数据集和scalar neighbors (默认:false)",
    )
    parser.add_argument(
        "--percentages",
        type=str,
        default="10,90",
        help="百分比数组，用逗号分隔，总和应 <= 100 (默认: 10,90)",
    )

    args = parser.parse_args()

    # 如果只生成base neighbors，不需要验证percentages
    if not args.only_base_neighbors:
        # 解析百分比
        try:
            percentages = parse_percentages(args.percentages)
        except ValueError as e:
            logger.error(f"Error parsing percentages: {e}")
            return 1

        # 验证百分比
        if sum(percentages) > 100:
            logger.error(f"Percentages sum ({sum(percentages)}) exceeds 100%")
            return 1
    else:
        percentages = []  # 只生成base neighbors时不需要percentages

    # 如果没有指定输出目录，则使用输入目录
    output_dir = args.output_dir if args.output_dir else args.data_dir

    try:
        generator = ScalarDatasetGenerator(args.data_dir, output_dir)
        generator.run(
            percentages,
            max_neighbors=args.max_neighbors,
            compute_base_neighbors=args.compute_base_neighbors,
            only_base_neighbors=args.only_base_neighbors,
        )
        return 0
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
