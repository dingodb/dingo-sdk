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

    def filter_data_by_id_threshold(self, data: List[Dict], id_threshold_percent: int):
        """根据ID阈值百分比过滤数据"""
        if id_threshold_percent is None:
            return data

        total_count = len(data)
        id_threshold = int(total_count * id_threshold_percent / 100)

        filtered_data = [item for item in data if item["id"] >= id_threshold]

        logger.info(
            f"ID threshold filtering: {id_threshold_percent}% = ID >= {id_threshold}"
        )
        logger.info(f"Filtered data: {len(filtered_data)}/{total_count} records")

        return filtered_data

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

        # 调整max_neighbors值到数据库大小
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

    def generate_id_filtered_neighbors(
        self, id_threshold_percent: float, max_neighbors=100
    ):
        """生成基于ID阈值过滤的邻居数据集"""
        logger.info(
            f"Generating ID filtered neighbors with threshold: {id_threshold_percent}%"
        )

        # 根据ID阈值过滤训练数据
        filtered_train_data = self.filter_data_by_id_threshold(
            self.train_data, id_threshold_percent
        )

        if len(filtered_train_data) == 0:
            logger.warning("No training data after ID filtering, skipping...")
            return None

        # 转换embedding向量为numpy数组
        filtered_train_embeddings = np.array(
            [item["emb"] for item in filtered_train_data]
        )
        test_embeddings = np.array([item["emb"] for item in self.test_data])
        test_ids = [item["id"] for item in self.test_data]
        filtered_train_ids = [item["id"] for item in filtered_train_data]

        # 计算neighbors
        neighbors_indices = self.compute_neighbors(
            test_embeddings,
            filtered_train_embeddings,
            max_neighbors=max_neighbors,
        )
        neighbors_ids = np.array(filtered_train_ids)[neighbors_indices]

        neighbors_data_filtered = []
        for i, test_id in enumerate(test_ids):
            neighbors_data_filtered.append(
                {"id": test_id, "neighbors_id": neighbors_ids[i].tolist()}
            )

        # 保存文件
        if id_threshold_percent == int(id_threshold_percent):
            # 如果是整数，不显示小数部分
            threshold_str = f"{int(id_threshold_percent)}"
        else:
            # 如果是小数，保留小数部分
            threshold_str = f"{id_threshold_percent:g}"

        neighbors_filename = f"neighbors_int_{threshold_str}p.json"
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
        del filtered_train_embeddings, test_embeddings
        gc.collect()

        return neighbors_filename

    def run(self, max_neighbors=100, id_threshold_percent=None):
        """主执行流程"""
        # 加载数据
        self.load_data()

        # ID过滤模式
        logger.info(
            f"Starting ID filtered neighbors generation with threshold: {id_threshold_percent}%"
        )
        neighbors_filename = self.generate_id_filtered_neighbors(
            id_threshold_percent, max_neighbors=max_neighbors
        )
        output_files = [neighbors_filename] if neighbors_filename else []
        logger.info("ID filtered neighbors generation completed!")

        # 显示生成的文件
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
        logger.info(f"file saved to: {self.output_dir}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="邻居数据集生成器 - JSON版本")
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
        "--id-threshold",
        type=float,
        default=0,
        help="ID阈值百分比，只使用ID >= 阈值的训练数据 (例如: 99表示只用ID >= 99%%数据集大小的数据，默认: 0)",
    )

    args = parser.parse_args()

    # 验证ID阈值参数
    if not (0 <= args.id_threshold <= 100):
        logger.error("ID threshold must be between 0 and 100")
        return 1

    # 如果没有指定输出目录，则使用输入目录
    output_dir = args.output_dir if args.output_dir else args.data_dir

    try:
        generator = ScalarDatasetGenerator(args.data_dir, output_dir)
        generator.run(
            max_neighbors=args.max_neighbors, id_threshold_percent=args.id_threshold
        )
        return 0
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
