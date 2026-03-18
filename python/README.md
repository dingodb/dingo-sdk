# dingosdk

Python SDK for [DingoDB](https://github.com/dingodb/dingo-store), providing efficient access to the dingo-store cluster.

Supports **RawKV**, **Transaction**, and **Vector** APIs.

> **Note:** Linux only (x86_64).

---

## Installation

```bash
pip install dingosdk
```

Requirements: Python >= 3.9, Linux x86_64

---

## Quick Start

### Connect

```python
import dingosdk

status, client = dingosdk.Client.BuildAndInitLog("127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003")
assert status.ok(), status.ToString()
```

### RawKV

```python
status, raw_kv = client.NewRawKV()

# Put / Get / Delete
raw_kv.Put("key1", "value1")
status, value = raw_kv.Get("key1")
print(value)  # value1

raw_kv.Delete("key1")
```

### Vector

```python
status, vector_client = client.NewVectorClient()

# Add vectors
vector = dingosdk.Vector(dingosdk.ValueType.kFloat, 2)
vector.float_values = [1.0, 2.0]
entry = dingosdk.VectorWithId(1, vector)
vector_client.AddByIndexId(index_id, [entry])

# Search
query = dingosdk.Vector(dingosdk.ValueType.kFloat, 2)
query.float_values = [1.0, 2.0]
target = dingosdk.VectorWithId()
target.vector = query

param = dingosdk.SearchParam()
param.topk = 5
status, results = vector_client.SearchByIndexId(index_id, param, [target])
```

---

## Links

- [GitHub](https://github.com/dingodb/dingo-sdk)
- [DingoDB](https://github.com/dingodb/dingo-store)
- [Issues](https://github.com/dingodb/dingo-sdk/issues)
