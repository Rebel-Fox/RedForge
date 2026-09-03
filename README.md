# RedForge

![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg?style=flat&logo=cplusplus)
![POSIX](https://img.shields.io/badge/POSIX-TCP-green.svg?style=flat)
![Redis Protocol](https://img.shields.io/badge/Protocol-RESP-orange.svg?style=flat)
![CMake](https://img.shields.io/badge/Build-CMake_3.13+-purple.svg?style=flat)

A high-performance, multithreaded Redis-compatible in-memory database built from scratch in modern C++17[cite: 1, 3]. Features zero-copy RESP protocol parsing[cite: 1, 3], type-safe numeric deserialization via `std::from_chars`[cite: 1], segregated storage subsystems with reader-writer concurrency[cite: 1, 2], and distributed leader-follower replication[cite: 1, 3].

---

## Architecture

┌──────────────────────────────────────────────────────────────────────────┐
│                         POSIX TCP Socket Layer                           │
│     socket() → bind() → listen() → accept() → non-blocking wire I/O      │
│                  ┌────────────────────────────┐                          │
│                  │    std::thread per client  │                          │
│                  └────────────┬───────────────┘                          │
│                               ▼                                          │
│                  ┌────────────────────────────┐                          │
│                  │    Zero-Copy RESP Parser   │                          │
│                  │    std::string_view        │                          │
│                  │    std::from_chars         │                          │
│                  │    Multi-Bulk + Inline     │                          │
│                  └────────────┬───────────────┘                          │
│                               ▼                                          │
│            ┌──────────────────┼──────────────────┐                       │
│            ▼                  ▼                  ▼                       │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐             │
│  │    KV Store     │ │   List Store    │ │  Stream Store   │             │
│  │ std::unordered_ │ │   std::deque    │ │    std::map     │             │
│  │      map        │ │                 │ │    (RB-Tree)    │             │
│  │ std::shared_    │ │ std::shared_    │ │ std::shared_    │             │
│  │     mutex       │ │     mutex       │ │     mutex       │             │
│  │   passive TTL   │ │ condition_var   │ │  monotonic IDs  │             │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘             │
│            │                  │                  │                       │
│            ▼                  ▼                  ▼                       │
│  ┌──────────────────────────────────────────────────────────────┐        │
│  │         Transaction Engine (MULTI / EXEC / DISCARD)          │        │
│  │         Replication Engine (PSYNC / WAIT / REPLCONF)         │        │
│  │         Persistence Engine (Binary RDB Ingestion)            │        │
│  └──────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────┘

---

## Features

| Subsystem | Technical Implementation |
|---|---|
| **Networking** | Raw POSIX TCP sockets (`socket`, `bind`, `listen`, `accept`) managing concurrent client connections via detached `std::thread` workers[cite: 1]. |
| **Protocol Framing** | Zero-copy deserialization using `std::string_view` and `std::from_chars` without dynamic allocation overhead; handles multi-bulk arrays and inline plain ASCII commands (`PING_INLINE`)[cite: 1]. |
| **Scalar KV Store** | In-memory key-value engine with `PX` expiration units and passive TTL pruning, synchronized via reader-writer locks (`std::shared_mutex`)[cite: 1]. |
| **List Primitives** | Double-ended queues (`std::deque`) supporting `LPUSH`, `RPUSH`, `LPOP`, and `LRANGE`, coordinating consumer queues via `std::condition_variable`[cite: 1]. |
| **Time-Series Streams** | Append-only event log backed by Red-Black trees (`std::map`), strictly enforcing 128-bit monotonic sequence ordering (`<ms>-<seq>`) for `XADD` and `XRANGE`[cite: 1, 2]. |
| **Atomic Transactions** | Per-client isolation queues executing commands sequentially under `MULTI`, `EXEC`, and `DISCARD` blocks[cite: 1, 3]. |
| **Snapshot Persistence** | Binary RDB file deserializer reconstructing database state on startup by decoding magic headers, opcodes (`0xFA`, `0xFE`, `0xFB`), string length encodings, and TTL attributes[cite: 1, 3]. |
| **Distributed Replication** | Master-follower TCP state machine supporting dynamic handshakes (`PING` → `REPLCONF` → `PSYNC`), write stream propagation, and synchronous quorum consistency via `WAIT`[cite: 1, 3]. |

---

## Performance & Benchmark Metrics

Evaluated using the official `redis-benchmark` suite on WSL2 Linux at concurrency `c = 20` across `n = 20,000` requests per test.

### Command Latency & Throughput Distribution

| Command | Throughput (QPS) | p50 Latency | p95 Latency | p99 Latency | Avg Latency | Max Latency |
|---|---:|---:|---:|---:|---:|---:|
| **PING (Inline ASCII)** | 12,180.27 | 0.687 ms | 1.127 ms | 1.551 ms | 0.721 ms | 6.695 ms |
| **PING (Multi-Bulk)** | 11,926.06 | 0.535 ms | 1.151 ms | 1.527 ms | 0.639 ms | 3.751 ms |
| **LPUSH (List Ingestion)** | 11,737.09 | 0.383 ms | 1.151 ms | 1.535 ms | 0.609 ms | 210.047 ms |
| **GET (Shared Read Path)** | 11,716.46 | 0.455 ms | 1.135 ms | 1.799 ms | 0.619 ms | 204.415 ms |
| **SET (Exclusive Write Path)** | 11,001.10 | 0.759 ms | 1.303 ms | 1.831 ms | 0.812 ms | 6.607 ms |
| **XADD (Stream Tree Insert)** | 10,598.83 | 0.783 ms | 1.319 ms | 2.895 ms | 0.888 ms | 28.015 ms |

### Architectural Insights

* **Optimistic Shared-Lock Read Paths:** Concurrent `GET` queries achieve lower median latency (**0.455 ms p50**) compared to `SET` (**0.759 ms p50**) due to non-blocking reader concurrency under `std::shared_lock`[cite: 1]. `SET` operations acquire an exclusive `std::unique_lock`, serializing concurrent writes without lock starvation.
* **Double-Ended List Efficiency:** `LPUSH` sustained the lowest median response latency across all collection operations at **0.383 ms p50** and **1.535 ms p99**, validating the low constant-factor overhead of `std::deque::emplace_front`[cite: 1].
* **Monotonic Tree Indexing:** Even while enforcing 128-bit millisecond sequence validation and node rebalancing in `std::map`, stream event ingestion (`XADD`) sustains **10,598+ QPS** with a sub-millisecond median latency of **0.783 ms**[cite: 1, 2].

### Payload Scaling: LRANGE Throughput

| Batch Elements (N) | Throughput (QPS) | p50 Latency | p95 Latency | p99 Latency | Max Latency |
|---:|---:|---:|---:|---:|---:|
| **100 elements** | 10,537.41 | 0.855 ms | 1.383 ms | 2.511 ms | 9.359 ms |
| **300 elements** | 8,261.05 | 1.063 ms | 1.615 ms | 2.303 ms | 208.767 ms |
| **500 elements** | 6,706.91 | 1.351 ms | 1.943 ms | 2.775 ms | 204.799 ms |
| **600 elements** | 5,884.08 | 1.535 ms | 2.375 ms | 3.655 ms | 206.463 ms |

`LRANGE` demonstrates O(M) throughput scaling as the element slice size increases[cite: 1]. The throughput decay from **10,537 QPS** down to **5,884 QPS** reflects the serialized byte-framing overhead of larger multi-bulk RESP responses traversing kernel TCP socket buffers[cite: 1].

---

## Build & Run

### Prerequisites

* C++17-compliant compiler (`g++` 7.0+ or `clang++` 5.0+)[cite: 1]
* POSIX-compliant environment (Linux, macOS, or WSL2)[cite: 1]
* CMake 3.13+ (optional) or GNU Make[cite: 1]

### Option 1: CMake (Recommended)

mkdir -p build && cd build
cmake ..
make -j$(nproc)
./redforge

### Option 2: Makefile

make release    # Optimized release build (-O3)
make debug      # Debug build (-g)
./redforge

### Option 3: Direct Compilation

g++ -O3 -std=c++17 src/Server.cpp -o redforge -pthread
./redforge

### CLI Parameters

./redforge --port <port>                 # Listening port (default: 6379)
./redforge --replicaof <host> <port>     # Connect as replica to master
./redforge --dir <path>                  # Directory of the RDB snapshot
./redforge --dbfilename <file>           # File name of the RDB snapshot

---

## Example Usage

Connect to RedForge using standard `redis-cli`:

redis-cli -p 6379

### Key-Value Strings

127.0.0.1:6379> SET user "alice"
OK
127.0.0.1:6379> GET user
"alice"
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> SET token "abc123xyz" PX 5000
OK

### Lists & Blocking Deque

127.0.0.1:6379> RPUSH tasks "task_1" "task_2"
(integer) 2
127.0.0.1:6379> LPUSH tasks "urgent_task"
(integer) 3
127.0.0.1:6379> LRANGE tasks 0 -1
1) "urgent_task"
2) "task_1"
3) "task_2"
127.0.0.1:6379> LPOP tasks
"urgent_task"

### Time-Series Streams

127.0.0.1:6379> XADD sensor_stream * sensor A1 temp 25.4
"1725400000000-0"
127.0.0.1:6379> XRANGE sensor_stream - +
1) 1) "1725400000000-0"
   2) 1) "sensor"
      2) "A1"
      3) "temp"
      4) "25.4"

### Transactions

127.0.0.1:6379> MULTI
OK
127.0.0.1:6379> SET account "active"
QUEUED
127.0.0.1:6379> INCR total_visits
QUEUED
127.0.0.1:6379> EXEC
1) OK
2) (integer) 1

### Distributed Replication

Start a master instance:
./redforge --port 6379

Start a replica instance in a separate terminal:
./redforge --port 6380 --replicaof 127.0.0.1 6379

Inspect replication metadata via `redis-cli`:
127.0.0.1:6379> INFO replication
# Replication
role:master
master_replid:8371b4fb1155b71f4a04d3f1bc3e18c4a990aeeb
master_repl_offset:0
connected_slaves:1

127.0.0.1:6379> WAIT 1 5000
(integer) 1

---

## Project Structure

RedForge/
├── .gitignore
├── CMakeLists.txt
├── Makefile
├── README.md
└── src/
    └── Server.cpp