# mikyu

## Purpose

**mikyu is an exploration of how far durable application infrastructure can be simplified without sacrificing performance.**

It began from an interest in the underlying ideas behind systems such as WALs and Kafka: durable append-only storage, replayable logs, and messaging primitives that can coordinate applications.

The central realization was that these ideas are powerful, but using mature distributed systems often exposes developers to a large amount of operational and conceptual complexity.

mikyu asks:

> **Can an application get the useful properties of systems like Kafka while experiencing something closer to SQLite in simplicity?**

The goal is not to build another Kafka or NATS clone, nor to compete with distributed messaging platforms on their own terms.

Instead, mikyu explores a **local-first, single-node model** where durable messaging and event infrastructure are presented as a simple application primitive, with the underlying machinery deliberately kept out of the user's way.

---

## Core Vision

mikyu aims to make durable messaging feel like something an application can simply **have**, rather than infrastructure an application must **operate**.

The intended experience is:

```text
        simple application API
                 │
                 ▼
              mikyu
                 │
       ┌─────────┴─────────┐
       │                   │
   embedded             daemon
       │                   │
       └─────────┬─────────┘
                 │
            remote daemon
```

The same fundamental system should work:

* embedded directly inside an application;
* as a local daemon when process separation is useful;
* as a remote daemon when the service needs to live elsewhere.

The application should not need to fundamentally change its messaging model as deployment changes.

---

## Core Principles

### 1. Local-first

mikyu should work exceptionally well on a single machine before introducing any distributed infrastructure.

A developer should be able to start with:

```text
application
    └── mikyu
```

without provisioning a broker cluster, configuring a control plane, or learning distributed-systems operational concepts.

### 2. One binary

The same `mikyu` executable provides the system's principal modes:

* embedded engine;
* daemon;
* CLI;
* interactive REPL;
* TUI/operator interface.

This means the software needed to **run**, **operate**, and **inspect** mikyu is unified rather than fragmented across separate products.

### 3. Deployment should be a continuum

The intended progression is approximately:

```text
embedded
   ↓
local daemon
   ↓
remote daemon
```

Moving between these should require as little application-level change as possible.

Ideally, the difference is primarily **where mikyu lives**, not a different programming model.

### 4. Hide operational complexity

The user should not need to manually reason about every low-level mechanism that makes durable messaging performant.

Internally, mikyu can expose sophisticated controls and mechanisms—WAL batching, segment sizing, caching, retention, checkpointing, scheduling, etc.—while presenting a much simpler surface to the application developer.

### 5. Performance matters

Simplicity should not mean accepting unnecessarily poor performance.

mikyu is intended to explore how much performance can be achieved from a carefully engineered single-node system using techniques such as:

* sequential/log-structured storage;
* WAL-backed durability;
* mmap-backed reads;
* in-memory caching;
* batching;
* adaptive flushing;
* efficient indexing;
* careful concurrency;
* efficient local IPC.

The objective is not simply to claim superiority over Kafka or another established system. The objective is to provide **excellent performance within a radically simpler deployment model**.

### 6. One underlying storage engine, multiple semantics

mikyu separates durable storage from higher-level delivery semantics.

The current architecture places log and task queues over a shared `engine.Storage` abstraction, allowing the same physical storage machinery to support different application-level behaviors. 

This keeps the storage mechanism independent from the policies built on top of it.

---

# What mikyu Provides

At its current architectural level, mikyu aims to provide durable primitives including:

**Event/log semantics**

* append-only streams;
* replay;
* tailing;
* consumer groups;
* persistent offsets;
* retention.

**Task/work semantics**

* at-least-once delivery;
* visibility leases;
* acknowledgements;
* retries and backoff;
* scheduling;
* stale-lease protection;
* dead-letter queues;
* crash-recoverable task state.

**Storage/infrastructure**

* WAL-backed durability;
* synchronous and asynchronous write modes;
* mmap-backed reads;
* in-memory caching;
* crash recovery;
* persistent consumer state;
* configurable retention.

**Deployment/access**

* embedded usage;
* daemon mode;
* TCP;
* Unix sockets;
* Windows named pipes;
* remote clients;
* TLS;
* CLI/REPL/TUI.

These capabilities are already reflected in the system's architecture and implementation specification. 

---

# The Developer Experience Goal

The ideal user experience is closer to:

```python
db = mikyu.open("./data")

queue = db.queue("jobs")
queue.enqueue(...)
```

than to:

```text
install cluster
configure broker
configure controller
configure replication
configure partitions
configure networking
configure consumers
...
```

The exact API can evolve, but the philosophy should remain:

> **The complexity required to make the system work should not become the complexity required to use the system.**

---

# Configuration Philosophy

mikyu should have a rich internal configuration model without requiring users to understand the entire machinery.

The project already follows a single-source-of-truth approach through `spec.Table`, from which configuration surfaces such as SDK options, CLI flags, environment variables, YAML, and wire patches are derived. 

The long-term goal is to preserve that consistency across languages and interfaces.

A Go developer, Python developer, Rust developer, CLI user, and configuration-file user should ultimately be expressing the **same underlying configuration semantics**, even when the surface syntax is idiomatic to each environment.

---

# What mikyu Is Deliberately Not

### Not a Kafka replacement

Kafka solves problems that mikyu intentionally does not currently attempt to solve, particularly large-scale distributed operation.

### Not a NATS clone

NATS-like functionality may overlap with some of mikyu's capabilities, but reproducing another established messaging platform is not the objective.

### Not a distributed system

For the foreseeable future, mikyu is intentionally **single-node**.

Replication, consensus, clustering, leader election, partition management, and distributed fault tolerance are outside the intended core scope.

This constraint is part of the product philosophy rather than merely a missing feature.

### Not "infrastructure for infrastructure's sake"

The project should resist accumulating features simply because mature messaging systems have them.

A feature should justify itself by advancing one or more of:

* simplicity;
* local-first deployment;
* performance;
* useful application semantics;
* operational usability;
* the embedded → daemon → remote model.

---

# The Deployment Philosophy

The central deployment model can be summarized as:

```text
             ONE SYSTEM
                 │
        ┌────────┼─────────┐
        │        │         │
     embedded  daemon    tools
        │        │         │
        │        │      CLI / TUI
        │        │
        └────────┼─────────┘
                 │
            same concepts
            same executable
            same protocol
```

A developer can start with mikyu embedded in their application.

As requirements change, the queue can move into a local daemon.

Later, that daemon can live on another machine.

The desired outcome is that **deployment topology changes more than application semantics**.

---

# Long-Term Product Goal

The ultimate ambition is something like:

> **A durable messaging and event infrastructure primitive that is as easy to adopt as an embedded database, while providing enough performance and functionality to remain useful as applications grow.**

Or more succinctly:

> **Kafka-like capabilities with SQLite-like simplicity.**

Not by reproducing Kafka's distributed architecture, but by asking how much complexity can be absorbed by the system itself while retaining strong performance and useful durability semantics.

---

# Success Criteria

mikyu should be considered successful if it can make the following feel normal:

```text
1. Install one thing.

2. Point it at a directory.

3. Start using durable queues/logs.

4. Get good performance without tuning dozens
   of infrastructure parameters.

5. Inspect and operate it with the same binary.

6. Move from embedded → local daemon → remote daemon
   without redesigning the application.

7. Have the underlying machinery remain mostly invisible.
```

The deeper goal is therefore not merely **building a message queue**.

It is exploring a particular software-design philosophy:

> **Powerful infrastructure should not require powerful operational complexity from the person using it.**
