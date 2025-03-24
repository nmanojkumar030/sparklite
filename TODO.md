
# TODO.md — Build a Miniature Spark in Java

This project is a step-by-step implementation of a Spark-like distributed data processing engine in Java, inspired by Apache Spark's RDD architecture. We will follow Test-Driven Development (TDD) principles throughout.

---

## ✅ Phase 1: RDD Core Abstractions (Local Execution)

### ☐ Step 1: Create `MiniRDD<T>`
- [ ] Abstract base class with `map`, `filter`, and `collect()` methods.
- [ ] `.map()` and `.filter()` should return new `MiniRDD` instances that keep a reference to the parent RDD.
- [ ] `.collect()` should evaluate the entire lineage lazily.

### ☐ Step 2: Implement `ParallelCollectionRDD<T>`
- [ ] Create a concrete subclass of `MiniRDD` that wraps a static list.
- [ ] Implements `collect()` by returning its internal data.

### ☐ Step 3: Implement `MapRDD<T, U>` and `FilterRDD<T>`
- [ ] Apply transformation on parent data in `collect()`.
- [ ] Must call `parent.collect()` internally.

### ☐ Step 4: Write Unit Tests
- [ ] `testMapAndFilter()`
- [ ] `testEmptyRDD()`
- [ ] Test transformation chaining (e.g., `rdd.filter(...).map(...).filter(...)`)

---

## 🔁 Phase 2: Partitioning and Parallelism

### ☐ Step 5: Introduce `Partition` abstraction
- [ ] Create `MiniPartition` class with `partitionId` and data slice.
- [ ] Add `MiniRDD.getPartitions()` returning list of partitions.

### ☐ Step 6: Implement `mapPartitions()` method
- [ ] Add to `MiniRDD<T>` to support transformation on partition level.

### ☐ Step 7: Execute partitions in parallel
- [ ] Create `LocalScheduler` that executes one task per partition using a thread pool.
- [ ] `MiniRDD.collect()` should submit partition tasks to the scheduler.

### ☐ Step 8: Write tests for parallel execution
- [ ] Use timing or thread ID tracking to verify concurrency.

---

## 🧱 Phase 3: DAG Scheduler and Stages

### ☐ Step 9: Create `Stage` and `Task` classes
- [ ] A `Stage` represents a group of `Task`s with no shuffle boundaries.
- [ ] A `Task` processes a single partition.

### ☐ Step 10: Implement `DAGScheduler`
- [ ] Resolves RDD lineage into a DAG of stages.
- [ ] Submits ready stages to the task scheduler in order.

### ☐ Step 11: Add fault injection and retries
- [ ] Simulate failure in a task and re-run from parent RDD.
- [ ] Write tests to confirm retry behavior.

---

## 🌉 Phase 4: Shuffle and Wide Dependencies

### ☐ Step 12: Implement `groupByKey()` with simulated shuffle
- [ ] Partition values by key hash and regroup on new RDD.
- [ ] Add `ShuffleMapStage` and `ResultStage` in DAG.

### ☐ Step 13: Simulate shuffle file writing and reading
- [ ] Write partitioned output to temp storage.
- [ ] Later stages read partitioned input from shuffle files.

---

## 🧭 Phase 5: Remote Execution Simulation (Optional)

### ☐ Step 14: Add `Executor` and `TaskScheduler`
- [ ] Simulate Spark's cluster mode locally.
- [ ] Use threads or lightweight RPC to simulate remote workers.

### ☐ Step 15: Add driver/executor message protocol
- [ ] Submit task from driver to executor.
- [ ] Collect result back into driver.

---

## 🧪 Testing Strategy

- [ ] Every transformation should have unit tests.
- [ ] Every scheduler should have integration tests.
- [ ] Add fault injection tests for retries and lineage recomputation.

---

## 📦 Suggested Project Structure

```
minispark/
├── core/
│   ├── MiniRDD.java
│   ├── ParallelCollectionRDD.java
│   └── transformations/
│       ├── MapRDD.java
│       └── FilterRDD.java
├── scheduler/
│   ├── Task.java
│   ├── Stage.java
│   └── DAGScheduler.java
├── executor/
│   ├── LocalExecutor.java
│   └── TaskScheduler.java
├── test/
│   └── MiniRDDTest.java
├── build.gradle
└── TODO.md
```

---

## 📝 Notes

- This project uses **Java + JUnit 5 + Gradle**.
- Code structure and concepts should match Apache Spark as closely as possible.
- Later phases may evolve toward Spark SQL's physical plan structure.
