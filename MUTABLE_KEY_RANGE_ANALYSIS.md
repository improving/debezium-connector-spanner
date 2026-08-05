# Cloud Spanner Change Streams: Mutable Key Range (v2) Fix Analysis

## Executive Summary

This document details the root cause analysis, code fixes, unit testing, and E2E verification for supporting **Cloud Spanner Change Streams Mutable Key Ranges (`partition_mode = 'mutable_key_range'`) / Placement Tables** in `debezium-connector-spanner`.

Three distinct bugs prevented data change records from being emitted on placement tables when key ranges moved between partitions. All three issues have been resolved, verified with unit tests, built into Docker image `gcr.io/span-cloud-testing/debezium-connector-spanner:latest`, and verified end-to-end on GKE.

---

## 1. Architectural Difference: Immutable vs. Mutable Key Ranges

```text
[ Immutable Key Range (v1) ]
Parent P_src  -------------------| (Terminates / FINISHED)
                                 +---> Child P_dst1 (Starts after parent FINISHED)
                                 +---> Child P_dst2 (Starts after parent FINISHED)

[ Mutable Key Range (v2 - Placement Tables) ]
Source P_src  ------------------- MoveOut -----------------------> (Stays Alive & Streaming)
                                    |
                                    v (Moves key range at commit timestamp T)
Dest P_dst    ------------------- MoveIn  -----------------------> (Starts & Streams)
```

| Characteristic | Immutable Key Range (`v1`) | Mutable Key Range (`v2` / Placement Tables) |
| :--- | :--- | :--- |
| **Partition Lifecycle** | Partitions split or merge; parent partitions **terminate and finish streaming** (`state = FINISHED`). | Partitions move key ranges *in* and *out* dynamically; parent/source partitions **stay alive and continue streaming**. |
| **Boundary Events** | `ChildPartitionsRecord` containing child partition tokens. | `MoveOut` event on source partition ($P_{src}$) and `MoveIn` event on destination partition ($P_{dst}$) sharing commit timestamp $T$. |
| **Dependency Waiting** | Child partitions wait for parent partitions to reach `state = FINISHED`. | Destination partition ($P_{dst}$) waits for source partition ($P_{src}$) to emit `MoveOut` at timestamp $T$. |
| **Connector Partition State** | Parent removed from task state once finished. | Source partition stays active in `TaskState` (`state = RUNNING`). |

---

## 2. Root Cause Analysis & Fixes

### Bug 1: Partition Scheduling Deadlock in `FindPartitionForStreamingOperation`
* **File**: `io/debezium/connector/spanner/task/operation/FindPartitionForStreamingOperation.java`
* **Root Cause**:
  When a destination partition $P_{dst}$ was discovered during a key range move, it was created with `parents = [P_src]` and `moveInState = null`. Because $P_{src}$ is an active mutable key range partition that never terminates, $P_{src}$ never entered `FINISHED` state. `FindPartitionForStreamingOperation` evaluated `finishedPartitions.containsAll([P_src])` to `false` and logged:
  > `"Task not taking partition for streaming, since parents are not finished"`
  This prevented $P_{dst}$ from starting to stream, meaning it could never read its change stream to discover its `MoveIn` event, creating a permanent initialization deadlock.
* **Fix**:
  We updated `FindPartitionForStreamingOperation.java` (and protected the logic for `isMutableKeyRange` change streams) to allow $P_{dst}$ with active parents to take for streaming so it can read up to its `MoveIn` record and transition into the MoveIn wait state.

---

### Bug 2: Offset Regression & Boundary Sequence Loss in `PartitionFactory`
* **File**: `io/debezium/connector/spanner/task/PartitionFactory.java`
* **Root Cause**:
  1. `resolveOffset` allowed partition start timestamps to be resolved earlier than `moveInState.getTimestamp()`, causing $P_{dst}$ to re-read records prior to the `MoveIn` boundary.
  2. `lastBoundaryRecordSequence` was not initialized from `moveInState.getRecordSequence()` when restoring partition state from Kafka sync topic checkpoints.
* **Fix**:
  1. Enforced that `resolveOffset` never sets start timestamp behind `moveInState.getTimestamp()`.
  2. Initialized `lastBoundaryRecordSequence` from `moveInState.getRecordSequence()` during state recovery.

---

### Bug 3: Window Boundary Sequence Erasure in `SpannerChangeStreamService`
* **File**: `io/debezium/connector/spanner/db/stream/SpannerChangeStreamService.java`
* **Root Cause**:
  1. Spanner Change Streams read in 20-minute query windows. When advancing to a new window without boundary events, `lastBoundaryRecordSequence` was being overwritten with `null`.
  2. `filterBoundaryDuplicates` used strict inequality (`isBefore`), missing duplicate boundary events occurring at the exact window boundary timestamp.
* **Fix**:
  1. Retained `lastBoundaryRecordSequence` across window advances instead of overwriting with `null`.
  2. Updated `filterBoundaryDuplicates` to check `isBeforeOrEqual(eventTimestamp, windowStart)`.

---

## 3. Execution Sequence After the Fixes

```text
[ P_src (Source Partition) ]             [ P_dst (Destination Partition) ]
           |                                             |
   Emits MoveOut (at T_0)                         Created with parents=[P_src]
           |                                             |
           |                                      FindPartitionForStreamingOperation
           |                                      (Takes for streaming via active parents)
           |                                             |
           |                                      Reads stream -> Hits MoveIn (at T_0)
           |                                             |
           |                                      Updates MoveInState & Pauses
           |                                             |
   Processes MoveOut                              Checks MoveOutState from P_src
           |                                             |
   Publishes MoveOutState ------------------------> Satisfied! Resumes streaming
                                                         |
                                                  Emits CDC DataChangeRecord (op='c')
```

### Detailed Step-by-Step Flow:

1. **Key Range Move Trigger**: Spanner moves a key range from $P_{src}$ to $P_{dst}$ at commit timestamp $T_0$.
2. **Partition Discovery**: $P_{dst}$ is created in `TaskState` with `state = CREATED`, `parents = [P_src]`, and `moveInState = null`.
3. **Active Parent Scheduling**: `FindPartitionForStreamingOperation` recognizes `isMutableKeyRange == true` and that parent $P_{src}$ is active. It marks $P_{dst}$ as `READY_FOR_STREAMING`.
4. **`MoveIn` Record Processing**: $P_{dst}$ opens its change stream query, reads the `MoveIn` event at $T_0$, populates `moveInState`, and pauses.
5. **`MoveOut` Record Synchronization**: $P_{src}$ reads the `MoveOut` event at $T_0$ and updates `TaskState` with `MoveOutState`.
6. **Partition Unblock & CDC Emission**: `canDestPartitionContinue` evaluates to `true`. $P_{dst}$ resumes streaming past $T_0$, reads the insert record, and emits the CDC event to Kafka.

---

## 4. Code Modifications & Diffs

### A. `FindPartitionForStreamingOperation.java`

```diff
+ private final boolean isMutableKeyRange;
+
+ public FindPartitionForStreamingOperation(boolean isMutableKeyRange) {
+     this.isMutableKeyRange = isMutableKeyRange;
+ }

  if (partitionState.getMoveInState() != null) {
      if (canDestPartitionContinue(taskSyncContext, partitionState, finishedPartitions)) {
          takePartitionForStreaming = true;
      }
  }
- else if (finishedPartitions.containsAll(partitionState.getParents())) {
+ else if (finishedPartitions.containsAll(partitionState.getParents()) 
+       || (isMutableKeyRange && atLeastOneParentExists(taskSyncContext, partitionState.getParents()))) {
      takePartitionForStreaming = true;
  }
```

### B. `TaskStateChangeEventHandler.java` & `ChangeStream.java`

```diff
// Passed isMutableKeyRange configuration flag to FindPartitionForStreamingOperation
  new FindPartitionForStreamingOperation(changeStream.isMutableKeyRange())
```

---

## 5. Verification & Testing

### Unit Test Verification (`PlacementMoveInMoveOutTest.java`)
All unit test suites pass cleanly (**BUILD SUCCESS**, 16/16 tests passing):
1. `testRecordMapping()`: Validates mapping of Protobuf `MoveOut`, `MoveIn`, and `DataChangeRecord` (INSERT into `BenchmarkPlacementUsers`).
2. `testMoveInMoveOutTaskStateSynchronization()`: Validates task state transitions (`CREATED` $\rightarrow$ paused on MoveIn $\rightarrow$ `READY_FOR_STREAMING` after source MoveOut).
3. `testMoveInReExecutionLoopBug()`: Validates that boundary duplicate filtering skips duplicate `MoveIn` events upon partition resumption without re-triggering `onMoveIn`.

### E2E GKE Deployment Verification
The updated connector image `gcr.io/span-cloud-testing/debezium-connector-spanner:latest` was deployed to GKE deployment `kafka-connect-cp-kafka-connect`. Upon inserting data into placement table `BenchmarkPlacementUsers`, the connector successfully emitted the CDC record to Kafka topic `cdc_spanner_jiangzzhu_test_08051023.BenchmarkPlacementUsers`:

```json
{
  "payload": {
    "before": null,
    "after": {
      "UserId": 6,
      "PlacementKey": "default"
    },
    "source": {
      "connector": "spanner",
      "name": "cdc_spanner_jiangzzhu_test_08051023",
      "change_stream_name": "mycs",
      "table": "BenchmarkPlacementUsers",
      "partition_token": "__8BAYEHAiwAH5AAAYLAQYNteWNzAAGEgQYlWS_gARGCgIMAhAS4rvlzhWcyNzNfMzkxNjI2MjIAAf__wGQBAf__"
    },
    "op": "c"
  }
}
```
