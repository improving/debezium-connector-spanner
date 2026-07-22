# Change Stream Integration Tests

Here is a comprehensive list explaining each integration test added for testing change streams.

Tests are grouped by file. Within each file, tests are listed in the order they appear in
the file. Each test runs against one of three possibilities:

- **The local Docker Spanner emulator** - the default backend for most of these tests, no
  extra flags needed.
- **Spanner Omni Docker Container** - a temporary stand-in used because the local emulator can't 
  create a `MUTABLE_KEY_RANGE` change stream at all. Any test running against Omni will be
  re-validated against a real Spanner instance as soon as one is available.
- **A real, multi-region Spanner instance** - not available yet. Some of the below tests are
  disabled because they require a real spanner instance.

## `MutableKeyRangeIT`

Dedicated suite for behavior that's unique to `MUTABLE_KEY_RANGE` partition mode - sliding
window mechanics, connector-restart mechanics, and key-range-split mechanics - with no
`IMMUTABLE_KEY_RANGE` equivalent to test alongside.

**Runs against:** Spanner Omni only (`-Dspanner.type=OMNI`). The whole class is gated behind
that flag and never runs against the local emulator, since the emulator's DDL parser rejects
`MUTABLE_KEY_RANGE` streams outright.

**`shouldStreamCrudEventsToKafka`**
Inserts a row, updates it, then deletes it. Confirms the four resulting events arrive in
order: an insert (`c`), an update (`u`), a delete (`d`), and a trailing tombstone.

**`shouldNotRepublishEventsAfterConnectorRestart`**
Inserts a row, confirms it's delivered, stops the connector, inserts a second row while it's
down, then restarts. Confirms the second row is delivered after restart (the first row may or
may not be redelivered, since delivery is at-least-once, not exactly-once).

**`shouldNotReplayAfterWindowElapses`**
Inserts a row, waits for the sliding window it falls in to fully close, restarts the
connector with no new data, and confirms nothing gets redelivered - the persisted
`processedTimestamp` correctly remembers that this window was already fully processed.

**`shouldPreserveOrderAcrossForcedKeyRangeSplit`**
Inserts three rows at different keys, then forces Spanner to physically split the key range
around them (via the `AddSplitPoints` admin API), then issues five rapid updates to each row
while the split is happening. Confirms every row's updates still arrive in the exact order
they were written, with no gaps or duplicates, even though the split forces the destination
partitions through a pause-and-resume handshake mid-stream.

**`shouldNotLoseOrReorderEventsWhenStoppedDuringForcedKeyRangeSplit`**
Inserts a row, forces a key-range split around it, and stops the connector immediately
afterward (no settle time), specifically trying to catch it mid-way through the
pause-and-resume handshake a split triggers. Restarts the connector and issues five more
updates. Confirms every value from before and after the restart still shows up, in the
correct order (duplicates from at-least-once redelivery are tolerated, but nothing may be
missing or out of order) - proving the in-progress handshake state survives a stop/start
cycle correctly.

**`shouldNotLoseEventsWhenStoppedMidWindow`**
Inserts five rows, waits just long enough for the connector to start processing them (well
short of the sliding window's real-time close), then stops and restarts the connector.
Confirms all five rows eventually get delivered - nothing inserted before a mid-window stop
is silently skipped after restart.

**`shouldNotLoseDeleteWhenStoppedMidWindow`**
Same idea as the test above, but for a delete instead of an insert: inserts then deletes a
row, stops the connector mid-window, restarts it, and confirms the insert, the delete, and
the delete's tombstone all eventually arrive.

**`shouldDeliverAllModsFromLargeSingleTransaction`**
Inserts 20 rows as one single Spanner transaction (rather than 20 separate transactions).
Confirms all 20 resulting records are delivered, in commit order, and that every one of them
carries the same transaction ID - proving a large multi-row transaction isn't split up or
delivered out of order.

**`shouldAdvanceThroughQuietWindowWithoutStalling`**
Starts the connector, then waits through a full sliding window with no data changes in it at
all (only heartbeats). Inserts a row afterward and confirms it's delivered. Guards against
the connector getting stuck re-querying an empty window instead of advancing past it, which
would otherwise cause every later row to never be delivered.

**`shouldNotStartConnectorWithWindowMinutesTooLow`** / **`shouldNotStartConnectorWithWindowMinutesTooHigh`**
Two config-validation tests (no real table or change stream involved). Confirm the connector
refuses to start when `gcp.spanner.mutable.window.minutes` is set to 0 or to 31, since the
valid range is documented as 1-30 inclusive.

**`shouldCatchUpQuicklyThroughHistoricalWindows`**
Inserts three rows, then waits several minutes before ever starting the connector, with the
connector's start time pointed back at before the inserts happened - so several sliding
windows have already elapsed in real time before the connector reads its first row. Confirms
all three rows are still delivered, and that catching up through those already-elapsed
windows happens quickly (not by pacing one window per real-time minute the way a
live-tailing connector naturally would).

**`shouldPickUpSchemaChangeMidStream`** (currently `@Disabled`)
Would insert a row, run `ALTER TABLE ADD COLUMN` mid-stream (without touching the change
stream's own configuration or restarting the connector), insert a second row using the new
column, then update the first row's new column.
Disabled because it can't be validated against either available test backend: the local
emulator doesn't support `MUTABLE_KEY_RANGE` at all, and Spanner Omni drops any UPDATE to a
pre-existing row once the table has a third column of type INT64. Not a connector bug -
waiting on a real Spanner instance to test against.

**`shouldPickUpSchemaChangeMidStreamForNewInserts`**
Recovers the useful part of the test above without hitting the same gap: inserts a row, runs
`ALTER TABLE ADD COLUMN` mid-stream, then inserts a second row using the new column. Confirms
both inserts are delivered correctly - including the second one correctly showing the new
column's value - proving the schema change is picked up automatically without a restart or
reconfiguration.

**`shouldResumeCorrectlyAfterWindowSizeIsChangedAcrossRestart`**
Starts the connector with one window size, inserts a row, confirms delivery, then stops the
connector and restarts it with a *different* `gcp.spanner.mutable.window.minutes` value using
the same connector name and offset file (a genuine resume of the same partition). Inserts a
second row and confirms it's still delivered. The window size lives only in the running
service instance, not in persisted offset state, so this proves a restart with a changed
window size still computes the next window correctly from wherever the partition left off.

## `ChangeStreamCorrectContentIT`

Parameterized across all partition modes. Verifies the actual content of change-stream
records matches documented behavior for various row/column scenarios.

**Runs against:** the local Docker emulator for the `IMMUTABLE_KEY_RANGE` parameter (the
default case, always runs). The `MUTABLE_KEY_RANGE` parameter is skipped unless the suite is
run with `-Dspanner.type=OMNI`, in which case it runs against Spanner Omni instead - a
temporary stand-in, same caveat as `MutableKeyRangeIT` above.

**`shouldCarryAllPrimaryKeyColumnsInKeyStruct`**
Uses a composite primary key (two columns). Inserts two rows that share one key column but
differ on the other, then updates one of them. Confirms the Kafka record key includes both
key columns in the correct order, the two rows never collide onto the same key, and the
update is correctly scoped to only the matching row.

**`shouldCarryLastKnownValuesInBeforeOnDelete`**
Inserts a row, updates one of its columns, then deletes it. Confirms the delete's `before`
image reflects the updated value (not the original insert value), `after` is null, and a
tombstone follows.

**`shouldRoundTripNullColumnTransitions`**
Inserts a row with a null column, sets it to a value, then sets it back to null. Confirms
each transition is captured correctly, including that "value is null" is correctly
distinguished from "field never set" on the return-to-null step.

**`shouldCarryUnchangedColumnsThroughOnPartialUpdate`**
Inserts a row with several columns, then updates only one of them. Confirms `before` and
`after` both correctly carry through the untouched columns' values, showing a real
old-to-new transition only for the column that was actually touched.

**`shouldPickUpColumnAddedAfterStreamCreationWithoutReconfiguring`**
The parameterized (all-partition-mode) equivalent of `MutableKeyRangeIT`'s schema-change
tests above: inserts a row, adds a column mid-stream via DDL, inserts a second row using the
new column, and updates the first row's new column. Confirms records before and after the
schema change all look correct.

**`shouldDefaultTransactionTagAndSystemTransactionFlagForOrdinaryWrites`**
Performs a plain insert with no explicit transaction tag. Confirms the record's source
metadata shows an empty tag and a `false` system-transaction flag by default.

**`shouldSurfaceExplicitTransactionTag`** (currently `@Disabled`)
Would run an insert inside a transaction carrying an explicit tag and confirm the tag
surfaces on the record.
Disabled because the Spanner emulator used for these tests doesn't propagate transaction 
tags through its change stream at all - this needs a real, fully featured Spanner instance.

## `ChangeStreamFilterIT`

Tests for change-stream event filters (excluding specific operation types or specific
transactions from the stream), and confirms filtered-out changes still correctly affect what
later, unfiltered events report as the "before" state.

**Runs against:** the local Docker emulator only, using the default (`IMMUTABLE_KEY_RANGE`)
partition mode.

**`shouldExcludeDeleteEventsAndTheirTombstones`**
With deletes excluded from the stream: inserts, updates, then deletes a row, and inserts an
unrelated second row. Confirms no delete or tombstone appears for the deleted row, while
everything else streams normally.

**`shouldReflectRealPriorStateOnUpdateAfterAnExcludedInsert`**
With inserts excluded from the stream: inserts a row (never streamed), then updates and
deletes it, plus inserts an untouched second row. Confirms no insert record appears, but the
update's `before` still correctly reflects the row's real original value even though its
insert was never seen, and the delete/tombstone still work normally.

**`shouldExcludeUpdateEventsButReflectRealStateOnSubsequentDelete`**
With updates excluded from the stream: inserts, updates (excluded), then deletes a row, plus
inserts an untouched second row. Confirms no update record appears, but the delete's `before`
correctly reflects the real (post-update) database state rather than a stale pre-update
value.

**`shouldNotRecordTransactionExplicitlyExcludedFromChangeStreams`**
Inserts a row, then runs an update inside a transaction explicitly marked to be excluded from
change streams, followed by a normal, visible update. Confirms the excluded transaction
produces no record at all, while the final visible update's `before` correctly reflects the
real state left behind by the excluded transaction.

## `ChangeStreamOrderingAndTransactionalIT`

Parameterized across all partition modes. Verifies cross-table transaction correlation,
strict ordering under rapid writes, and restart correctness.

**Runs against:** same pattern as `ChangeStreamCorrectContentIT` above - local Docker emulator
for `IMMUTABLE_KEY_RANGE` (always), Spanner Omni for `MUTABLE_KEY_RANGE` (only when run with
`-Dspanner.type=OMNI`), with the same temporary-stand-in caveat for the Omni portion.

**`shouldCorrelateChangesAcrossTablesInSameTransaction`**
Seeds a row in table A, then runs one atomic transaction that updates table A and inserts
into table B. Confirms both changes from that shared transaction carry the same transaction
ID and commit timestamp, while the earlier, separate seed transaction has a different ID.

**`shouldPreserveStrictOrderAcrossManyRapidUpdatesToSameRow`**
Inserts a row, then issues eight rapid sequential updates to it. Confirms every resulting
record appears in exactly the order the updates were issued, with strictly increasing commit
timestamps throughout.

**`shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart`**
Inserts a row, confirms delivery, stops the connector, updates the row while it's down,
restarts, and confirms the missed update is delivered exactly once (not lost, not
duplicated) with correct before/after content. Then performs one more update after resuming
to confirm the connector keeps working normally afterward.

## `ChangeStreamValueCaptureTypeIT`

Verifies the three non-default `value_capture_type` change-stream options behave as
documented, each inserting a row and updating only one of its columns.

**Runs against:** the local Docker emulator only, using the default partition mode. No Omni
involvement.

**`shouldCaptureFullNewRowWithNoNonKeyOldValues`** (`NEW_VALUES`)
Confirms `before` contains only the primary key (no old values), while `after` contains the
full row, including untouched columns.

**`shouldCaptureFullNewRowWithNoOldValues`** (`NEW_ROW`)
Confirms `before` contains no old column values (just the key), while `after` contains the
complete row.

**`shouldCaptureFullRowOnBothSides`** (`NEW_ROW_AND_OLD_VALUES`)
Confirms both `before` and `after` contain the complete row, with `before` showing the prior
value of the touched column and `after` showing its new value.

## `ConcurrentKeysIT`

**Runs against:** the local Docker emulator only, using the default partition mode.

**`shouldNotCrossContaminateStateBetweenInterleavedKeys`**
Inserts four different rows, then updates all four in a deliberately interleaved order
(not fully processing one key before starting the next), to stress any per-row state
tracking that might be indexed incorrectly. Confirms all eight resulting events are captured
and, critically, that each row's update correctly reflects that row's own prior/new values -
not a value that leaked in from a different row being processed nearby.

## `CrossPartitionSplitOrderingIT`

**Runs against:** the local Docker emulator only, and deliberately so - this test relies on
the emulator's own quirk of automatically re-splitting partitions on a timer, which real
Spanner doesn't do (real Spanner splits based on load, not a fixed schedule). That's not a
temporary stand-in situation like Omni above; the emulator's timer-driven splitting is used
on purpose here to get a churning partition topology "for free" without needing to force a
split manually.

**`shouldDeliverFollowUpWriteExactlyOnceAndInOrderAcrossBackgroundPartitionSplits`**
Inserts a row, then waits 45 seconds - long enough for the Spanner emulator's own
timer-driven background partition splitting to run through several generations of splits on
its own, with no forced split needed - then updates that row. Confirms exactly one insert and
one update are delivered (no duplicates or drops from the row's key range having moved across
several partition generations), and that the update's timestamp is strictly later than the
insert's.

## `InterleavedTableIT`

**Runs against:** the local Docker emulator only, using the default partition mode.

**`shouldCaptureCascadingDeleteOfInterleavedChildRows`**
Inserts a parent row and a child row interleaved under it, in one transaction, then deletes
only the parent (never issuing any DML directly against the child), relying on
`ON DELETE CASCADE` to remove the child. Confirms both parent and child each produce insert,
delete, and tombstone events; that the parent and child inserts share one transaction ID
(proving the atomic multi-table insert is correlated); and that the parent's explicit delete
and the child's cascaded delete also share one transaction ID - confirming the
cascade-triggered child delete is correctly captured even though no direct DML touched the
child.

## `TransactionRecordCountIT`

**Runs against:** the local Docker emulator only, using the default partition mode.

**`shouldReportRecordAndPartitionCountsForTransaction`**
Inserts one row in its own transaction, then in a separate transaction updates that row and
inserts a new row as two statements executed atomically together. Confirms the single-row
transaction reports a record count and partition count of 1, and that both records from the
two-statement transaction report a transaction-wide record count of 2 (not a count scoped to
just one row), while still correctly showing 1 partition.

## `DataTypesIT` (new addition)

Pre-existing file; the one new test added alongside it. **Runs against:** the local Docker
emulator only, using the default partition mode.

**`shouldRoundTripEdgeCaseValuesAcrossInsertUpdateDelete`**
Inserts a row with intentionally tricky values - an empty string, empty bytes, a negative
`NUMERIC`, a unicode/emoji string, and an empty array - then updates the row to flip the
empty string to `NULL`, set real bytes content, swap in a large positive `NUMERIC`, change
the unicode string, and populate the array, then deletes the row. Confirms empty values
round-trip as empty (not coerced to `NULL`) on insert, that the update's `before`/`after`
correctly show the edge-case-to-new-value transitions (including empty-string-to-`NULL`), and
that the delete's `before` reflects the post-update state with a trailing tombstone.

## Blocked / disabled - TTL eviction

Both classes below are entirely `@Disabled`, for the same reason: TTL-driven row eviction
never actually runs in the local Docker emulator within any test-practical time window. That's
where these tests are written to run (default partition mode, no Omni involvement) - the
blocker is TTL background garbage collection itself never firing there, not partition mode.
Unblocking this needs a real Spanner instance where TTL eviction actually runs on a
test-practical schedule.

**`TtlDeleteEventIT.shouldEmitDeleteAsSystemTransactionWhenRowExpiresViaTtl`**
Would insert a row with its TTL expiration already 2 days in the past (immediately eligible
for garbage collection) into a table with a 1-day row-deletion TTL policy, and confirm the
resulting TTL-triggered delete's `source.system_transaction` field is `true` - the only
scenario in the whole suite that would exercise a positive `system_transaction` value, since
every other event in these tests comes from ordinary user DML and is always `false`.

**`ExcludeTtlDeletesFilterIT.shouldFilterOutTtlDeletesButStillDeliverUserIssuedDeletes`**
Would insert one row eligible for immediate TTL eviction and a second row with a
far-future expiration that's explicitly deleted by the user, on a change stream configured
with `exclude_ttl_deletes`. Would confirm the user-issued delete and its tombstone still
arrive normally, while no delete or tombstone ever appears for the TTL-expired row.

## Blocked / disabled - geo-partitioned placement

Three classes below are entirely `@Disabled`, all blocked on the same two things: neither the
local Docker emulator nor Spanner Omni support geo-partitioning/`CREATE PLACEMENT` (the
emulator's DDL parser rejects `partition_mode` outright; Omni accepts `MUTABLE_KEY_RANGE` but
rejects placement), and the test `Connection` helper doesn't yet have a `createPlacement(...)`
method. The connector-side `MUTABLE_KEY_RANGE` move-in/move-out wiring itself is already
confirmed correct for the non-geo-partitioned case (see `MutableKeyRangeIT`'s forced-split
tests above) - these three are specifically about geo-partitioned placement moves, which need
real infrastructure to exercise.

**Runs against:** none currently - all three are written to create their change streams with
`PartitionMode.MUTABLE_KEY_RANGE`, but can't be validated against either available test
backend, same reasoning as `MutableKeyRangeIT.shouldPickUpSchemaChangeMidStream` above. They
need a real, multi-region Spanner instance, or a future geo-partitioning-enabled Omni edition.

**`PlacementKeyMoveIT.shouldOrderRecordsCorrectlyWhenRowMovesBetweenPlacements`**
Would insert a row in one geo-partitioned placement region, move it to another region by
updating its placement key, then immediately issue a follow-up update to that same row.
Would confirm the move event shows the correct before/after regions, and that the follow-up
write's timestamp is strictly later than the move's - verifying the connector doesn't deliver
the follow-up out of order relative to the placement move.

**`InterleavedPlacementMoveIT.shouldMoveInterleavedChildRowsWithParentPlacementChange`**
Would insert a parent row with an interleaved child row (the child has no placement key of
its own and always moves with its parent), then move the parent between placements. Would
confirm the parent's move event shows the new region, and that a follow-up update on the
child is correctly ordered after the parent's move timestamp - answering an open question
about whether the child emits its own visible move signal or the move is only observable
indirectly through correct ordering.

**`CascadingDeleteDuringPlacementMoveIT.shouldOrderCascadingDeleteCorrectlyRelativeToInFlightPlacementMove`**
The most complex of the three: would insert a parent row with an interleaved child (using
`ON DELETE CASCADE`), move the parent to a different placement, then immediately delete the
parent so the delete cascades to the child. Would confirm both parent and child produce the
expected insert/move/delete/tombstone events, that the parent and child deletes share one
transaction ID, and that there's exactly one delete and one tombstone per key - verifying no
duplicate or dropped delete arises from the move and the cascade both needing to explain the
same row's disappearance at once.
