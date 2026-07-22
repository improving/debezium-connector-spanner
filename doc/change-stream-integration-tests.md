# Change Stream Integration Tests

Here is a comprehensive list explaining each integration test added for testing change streams.

Tests are grouped by file. Within each file, tests are listed in the order they appear in
the file. Each test runs against one of three possibilities:

- **The local Docker Spanner emulator** - the default backend for most of these tests, no
  extra flags needed.
- **Spanner Omni Docker Container** - a Docker-based fallback backend for `MUTABLE_KEY_RANGE`
  testing, used because the local emulator's DDL parser rejects a `MUTABLE_KEY_RANGE` change
  stream outright. Kept as a lightweight, no-cloud-dependency option for local runs; the full
  `MutableKeyRangeIT` suite has since been re-validated against a real Spanner instance as well
  (see below), and a small number of Omni-specific gaps have been identified and are called out
  where relevant.
- **A real, multi-region Spanner instance** (`-Dspanner.test.real=true`, on classes annotated
  `@RealSpannerCompatible`) - available, and the preferred backend for `MUTABLE_KEY_RANGE`
  tests going forward; several key scenarios (the schema-change tests and the forced
  key-range-split tests) have been independently confirmed passing against it.

## `MutableKeyRangeIT`

Dedicated suite for behavior that's unique to `MUTABLE_KEY_RANGE` partition mode - sliding
window mechanics, connector-restart mechanics, and key-range-split mechanics - with no
`IMMUTABLE_KEY_RANGE` equivalent to test alongside.

**Runs against:** Spanner Omni (`-Dspanner.type=OMNI`) or a real Cloud Spanner instance
(`-Preal-spanner`) - the whole class is gated behind `@EnabledIf("hasNonEmulatorBackend")` and
never runs against the local emulator, since the emulator's DDL parser rejects
`MUTABLE_KEY_RANGE` streams outright. The full suite passes against both Omni and a real
Cloud Spanner instance, aside from the Omni-specific exception called out below.

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

**`shouldPickUpSchemaChangeMidStream`**
Inserts a row, runs `ALTER TABLE ADD COLUMN` mid-stream (without touching the change stream's
own configuration or restarting the connector), inserts a second row using the new column,
then updates the first row's new column. Confirmed passing against a real Cloud Spanner
instance (`-Preal-spanner`) - twice.

**`shouldPickUpSchemaChangeMidStreamForNewInserts`**
Subset of the test above, for rows inserted after the schema change only: inserts a row, runs
`ALTER TABLE ADD COLUMN` mid-stream, then inserts a second row using the new column. Confirms
both inserts are delivered correctly - including the second one correctly showing the new
column's value - proving the schema change is picked up automatically without a restart or
reconfiguration. Deliberately doesn't touch the pre-existing-row-UPDATE path exercised by the
test above, since Spanner Omni drops any UPDATE to a pre-existing row once the table has a
third column of type INT64 (confirmed Omni-specific, not a connector bug) - `shouldPickUpSchemaChangeMidStream`
self-skips against Omni for that reason, and this test gives Omni runs some coverage of the
scenario regardless.

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
default case, always runs). The `MUTABLE_KEY_RANGE` parameter is skipped unless a
non-emulator backend is available (same `hasNonEmulatorBackend` check as `MutableKeyRangeIT`
above), in which case it runs against Spanner Omni or a real Cloud Spanner instance. The full
class has been confirmed against both backends, aside from one real-Spanner-specific failure
called out below (`shouldCarryUnchangedColumnsThroughOnPartialUpdate`).

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
old-to-new transition only for the column that was actually touched. Passes against the
emulator and Spanner Omni. **Fails against a real Cloud Spanner instance**, on both
partition modes: an unchanged column comes back `null` in the UPDATE's `before`/`after`
struct instead of its real value. Confirmed reproducible across two independent runs; root
cause not yet investigated. The equivalent unchanged-column check on a DELETE's `before`
struct (`shouldCarryLastKnownValuesInBeforeOnDelete`, above) passes fine against real
Spanner, so the gap looks specific to the UPDATE code path.

**`shouldPickUpColumnAddedAfterStreamCreationWithoutReconfiguring`**
The parameterized (all-partition-mode) equivalent of `MutableKeyRangeIT`'s schema-change
tests above: inserts a row, adds a column mid-stream via DDL, inserts a second row using the
new column, and updates the first row's new column. Confirms records before and after the
schema change all look correct.

**`shouldDefaultTransactionTagAndSystemTransactionFlagForOrdinaryWrites`**
Performs a plain insert with no explicit transaction tag. Confirms the record's source
metadata shows an empty tag and a `false` system-transaction flag by default.

**`shouldSurfaceExplicitTransactionTag`**
Runs an insert inside a transaction carrying an explicit tag and confirms the tag surfaces on
the record. Only runs against a real Cloud Spanner instance (`-Preal-spanner`) - confirmed
passing there, twice. Skipped otherwise: the local Docker emulator doesn't propagate
transaction tags through its change stream at all.

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
for `IMMUTABLE_KEY_RANGE` (always), Spanner Omni or a real Cloud Spanner instance for
`MUTABLE_KEY_RANGE` (whichever non-emulator backend is available). The full class has been
confirmed against both backends, aside from one real-Spanner-specific failure called out below
(`shouldResumeWithoutDuplicatingOrLosingContentAcrossRestart`).

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
to confirm the connector keeps working normally afterward. Passes against the emulator and
Spanner Omni. **Fails against a real Cloud Spanner instance**, on both partition modes: the
pre-restart insert is redelivered alongside the legitimate post-restart update, so the
"exactly once" assertion doesn't hold there. Root cause not yet investigated - could be a
genuine at-least-once duplicate this assertion is too strict to tolerate, or a real
difference in restart/offset behavior under real Spanner.

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

## `PlacementMoveIT`

**Currently `@Disabled`**: `DROP PLACEMENT` alone can take minutes to hours on the shared
real-Spanner test instance, making iteration on this suite expensive; re-enable once stable.

Three real-Cloud-Spanner placement-move scenarios sharing a single `east`/`west` placement
pair, provisioned once for the whole class (one `@BeforeAll`/`@AfterAll`) since
`DROP PLACEMENT` alone can take minutes to hours on the shared test instance. Each test
still creates and drops its own tables/change stream inline.

**Runs against:** a real Cloud Spanner instance only - the class is
`@RealSpannerCompatible` and opts into real Spanner via
`-Dspanner.test.real=true` (see
[`doc/real-spanner-testing.md`](../doc/real-spanner-testing.md)). Requires
the pre-provisioned `east-partition`/`west-partition` instance partitions
described there too. The local emulator and Spanner Omni can't run any of
these three tests (DDL rejection and disabled geo-partitioning,
respectively).

Spanner also restricts placement tables to a single `INSERT` or `DELETE` DML statement per
transaction during preview, so the two interleaved-child tests below issue the parent and
child inserts as separate transactions rather than combining them into one.

**`shouldOrderRecordsCorrectlyWhenRowMovesBetweenPlacements`**
Inserts a row in one geo-partitioned placement, moves it to another placement by updating
its placement key, then immediately issues a follow-up update to that same row. Confirms
the move event shows the correct before/after placement values, and that the follow-up
write's timestamp is strictly later than the move's - verifying the connector doesn't
deliver the follow-up out of order relative to the placement move. This is the first
geo-partitioned placement scenario confirmed working end to end through the actual
deployed connector.

**`shouldMoveInterleavedChildRowsWithParentPlacementChange`**
Inserts a parent row with an interleaved child row (the child has no placement key of
its own and always moves with its parent), then moves the parent between placements.
Confirms the parent's move event shows the new region, and that a follow-up update on the
child is correctly ordered after the parent's move timestamp - answering an open question
about whether the child emits its own visible move signal or the move is only observable
indirectly through correct ordering.

**`shouldOrderCascadingDeleteCorrectlyRelativeToInFlightPlacementMove`**
Inserts a parent row with an interleaved child (using `ON DELETE CASCADE`), moves the
parent to a different placement, then immediately deletes the parent so the delete cascades
to the child. Confirms both parent and child produce the expected
insert/move/delete/tombstone events, that the parent and child deletes share one
transaction ID, and that there's exactly one delete and one tombstone per key - verifying no
duplicate or dropped delete arises from the move and the cascade both needing to explain the
same row's disappearance at once.

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

