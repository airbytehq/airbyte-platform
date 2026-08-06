# AGENTS.md — `oss/airbyte-workers/`

Conventions and implementation guidance for Temporal workflows, sync
activities, connector commands, and connection scheduling. Read the root
[AGENTS.md](../../AGENTS.md) and [`oss/AGENTS.md`](../AGENTS.md) first.

## Module scope

- Temporal workflow implementations and activities for connection scheduling,
  syncs, retries, post-processing, and connector commands.
- Connector command strategies and workload/API coordination.
- Schedule wait-time calculation, cron handling, capacity checks, and
  workflow state carried across `continueAsNew`.

Keep workflow decision logic deterministic. Put external I/O in activities,
use constructor-injected Micronaut beans, and preserve the distinction between
workflow contracts in `airbyte-commons-temporal` and implementations here.

## Key implementation paths

- `src/main/kotlin/io/airbyte/workers/temporal/scheduling/ConnectionManagerWorkflowImpl.kt`
- `src/main/kotlin/io/airbyte/workers/temporal/scheduling/activities/ConfigFetchActivityImpl.kt`
- `src/main/kotlin/io/airbyte/workers/helpers/CronSchedulingHelper.kt`
- `src/main/kotlin/io/airbyte/workers/temporal/sync/SyncWorkflowV2Impl.kt`
- `src/main/kotlin/io/airbyte/workers/temporal/workflows/ConnectorCommandWorkflow.kt`

Further reading: [Temporal orchestration](temporal-orchestration.md) and
[connection scheduling](connection-scheduling.md).
