/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.db.Database
import io.airbyte.domain.models.fusion.ActorEnablementFlags
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/** Dedicated column access keeps generic actor/entity writes from overwriting enablement intent. */
@Singleton
class ActorEnablementPersistence(
  @Named("configDatabase") private val database: Database,
) {
  fun read(
    organizationId: UUID,
    actorId: UUID,
    actorType: String,
  ): ActorEnablementRecord? =
    database.query { ctx ->
      ctx
        .fetchOne(
          """
          SELECT a.id, a.workspace_id, a.enable_agent_access, a.enable_indexing, a.enable_backfill, a.backfill_start_time FROM actor a JOIN workspace w ON w.id = a.workspace_id
          WHERE w.organization_id = ? AND a.id = ? AND a.actor_type::text = ?
            AND a.tombstone = false AND w.tombstone = false
          """.trimIndent(),
          organizationId,
          actorId,
          actorType,
        )?.let {
          ActorEnablementRecord(
            it.get("id", UUID::class.java),
            it.get("workspace_id", UUID::class.java),
            ActorEnablementFlags(
              it.get("enable_agent_access", Boolean::class.java),
              it.get("enable_indexing", Boolean::class.java),
              it.get("enable_backfill", Boolean::class.java),
              it.get("backfill_start_time", OffsetDateTime::class.java)?.toInstant(),
            ),
          )
        }
    }

  fun compareAndSet(
    organizationId: UUID,
    workspaceId: UUID,
    actorId: UUID,
    actorType: String,
    expected: ActorEnablementFlags,
    desired: ActorEnablementFlags,
  ): Boolean =
    database.query { ctx ->
      ctx.execute(
        """
        UPDATE actor a SET enable_agent_access = ?, enable_indexing = ?, enable_backfill = ?, backfill_start_time = ?::timestamptz, updated_at = NOW()
        FROM workspace w
        WHERE a.workspace_id = w.id AND w.organization_id = ? AND w.id = ?
          AND a.id = ? AND a.actor_type::text = ? AND a.tombstone = false AND w.tombstone = false
          AND a.enable_agent_access = ? AND a.enable_indexing = ? AND a.enable_backfill = ?
          AND a.backfill_start_time IS NOT DISTINCT FROM ?::timestamptz
        """.trimIndent(),
        desired.enableAgentAccess,
        desired.enableIndexing,
        desired.enableBackfill,
        desired.backfillStartTime?.atOffset(ZoneOffset.UTC),
        organizationId,
        workspaceId,
        actorId,
        actorType,
        expected.enableAgentAccess,
        expected.enableIndexing,
        expected.enableBackfill,
        expected.backfillStartTime?.atOffset(ZoneOffset.UTC),
      ) == 1
    }

  /** Every tenant table is scoped; the authenticated service account must own the assigned dataplane. */
  fun assignedSyncInput(
    organizationId: UUID,
    workspaceId: UUID,
    connectionId: UUID,
    sourceId: UUID,
    destinationId: UUID,
    workloadId: String,
    dataplaneGroupId: UUID,
    serviceAccountId: UUID,
  ): AssignedSyncEnablementRecord? =
    database.query { ctx ->
      ctx
        .fetchOne(
          """
          SELECT wl.input_payload, s.enable_indexing AS source_search_indexing, d.enable_indexing AS destination_search_indexing FROM workload wl
          JOIN dataplane dp ON dp.id::text = wl.dataplane_id
          JOIN connection c ON c.id = ?
          JOIN actor s ON s.id = c.source_id
          JOIN workspace sw ON sw.id = s.workspace_id AND sw.organization_id = ?
          JOIN actor d ON d.id = c.destination_id
          JOIN workspace dw ON dw.id = d.workspace_id AND dw.organization_id = ?
          WHERE wl.id = ? AND wl.organization_id = ? AND wl.workspace_id = ? AND wl.type::text = 'sync'
            AND wl.status::text IN ('claimed', 'launched', 'running')
            AND dp.service_account_id = ? AND dp.dataplane_group_id = ? AND dp.enabled = true AND dp.tombstone = false
            AND wl.dataplane_group = ? AND s.id = ? AND d.id = ?
            AND sw.id = ? AND dw.id = ? AND sw.tombstone = false AND dw.tombstone = false
            AND s.tombstone = false AND d.tombstone = false AND s.actor_type::text = 'source' AND d.actor_type::text = 'destination'
            AND c.status::text != 'deprecated'
          """.trimIndent(),
          connectionId,
          organizationId,
          organizationId,
          workloadId,
          organizationId,
          workspaceId,
          serviceAccountId,
          dataplaneGroupId,
          dataplaneGroupId.toString(),
          sourceId,
          destinationId,
          workspaceId,
          workspaceId,
        )?.let {
          AssignedSyncEnablementRecord(
            it.get("input_payload", String::class.java),
            it.get("source_search_indexing", Boolean::class.java),
            it.get("destination_search_indexing", Boolean::class.java),
          )
        }
    }
}

data class ActorEnablementRecord(
  val actorId: UUID,
  val workspaceId: UUID,
  val flags: ActorEnablementFlags,
)

data class AssignedSyncEnablementRecord(
  val inputPayload: String,
  val sourceSearchIndexing: Boolean,
  val destinationSearchIndexing: Boolean,
)
