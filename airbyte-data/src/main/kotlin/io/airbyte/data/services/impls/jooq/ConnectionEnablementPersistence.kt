/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.db.Database
import io.airbyte.domain.models.fusion.ConnectionEnablementFlags
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

/** Dedicated column access keeps generic connection writes from overwriting enablement intent. */
@Singleton
class ConnectionEnablementPersistence(
  @Named("configDatabase") private val database: Database,
) {
  fun read(
    organizationId: UUID,
    connectionId: UUID,
  ): ConnectionEnablementRecord? =
    database.query { ctx ->
      ctx
        .fetchOne(
          """
          SELECT c.id, sw.id AS workspace_id, c.source_id, c.destination_id, c.enable_indexing FROM connection c
          JOIN actor s ON s.id = c.source_id
          JOIN workspace sw ON sw.id = s.workspace_id
          JOIN actor d ON d.id = c.destination_id
          JOIN workspace dw ON dw.id = d.workspace_id
          WHERE c.id = ? AND sw.organization_id = ? AND dw.organization_id = ? AND sw.id = dw.id
            AND s.tombstone = false AND d.tombstone = false AND sw.tombstone = false AND dw.tombstone = false
            AND c.status::text != 'deprecated'
          """.trimIndent(),
          connectionId,
          organizationId,
          organizationId,
        )?.let {
          ConnectionEnablementRecord(
            it.get("id", UUID::class.java),
            it.get("workspace_id", UUID::class.java),
            it.get("source_id", UUID::class.java),
            it.get("destination_id", UUID::class.java),
            ConnectionEnablementFlags(
              it.get("enable_indexing", Boolean::class.java),
            ),
          )
        }
    }

  fun compareAndSet(
    organizationId: UUID,
    workspaceId: UUID,
    connectionId: UUID,
    expected: ConnectionEnablementFlags,
    desired: ConnectionEnablementFlags,
  ): Boolean =
    database.query { ctx ->
      ctx.execute(
        """
        UPDATE connection c SET enable_indexing = ?, updated_at = NOW()
        FROM actor s
        JOIN workspace sw ON sw.id = s.workspace_id,
        actor d
        JOIN workspace dw ON dw.id = d.workspace_id
        WHERE c.source_id = s.id AND c.destination_id = d.id AND sw.organization_id = ? AND dw.organization_id = ? AND sw.id = ?
          AND sw.id = dw.id AND c.id = ? AND s.tombstone = false AND d.tombstone = false
          AND sw.tombstone = false AND dw.tombstone = false AND c.status::text != 'deprecated'
          AND c.enable_indexing = ?
        """.trimIndent(),
        desired.enableIndexing,
        organizationId,
        organizationId,
        workspaceId,
        connectionId,
        expected.enableIndexing,
      ) == 1
    }
}

data class ConnectionEnablementRecord(
  val connectionId: UUID,
  val workspaceId: UUID,
  val sourceId: UUID,
  val destinationId: UUID,
  val flags: ConnectionEnablementFlags,
)
