/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Actor
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ActorRepository : PageableRepository<Actor, UUID> {
  @Query("SELECT * FROM Actor a WHERE a.id = :actorId")
  fun findByActorId(actorId: UUID): Actor?

  @Query("SELECT workspace_id FROM actor WHERE id = :sourceId AND actor_type = 'source'")
  fun findSourceWorkspaceId(sourceId: UUID): Optional<UUID>

  @Query("SELECT workspace_id FROM actor WHERE id = :destinationId AND actor_type = 'destination'")
  fun findDestinationWorkspaceId(destinationId: UUID): Optional<UUID>

  @Query("SELECT actor_type FROM actor_definition WHERE id = :definitionId")
  fun findDefinitionTypeById(definitionId: UUID): Optional<ActorType>

  @Query("SELECT EXISTS(SELECT 1 FROM actor_catalog_fetch_event WHERE actor_catalog_id = :catalogId AND actor_id = :actorId)")
  fun catalogWasFetchedByActor(
    catalogId: UUID,
    actorId: UUID,
  ): Boolean

  @Query("SELECT actor_id FROM actor_catalog_fetch_event WHERE actor_catalog_id = :catalogId ORDER BY created_at DESC LIMIT 1")
  fun findLatestActorIdByCatalogId(catalogId: UUID): Optional<UUID>

  @Query(
    """
    SELECT DISTINCT a.workspace_id FROM actor_catalog_fetch_event f
    JOIN actor a ON a.id = f.actor_id
    WHERE f.actor_catalog_id = :catalogId AND a.actor_type = 'source'
    """,
  )
  fun findSourceWorkspaceIdsByCatalogId(catalogId: UUID): List<UUID>

  @Query(
    """
    SELECT EXISTS(
      SELECT 1 FROM workload wl
      JOIN dataplane dp ON dp.id::text = wl.dataplane_id
      JOIN connection c ON c.id = :connectionId
      JOIN actor s ON s.id = c.source_id
      JOIN workspace sw ON sw.id = s.workspace_id AND sw.organization_id = :organizationId
      JOIN actor d ON d.id = c.destination_id
      JOIN workspace dw ON dw.id = d.workspace_id AND dw.organization_id = :organizationId
      WHERE wl.id = :workloadId AND wl.organization_id = :organizationId AND wl.workspace_id = :workspaceId
        AND wl.type::text = 'sync' AND wl.status::text IN ('claimed', 'launched', 'running')
        AND dp.service_account_id = :serviceAccountId AND dp.dataplane_group_id = :dataplaneGroupId
        AND dp.enabled = true AND dp.tombstone = false AND wl.dataplane_group = CAST(:dataplaneGroupId AS text)
        AND s.id = :sourceId AND d.id = :destinationId AND sw.id = :workspaceId AND dw.id = :workspaceId
        AND sw.tombstone = false AND dw.tombstone = false AND s.tombstone = false AND d.tombstone = false
        AND s.actor_type::text = 'source' AND d.actor_type::text = 'destination' AND c.status::text != 'deprecated'
    )
    """,
  )
  fun isAssignedSyncWorkload(
    organizationId: UUID,
    workspaceId: UUID,
    connectionId: UUID,
    sourceId: UUID,
    destinationId: UUID,
    workloadId: String,
    dataplaneGroupId: UUID,
    serviceAccountId: UUID,
  ): Boolean

  @Query("SELECT actor_definition_id FROM actor WHERE id = :actorId AND workspace_id = :workspaceId")
  fun findActorDefinitionIdByIdAndWorkspaceId(
    actorId: UUID,
    workspaceId: UUID,
  ): Optional<UUID>
}
