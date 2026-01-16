/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Dataplane
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataplaneRepository : PageableRepository<Dataplane, UUID> {
  fun save(dataplane: Dataplane): Dataplane

  fun findAllByDataplaneGroupIdOrderByUpdatedAtDesc(dataplaneGroupId: UUID): List<Dataplane>

  fun findAllByDataplaneGroupIdAndTombstoneFalseOrderByUpdatedAtDesc(dataplaneGroupId: UUID): List<Dataplane>

  @Query(
    """
    SELECT d.* FROM dataplane d
    WHERE d.dataplane_group_id IN (:dataplaneGroupIds)
    AND (:withTombstone = true OR d.tombstone = false)
    ORDER BY d.updated_at DESC
    """,
  )
  fun findAllByDataplaneGroupIds(
    dataplaneGroupIds: List<UUID>,
    withTombstone: Boolean,
  ): List<Dataplane>

  fun findAllByTombstone(withTombstone: Boolean): List<Dataplane>

  fun findByServiceAccountId(serviceAccountId: UUID): Dataplane?

  @Query(
    """
    SELECT d.* FROM dataplane d
    INNER JOIN dataplane_group dg ON d.dataplane_group_id = dg.id
    WHERE (:withTombstone = true OR d.tombstone = false)
    AND dg.organization_id IN (:organizationIds)
    ORDER BY d.updated_at DESC
  """,
  )
  fun findAllByOrganizationIds(
    organizationIds: List<UUID>,
    withTombstone: Boolean,
  ): List<Dataplane>
}
