/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneGroup
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataplaneGroupRepository : PageableRepository<DataplaneGroup, UUID> {
  fun save(dataplaneGroup: DataplaneGroup): DataplaneGroup

  fun findAllByOrganizationIdInOrderByUpdatedAtDesc(organizationIds: List<UUID>): List<DataplaneGroup>

  fun findAllByOrganizationIdAndNameIgnoreCase(
    organizationId: UUID,
    name: String,
  ): List<DataplaneGroup>

  fun findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(organizationIds: List<UUID>): List<DataplaneGroup>

  @Query(
    """
      SELECT dg.organization_id
      FROM dataplane_group dg
      WHERE dg.id = :dataplaneGroup
    """,
  )
  fun getOrganizationIdFromDataplaneGroup(dataplaneGroup: UUID): UUID
}
