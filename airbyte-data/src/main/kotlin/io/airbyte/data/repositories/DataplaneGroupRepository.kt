/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneGroup
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataplaneGroupRepository : PageableRepository<DataplaneGroup, UUID> {
  fun save(dataplaneGroup: DataplaneGroup): DataplaneGroup

  fun findAllByOrganizationIdOrderByUpdatedAtDesc(organizationId: UUID): List<DataplaneGroup>

  fun findAllByOrganizationIdAndTombstoneFalseOrderByUpdatedAtDesc(organizationId: UUID): List<DataplaneGroup>
}
