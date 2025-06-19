/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.PartialUserConfig
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface PartialUserConfigRepository : PageableRepository<PartialUserConfig, UUID> {
  fun findByWorkspaceIdAndTombstoneFalse(workspaceId: UUID): List<PartialUserConfig>

  fun findByIdAndTombstoneFalse(id: UUID): Optional<PartialUserConfig>

  fun findByActorId(actorId: UUID): List<PartialUserConfig>
}
