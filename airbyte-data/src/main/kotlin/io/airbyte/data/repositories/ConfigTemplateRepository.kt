/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ConfigTemplate
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ConfigTemplateRepository : PageableRepository<ConfigTemplate, UUID> {
  fun findByOrganizationId(organizationId: UUID): List<ConfigTemplate>

  fun findByActorDefinitionIdInAndOrganizationIdIsNullAndTombstoneFalse(actorDefinitionIds: List<UUID>): List<ConfigTemplate>

  fun findByOrganizationIdAndActorDefinitionIdInAndTombstoneFalse(
    organizationId: UUID,
    actorDefinitionIds: List<UUID>,
  ): List<ConfigTemplate>
}
