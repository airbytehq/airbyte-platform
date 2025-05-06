/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ActorDefinition
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ActorDefinitionRepository : PageableRepository<ActorDefinition, UUID> {
  @Query("SELECT * FROM actor_definition a WHERE a.id = :actorDefinitionId")
  fun findByActorDefinitionId(actorDefinitionId: UUID): ActorDefinition?
}
