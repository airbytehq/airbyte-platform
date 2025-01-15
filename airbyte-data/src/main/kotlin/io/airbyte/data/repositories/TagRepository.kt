package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Tag
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface TagRepository : PageableRepository<Tag, UUID> {
  fun findByWorkspaceId(workspaceId: UUID): List<Tag>

  fun countByWorkspaceId(workspaceId: UUID): Int

  fun findByIdForUpdate(id: UUID): Tag

  fun findByIdAndWorkspaceId(
    id: UUID,
    workspaceId: UUID,
  ): Tag
}
