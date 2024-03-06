package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.UserInvitation
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface UserInvitationRepository : PageableRepository<UserInvitation, UUID> {
  fun findByInviteCode(inviteCode: String): Optional<UserInvitation>
}
