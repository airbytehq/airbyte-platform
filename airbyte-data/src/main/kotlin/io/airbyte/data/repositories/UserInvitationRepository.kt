package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.UserInvitation
import io.airbyte.data.services.impls.data.mappers.EntityInvitationStatus
import io.airbyte.data.services.impls.data.mappers.EntityScopeType
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface UserInvitationRepository : PageableRepository<UserInvitation, UUID> {
  fun findByInviteCode(inviteCode: String): Optional<UserInvitation>

  fun findByStatusAndScopeTypeAndScopeId(
    status: EntityInvitationStatus,
    scopeType: EntityScopeType,
    scopeId: UUID,
  ): List<UserInvitation>

  fun findByStatusAndScopeTypeAndScopeIdAndInvitedEmail(
    status: EntityInvitationStatus,
    scopeType: EntityScopeType,
    scopeId: UUID,
    invitedEmail: String,
  ): List<UserInvitation>
}
