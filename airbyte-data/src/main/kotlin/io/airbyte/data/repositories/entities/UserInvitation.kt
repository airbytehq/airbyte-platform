package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.util.UUID

@MappedEntity("user_invitation")
data class UserInvitation(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var inviteCode: String,
  var inviterUserId: UUID,
  var invitedEmail: String,
  @Nullable
  var acceptedByUserId: UUID? = null,
  var scopeId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  var scopeType: ScopeType,
  @field:TypeDef(type = DataType.OBJECT)
  var permissionType: PermissionType,
  @field:TypeDef(type = DataType.OBJECT)
  var status: InvitationStatus,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: java.time.OffsetDateTime? = null,
  var expiresAt: java.time.OffsetDateTime,
)
