package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.UserInvitation
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

typealias EntityScopeType = io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType
typealias ModelScopeType = io.airbyte.config.ScopeType
typealias EntityInvitationStatus = io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus
typealias ModelInvitationStatus = io.airbyte.config.InvitationStatus
typealias EntityUserInvitation = UserInvitation
typealias ModelUserInvitation = io.airbyte.config.UserInvitation

fun EntityScopeType.toConfigModel(): ModelScopeType {
  return when (this) {
    EntityScopeType.organization -> ModelScopeType.ORGANIZATION
    EntityScopeType.workspace -> ModelScopeType.WORKSPACE
  }
}

fun ModelScopeType.toEntity(): EntityScopeType {
  return when (this) {
    ModelScopeType.ORGANIZATION -> EntityScopeType.organization
    ModelScopeType.WORKSPACE -> EntityScopeType.workspace
  }
}

fun EntityInvitationStatus.toConfigModel(): ModelInvitationStatus {
  return when (this) {
    EntityInvitationStatus.pending -> ModelInvitationStatus.PENDING
    EntityInvitationStatus.accepted -> ModelInvitationStatus.ACCEPTED
    EntityInvitationStatus.cancelled -> ModelInvitationStatus.CANCELLED
    EntityInvitationStatus.declined -> ModelInvitationStatus.DECLINED
    EntityInvitationStatus.expired -> ModelInvitationStatus.EXPIRED
  }
}

fun ModelInvitationStatus.toEntity(): EntityInvitationStatus {
  return when (this) {
    ModelInvitationStatus.PENDING -> EntityInvitationStatus.pending
    ModelInvitationStatus.ACCEPTED -> EntityInvitationStatus.accepted
    ModelInvitationStatus.CANCELLED -> EntityInvitationStatus.cancelled
    ModelInvitationStatus.DECLINED -> EntityInvitationStatus.declined
    ModelInvitationStatus.EXPIRED -> EntityInvitationStatus.expired
  }
}

fun EntityUserInvitation.toConfigModel(): ModelUserInvitation {
  return ModelUserInvitation()
    .withId(this.id)
    .withInviteCode(this.inviteCode)
    .withInviterUserId(this.inviterUserId)
    .withInvitedEmail(this.invitedEmail)
    .withAcceptedByUserId(this.acceptedByUserId)
    .withScopeId(this.scopeId)
    .withScopeType(this.scopeType.toConfigModel())
    .withPermissionType(this.permissionType.toConfigModel())
    .withStatus(this.status.toConfigModel())
    .withCreatedAt(this.createdAt?.toEpochSecond())
    .withUpdatedAt(this.updatedAt?.toEpochSecond())
    .withExpiresAt(this.expiresAt.toEpochSecond())
}

fun ModelUserInvitation.toEntity(): EntityUserInvitation {
  return EntityUserInvitation(
    id = this.id,
    inviteCode = this.inviteCode,
    inviterUserId = this.inviterUserId,
    invitedEmail = this.invitedEmail,
    acceptedByUserId = this.acceptedByUserId,
    scopeId = this.scopeId,
    scopeType = this.scopeType.toEntity(),
    permissionType = this.permissionType.toEntity(),
    status = this.status.toEntity(),
    createdAt = this.createdAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    updatedAt = this.updatedAt?.let { OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) },
    expiresAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(this.expiresAt), ZoneOffset.UTC),
  )
}
