/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.Job
import io.airbyte.config.Permission
import io.airbyte.config.ScopeType
import io.airbyte.config.UserInvitation
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.repositories.AuthorizationCommandRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.OrganizationDomainVerificationRepository
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.PrivateLinkService
import io.airbyte.data.services.SecretStorageService
import io.airbyte.data.services.TagService
import io.airbyte.data.services.UserInvitationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import jakarta.inject.Singleton
import java.util.UUID

/** Fresh, non-secret saved relationships for authorization. */
@Singleton
class AuthorizationDataService(
  private val workspaces: WorkspaceService,
  private val permissions: PermissionService,
  private val actors: ActorDefinitionService,
  private val connections: ConnectionService,
  private val operations: OperationService,
  private val jobs: JobService,
  private val organizations: OrganizationService,
  private val dataplanes: DataplaneService,
  private val dataplaneGroups: DataplaneGroupService,
  private val groups: GroupRepository,
  private val verifications: OrganizationDomainVerificationRepository,
  private val secretStorages: SecretStorageService,
  private val tags: TagService,
  private val projects: ConnectorBuilderService,
  private val commands: AuthorizationCommandRepository,
  private val users: UserPersistence,
  private val invitations: UserInvitationService,
  private val privateLinks: PrivateLinkService,
  private val connectionEvents: ConnectionTimelineEventService,
) {
  fun workspaceAndOrganization(workspaceId: UUID): WorkspaceAndOrganization {
    val workspace = resource { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) }
    requireResource(workspace.workspaceId == workspaceId)
    val organizationId = workspace.organizationId ?: throw AuthorizationResourceNotFoundException()
    return WorkspaceAndOrganization(workspaceId, organizationId)
  }

  fun actorWorkspaceAndOrganization(actorId: UUID): WorkspaceAndOrganization {
    val actor = actors.getIdsForActors(listOf(actorId)).singleOrNull() ?: throw AuthorizationResourceNotFoundException()
    requireResource(actor.actorId == actorId)
    val organizationId = actor.organizationId ?: throw AuthorizationResourceNotFoundException()
    return WorkspaceAndOrganization(actor.workspaceId, organizationId)
  }

  fun sourceWorkspaceAndOrganization(sourceId: UUID): WorkspaceAndOrganization = actorWorkspaceAndOrganization(sourceId)

  fun destinationWorkspaceAndOrganization(destinationId: UUID): WorkspaceAndOrganization = actorWorkspaceAndOrganization(destinationId)

  fun privateLinkWorkspaceAndOrganization(privateLinkId: UUID): WorkspaceAndOrganization {
    val privateLink =
      try {
        privateLinks.getPrivateLink(privateLinkId)
      } catch (_: NoSuchElementException) {
        throw AuthorizationResourceNotFoundException()
      }
    requireResource(privateLink.id == privateLinkId)
    return workspaceAndOrganization(privateLink.workspaceId)
  }

  fun connectionEventWorkspaceAndOrganization(
    connectionEventId: UUID,
    expectedConnectionId: UUID,
  ): WorkspaceAndOrganization {
    val event =
      try {
        connectionEvents.getEvent(connectionEventId)
      } catch (_: NoSuchElementException) {
        throw AuthorizationResourceNotFoundException()
      }
    requireResource(event.id == connectionEventId && event.connectionId == expectedConnectionId)
    return connectionWorkspaceAndOrganization(event.connectionId)
  }

  fun connectionWorkspaceAndOrganization(connectionId: UUID): WorkspaceAndOrganization {
    val connection = resource { connections.getStandardSync(connectionId) }
    requireResource(connection.connectionId == connectionId)
    val sourceId = connection.sourceId ?: throw AuthorizationResourceNotFoundException()
    val destinationId = connection.destinationId ?: throw AuthorizationResourceNotFoundException()
    val source = sourceWorkspaceAndOrganization(sourceId)
    requireResource(source == destinationWorkspaceAndOrganization(destinationId))
    return source
  }

  fun operationWorkspaceAndOrganization(operationId: UUID): WorkspaceAndOrganization {
    val operation = resource { operations.getStandardSyncOperation(operationId) }
    requireResource(operation.operationId == operationId)
    val workspaceId = operation.workspaceId ?: throw AuthorizationResourceNotFoundException()
    return workspaceAndOrganization(workspaceId)
  }

  fun jobWorkspaceAndOrganization(jobId: Long): WorkspaceAndOrganization {
    val job = jobs.findById(jobId) ?: throw AuthorizationResourceNotFoundException()
    requireResource(job.id == jobId && job.configType in Job.REPLICATION_TYPES)
    val connectionId =
      try {
        UUID.fromString(job.scope)
      } catch (_: IllegalArgumentException) {
        throw AuthorizationResourceNotFoundException()
      }
    return connectionWorkspaceAndOrganization(connectionId)
  }

  fun permissionTarget(permissionId: UUID): AuthorizationTarget {
    val permission = resource { permissions.getPermission(permissionId) }
    requireResource(permission.permissionId == permissionId)
    val workspaceId = permission.workspaceId
    val organizationId = permission.organizationId
    requireResource(workspaceId == null || organizationId == null)
    if (workspaceId != null) return AuthorizationTarget.Workspace(workspaceAndOrganization(workspaceId))
    if (organizationId != null) return AuthorizationTarget.Organization(organization(organizationId))
    requireResource(permission.permissionType in setOf(Permission.PermissionType.INSTANCE_ADMIN, Permission.PermissionType.DATAPLANE))
    return AuthorizationTarget.Global
  }

  fun organization(organizationId: UUID): UUID {
    val organization = organizations.getOrganization(organizationId).orElseThrow { AuthorizationResourceNotFoundException() }
    requireResource(organization.organizationId == organizationId)
    return organizationId
  }

  fun dataplaneOrganization(dataplaneId: UUID): UUID {
    val dataplane = resource { dataplanes.getDataplane(dataplaneId) }
    requireResource(dataplane.id == dataplaneId)
    val groupId = dataplane.dataplaneGroupId ?: throw AuthorizationResourceNotFoundException()
    return dataplaneGroupOrganization(groupId)
  }

  fun dataplaneGroupOrganization(dataplaneGroupId: UUID): UUID {
    val group = resource { dataplaneGroups.getDataplaneGroup(dataplaneGroupId) }
    requireResource(group.id == dataplaneGroupId)
    val organizationId = group.organizationId ?: throw AuthorizationResourceNotFoundException()
    return organization(organizationId)
  }

  fun groupOrganization(groupId: UUID): UUID {
    val group = groups.findById(groupId).orElseThrow { AuthorizationResourceNotFoundException() }
    requireResource(group.id == groupId)
    return organization(group.organizationId)
  }

  fun domainVerificationOrganization(verificationId: UUID): UUID {
    val verification = verifications.findById(verificationId).orElseThrow { AuthorizationResourceNotFoundException() }
    requireResource(verification.id == verificationId && !verification.tombstone)
    return organization(verification.organizationId)
  }

  fun secretStorageTarget(secretStorageId: UUID): AuthorizationTarget {
    val storage = secretStorages.findById(SecretStorageId(secretStorageId)) ?: throw AuthorizationResourceNotFoundException()
    requireResource(storage.id == SecretStorageId(secretStorageId) && !storage.tombstone)
    return when (storage.scopeType) {
      SecretStorageScopeType.WORKSPACE -> AuthorizationTarget.Workspace(workspaceAndOrganization(storage.scopeId))
      SecretStorageScopeType.ORGANIZATION -> AuthorizationTarget.Organization(organization(storage.scopeId))
    }
  }

  fun tagWorkspaceAndOrganization(tagId: UUID): WorkspaceAndOrganization {
    val tag = resource { tags.getTagById(tagId) }
    requireResource(tag.tagId == tagId)
    val workspaceId = tag.workspaceId ?: throw AuthorizationResourceNotFoundException()
    return workspaceAndOrganization(workspaceId)
  }

  fun builderProjectWorkspaceAndOrganization(builderProjectId: UUID): WorkspaceAndOrganization {
    val project = resource { projects.getConnectorBuilderProject(builderProjectId, false) }
    requireResource(project.builderProjectId == builderProjectId)
    val workspaceId = project.workspaceId ?: throw AuthorizationResourceNotFoundException()
    return workspaceAndOrganization(workspaceId)
  }

  fun commandWorkspaceAndOrganization(
    commandId: String,
    organizationId: UUID,
  ): WorkspaceAndOrganization {
    val command = commands.findByIdAndOrganizationId(commandId, organizationId).orElseThrow { AuthorizationResourceNotFoundException() }
    requireResource(command.id == commandId && command.organizationId == organizationId)
    val workspace = workspaceAndOrganization(command.workspaceId)
    requireResource(workspace.organizationId == organizationId)
    return workspace
  }

  fun userAuthSubjects(userId: UUID): Set<String> {
    val subjects = users.listAuthUserIdsForUser(userId).toSet()
    requireResource(subjects.isNotEmpty() && subjects.none(String::isBlank))
    return subjects
  }

  fun invitationTarget(invitationId: UUID): AuthorizationTarget {
    val invitation = resource { invitations.getUserInvitationById(invitationId) }
    requireResource(invitation.id == invitationId)
    return invitationTarget(invitation)
  }

  fun invitationTargetByCode(inviteCode: String): AuthorizationTarget {
    val invitation = resource { invitations.getUserInvitationByInviteCode(inviteCode) }
    requireResource(invitation.inviteCode == inviteCode)
    return invitationTarget(invitation)
  }

  private fun invitationTarget(invitation: UserInvitation): AuthorizationTarget {
    val scopeId = invitation.scopeId ?: throw AuthorizationResourceNotFoundException()
    return when (invitation.scopeType) {
      ScopeType.WORKSPACE -> AuthorizationTarget.Workspace(workspaceAndOrganization(scopeId))
      ScopeType.ORGANIZATION -> AuthorizationTarget.Organization(organization(scopeId))
      null -> throw AuthorizationResourceNotFoundException()
    }
  }

  private fun requireResource(valid: Boolean) {
    if (!valid) throw AuthorizationResourceNotFoundException()
  }

  private fun <T> resource(lookup: () -> T): T =
    try {
      lookup()
    } catch (_: io.airbyte.data.ConfigNotFoundException) {
      throw AuthorizationResourceNotFoundException()
    } catch (_: io.airbyte.config.persistence.ConfigNotFoundException) {
      throw AuthorizationResourceNotFoundException()
    }
}
