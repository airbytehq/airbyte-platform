/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.ListWorkspacesByUserRequestBody
import io.airbyte.api.server.generated.models.ListWorkspacesInOrganizationRequestBody
import io.airbyte.api.server.generated.models.SlugRequestBody
import io.airbyte.api.server.generated.models.WorkspaceCreate
import io.airbyte.api.server.generated.models.WorkspaceCreateWithId
import io.airbyte.api.server.generated.models.WorkspaceDataWorkerAvailabilityRequestBody
import io.airbyte.api.server.generated.models.WorkspaceGetDbtJobsRequest
import io.airbyte.api.server.generated.models.WorkspaceGiveFeedback
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceUpdate
import io.airbyte.api.server.generated.models.WorkspaceUpdateName
import io.airbyte.api.server.generated.models.WorkspaceUpdateOrganization
import io.airbyte.api.server.generated.models.WorkspaceUsageRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface WorkspaceOperationInput : OperationInput {
  data class Saved(
    val ids: List<UUID>,
    val deleted: Boolean = false,
    val organization: Optional<UUID> = Optional.empty(),
    val dataplaneGroup: Optional<UUID> = Optional.empty(),
    val dbtConfig: Optional<UUID> = Optional.empty(),
  ) : WorkspaceOperationInput

  data class Slug(
    val slug: String,
  ) : WorkspaceOperationInput

  data class Connection(
    val id: UUID,
  ) : WorkspaceOperationInput

  data class Create(
    val organization: UUID,
    val id: Optional<UUID>,
    val dataplaneGroup: Optional<UUID>,
  ) : WorkspaceOperationInput

  data class Transfer(
    val id: UUID,
    val organization: UUID,
  ) : WorkspaceOperationInput

  data class FilteredOrganization(
    val id: UUID,
  ) : WorkspaceOperationInput

  data class User(
    val id: UUID,
  ) : WorkspaceOperationInput
}

private fun workspaceInput(args: Array<out Any?>): WorkspaceOperationInput =
  when (val body = args.firstOrNull()) {
    is WorkspaceIdRequestBody -> WorkspaceOperationInput.Saved(listOf(body.workspaceId), body.includeTombstone ?: false)
    is WorkspaceUsageRequestBody -> WorkspaceOperationInput.Saved(listOf(body.workspaceId))
    is WorkspaceDataWorkerAvailabilityRequestBody ->
      WorkspaceOperationInput.Saved(
        listOf(body.workspaceId),
        organization = Optional.of(body.organizationId),
      )
    is WorkspaceUpdate -> WorkspaceOperationInput.Saved(listOf(body.workspaceId), dataplaneGroup = Optional.ofNullable(body.dataplaneGroupId))
    is WorkspaceUpdateName -> WorkspaceOperationInput.Saved(listOf(body.workspaceId))
    is WorkspaceGiveFeedback -> WorkspaceOperationInput.Saved(listOf(body.workspaceId))
    is WorkspaceGetDbtJobsRequest -> WorkspaceOperationInput.Saved(listOf(body.workspaceId), dbtConfig = Optional.of(body.dbtConfigId))
    is ListResourcesForWorkspacesRequestBody -> WorkspaceOperationInput.Saved(body.workspaceIds.toList(), body.includeDeleted ?: false)
    is SlugRequestBody -> WorkspaceOperationInput.Slug(body.slug)
    is ConnectionIdRequestBody -> WorkspaceOperationInput.Connection(body.connectionId)
    is WorkspaceCreate -> WorkspaceOperationInput.Create(body.organizationId, Optional.empty(), Optional.ofNullable(body.dataplaneGroupId))
    is WorkspaceCreateWithId ->
      WorkspaceOperationInput.Create(
        body.organizationId ?: throw BadRequestProblem(),
        Optional.of(body.id),
        Optional.ofNullable(body.dataplaneGroupId),
      )
    is WorkspaceUpdateOrganization -> WorkspaceOperationInput.Transfer(body.workspaceId, body.organizationId)
    is ListWorkspacesInOrganizationRequestBody -> WorkspaceOperationInput.FilteredOrganization(body.organizationId)
    is ListWorkspacesByUserRequestBody -> WorkspaceOperationInput.User(body.userId)
    is String ->
      WorkspaceOperationInput.Saved(
        listOf(
          try {
            UUID.fromString(body)
          } catch (_: IllegalArgumentException) {
            throw BadRequestProblem()
          },
        ),
      )
    else -> throw BadRequestProblem()
  }

abstract class WorkspaceScopedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val callers: Set<OperationCaller> = organizationCallers,
  private val deletedConnectionWorkspace: Boolean = false,
  private val honorDeleted: Boolean = false,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WorkspaceOperationInput = workspaceInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val owners =
      when (val request = input as WorkspaceOperationInput) {
        is WorkspaceOperationInput.Saved -> request.ids.distinct().map { data.savedWorkspaceAndOrganization(it, honorDeleted && request.deleted) }
        is WorkspaceOperationInput.Slug -> listOf(data.workspaceBySlug(request.slug))
        is WorkspaceOperationInput.Connection -> {
          listOf(data.connectionSourceWorkspaceAndOrganization(request.id, deletedConnectionWorkspace))
        }
        else -> throw BadRequestProblem()
      }
    if (owners.isEmpty()) permissions.requireGlobal(caller, roles, callers) else permissions.requireWorkspaces(caller, owners, roles, callers)
    if (input is WorkspaceOperationInput.Saved) {
      input.organization.ifPresent { organization -> if (owners.any { it.organizationId != organization }) throw BadRequestProblem() }
      input.dataplaneGroup.ifPresent { group ->
        owners.forEach { owner ->
          permissions.requireOrganizations(caller, listOf(owner.organizationId), setOf(AuthRoleConstants.ORGANIZATION_EDITOR), callers)
          if (!data.canUseDataplaneGroup(group, owner.organizationId)) throw ForbiddenProblem()
        }
      }
      input.dbtConfig.ifPresent { config -> owners.forEach { data.requireWorkspaceDbtConfig(it.workspaceId, config) } }
    }
  }
}

@Singleton
class WorkspaceReadRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceScopedRule(data, permissions, workspaceReadRoles)

data class WorkspaceUnsupportedInput(
  val workspace: Optional<UUID>,
) : OperationInput

/** OSS uses the workspace only for its permission check; Cloud consumes the other selectors. */
abstract class WorkspaceUnsupportedRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    WorkspaceUnsupportedInput(
      when (val body = args.singleOrNull()) {
        is WorkspaceUsageRequestBody -> Optional.of(body.workspaceId)
        is WorkspaceDataWorkerAvailabilityRequestBody -> Optional.of(body.workspaceId)
        is WorkspaceGetDbtJobsRequest -> Optional.of(body.workspaceId)
        null -> Optional.empty()
        else -> throw BadRequestProblem()
      },
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    try {
      // Instance administrators and no-auth reach the static response without tenant resolution.
      permissions.requireGlobal(caller, roles, organizationCallers)
      return
    } catch (_: ForbiddenProblem) {
      // A workspace-scoped caller still needs the original action on that workspace.
    }
    val workspace = (input as WorkspaceUnsupportedInput).workspace.orElseThrow { ForbiddenProblem() }
    val owner = data.savedWorkspaceAndOrganization(workspace, false)
    permissions.requireWorkspaces(caller, listOf(owner), roles, organizationCallers)
  }
}

@Singleton
class WorkspaceReadUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceUnsupportedRule(data, permissions, workspaceReadRoles)

@Singleton
class WorkspaceWriteUnsupportedRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceUnsupportedRule(data, permissions, connectionWriteRoles)

@Singleton
class WorkspaceGetRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceScopedRule(
    data,
    permissions,
    workspaceReadRoles + setOf(AuthRoleConstants.DATAPLANE, AuthRoleConstants.EMBEDDED_END_USER),
    organizationCallers + OperationCaller.EMBEDDED,
    honorDeleted = true,
  )

@Singleton
class WorkspaceListRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceScopedRule(data, permissions, workspaceReadRoles, honorDeleted = true)

@Singleton
class WorkspaceTombstoneConnectionRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceScopedRule(data, permissions, workspaceReadRoles, deletedConnectionWorkspace = true)

@Singleton
class WorkspaceWriteRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceScopedRule(
    data,
    permissions,
    setOf(
      AuthRoleConstants.WORKSPACE_EDITOR,
      AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
      AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
      AuthRoleConstants.ORGANIZATION_EDITOR,
    ),
  )

@Singleton
class WorkspaceAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : WorkspaceScopedRule(data, permissions, setOf(AuthRoleConstants.WORKSPACE_ADMIN, AuthRoleConstants.ORGANIZATION_ADMIN))

@Singleton
class WorkspaceCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WorkspaceOperationInput = workspaceInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WorkspaceOperationInput.Create
    val organization = data.organization(request.organization)
    permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
    request.id.ifPresent { id ->
      data.existingWorkspaceAndOrganization(id).ifPresent { saved ->
        permissions.requireOrganizations(caller, listOf(saved.organizationId), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
      }
    }
    request.dataplaneGroup.ifPresent { if (!data.canUseDataplaneGroup(it, organization)) throw ForbiddenProblem() }
  }
}

@Singleton
class WorkspaceTransferRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WorkspaceOperationInput = workspaceInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as WorkspaceOperationInput.Transfer
    data.workspaceAndOrganization(request.id)
    data.organization(request.organization)
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
  }
}

@Singleton
class WorkspaceFilteredOrganizationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WorkspaceOperationInput = workspaceInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    data.organization((input as WorkspaceOperationInput.FilteredOrganization).id)
    permissions.requireAuthenticated(caller, setOf(OperationCaller.USER))
  }
}

@Singleton
class WorkspaceUserListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): WorkspaceOperationInput = workspaceInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    try {
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
      return
    } catch (_: ForbiddenProblem) {
      // A filtered list needs a saved subject only for its SELF alternative.
    }
    permissions.requireSelfOrInstanceAdmin(caller, data.userAuthSubjects((input as WorkspaceOperationInput.User).id), organizationCallers)
  }
}
