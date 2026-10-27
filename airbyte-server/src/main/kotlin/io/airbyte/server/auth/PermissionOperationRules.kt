/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.PermissionCheckRequest
import io.airbyte.api.server.generated.models.PermissionCreate
import io.airbyte.api.server.generated.models.PermissionDeleteUserFromWorkspaceRequestBody
import io.airbyte.api.server.generated.models.PermissionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionUpdate
import io.airbyte.api.server.generated.models.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.api.server.generated.models.UserIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.errors.OperationNotAllowedException
import io.airbyte.config.Permission
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.MembershipAuthorizationDataService
import io.airbyte.domain.services.auth.SavedAuthorizationPermission
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface PermissionOperationInput : OperationInput {
  data class Saved(
    val id: UUID,
    val updateType: Optional<Permission.PermissionType> = Optional.empty(),
  ) : PermissionOperationInput

  data class Create(
    val user: UUID,
    val workspace: Optional<UUID>,
    val organization: Optional<UUID>,
    val type: Permission.PermissionType,
  ) : PermissionOperationInput

  data class Leave(
    val user: UUID,
    val workspace: UUID,
  ) : PermissionOperationInput

  data class User(
    val id: UUID,
  ) : PermissionOperationInput

  data class Diagnostic(
    val user: UUID,
    val workspaces: List<Optional<UUID>>,
    val organization: Optional<UUID>,
    val question: Permission.PermissionType,
  ) : PermissionOperationInput
}

private fun permissionInput(args: Array<out Any?>): PermissionOperationInput =
  when (val body = args.singleOrNull()) {
    is PermissionIdRequestBody -> PermissionOperationInput.Saved(body.permissionId)
    is PermissionUpdate ->
      PermissionOperationInput.Saved(
        body.permissionId,
        Optional.ofNullable(
          body.permissionType?.let {
            Permission.PermissionType.valueOf(it.name)
          },
        ),
      )
    is PermissionCreate ->
      PermissionOperationInput.Create(
        body.userId,
        Optional.ofNullable(body.workspaceId),
        Optional.ofNullable(body.organizationId),
        Permission.PermissionType.valueOf(body.permissionType.name),
      )
    is PermissionDeleteUserFromWorkspaceRequestBody -> PermissionOperationInput.Leave(body.userId, body.workspaceId)
    is UserIdRequestBody -> PermissionOperationInput.User(body.userId)
    is PermissionCheckRequest ->
      PermissionOperationInput.Diagnostic(
        body.userId,
        listOfNotNull(body.workspaceId).map {
          Optional.of(it)
        },
        Optional.ofNullable(body.organizationId),
        Permission.PermissionType.valueOf(body.permissionType.name),
      )
    is PermissionsCheckMultipleWorkspacesRequest ->
      PermissionOperationInput.Diagnostic(
        body.userId,
        (
          body.workspaceIds
            ?: throw NullPointerException("workspaceIds")
        ).map { id: UUID? -> Optional.ofNullable(id) },
        Optional.empty(),
        Permission.PermissionType.valueOf(body.permissionType.name),
      )
    else -> throw BadRequestProblem()
  }

internal val permissionAdminRoles = setOf(AuthRoleConstants.WORKSPACE_ADMIN, AuthRoleConstants.ORGANIZATION_ADMIN)

internal fun requireAuthorizationTarget(
  permissions: OperationPermissions,
  caller: Authentication,
  target: AuthorizationTarget,
  roles: Set<String>,
  callers: Set<OperationCaller> = organizationCallers,
) {
  when (target) {
    is AuthorizationTarget.Workspace -> permissions.requireWorkspaces(caller, listOf(target.workspaceAndOrganization), roles, callers)
    is AuthorizationTarget.Organization -> permissions.requireOrganizations(caller, listOf(target.organizationId), roles, callers)
    AuthorizationTarget.Global -> permissions.requireGlobal(caller, roles, callers)
  }
}

@Singleton
class PermissionCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): PermissionOperationInput = permissionInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as PermissionOperationInput.Create
    if (request.workspace.isPresent ==
      request.organization.isPresent
    ) {
      throw BadObjectSchemaKnownException("Provide exactly one workspaceId or organizationId.")
    }
    val target =
      if (request.workspace.isPresent) {
        AuthorizationTarget.Workspace(
          data.workspaceAndOrganization(request.workspace.orElseThrow()),
        )
      } else {
        AuthorizationTarget.Organization(data.organization(request.organization.orElseThrow()))
      }
    requireAuthorizationTarget(permissions, caller, target, permissionAdminRoles)
    if (request.type == Permission.PermissionType.INSTANCE_ADMIN ||
      request.type == Permission.PermissionType.DATAPLANE
    ) {
      throw BadObjectSchemaKnownException("Instance and dataplane permissions cannot be created via API.")
    }
    data.requireUser(request.user)
  }
}

abstract class SavedUserPermissionRule(
  protected val data: AuthorizationDataService,
  private val membership: MembershipAuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
  private val forbiddenAction: String,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): PermissionOperationInput = permissionInput(args)

  protected fun requireSavedPermission(
    caller: Authentication,
    input: PermissionOperationInput.Saved,
  ): SavedAuthorizationPermission {
    val saved = membership.permission(input.id)
    if (saved.groupId.isPresent) {
      throw OperationNotAllowedException(
        "Permission is inherited from a group and cannot be $forbiddenAction as a user permission.",
      )
    }
    requireAuthorizationTarget(permissions, caller, saved.target, roles)
    return saved
  }
}

@Singleton
class PermissionReadRule(
  data: AuthorizationDataService,
  membership: MembershipAuthorizationDataService,
  permissions: OperationPermissions,
) : SavedUserPermissionRule(data, membership, permissions, workspaceReadRoles, "read") {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireSavedPermission(caller, input as PermissionOperationInput.Saved)
  }
}

@Singleton
class PermissionUpdateRule(
  data: AuthorizationDataService,
  membership: MembershipAuthorizationDataService,
  permissions: OperationPermissions,
) : SavedUserPermissionRule(data, membership, permissions, permissionAdminRoles, "updated") {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as PermissionOperationInput.Saved
    val saved = requireSavedPermission(caller, request)
    val type = request.updateType.orElseThrow { NullPointerException("permissionType") }
    if (type == Permission.PermissionType.INSTANCE_ADMIN) throw BadObjectSchemaKnownException("Cannot modify Instance Admin permissions via API.")
    data.requireUser(saved.userId.orElseThrow { NullPointerException("userId") })
  }
}

@Singleton
class PermissionDeleteRule(
  data: AuthorizationDataService,
  membership: MembershipAuthorizationDataService,
  permissions: OperationPermissions,
) : SavedUserPermissionRule(data, membership, permissions, permissionAdminRoles, "deleted") {
  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val saved = requireSavedPermission(caller, input as PermissionOperationInput.Saved)
    data.requireUser(saved.userId.orElseThrow { AuthorizationResourceNotFoundException() })
  }
}

@Singleton
class PermissionLeaveWorkspaceRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): PermissionOperationInput = permissionInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as PermissionOperationInput.Leave
    val owner = data.workspaceAndOrganization(request.workspace)
    data.requireUser(request.user)
    try {
      permissions.requireWorkspaces(caller, listOf(owner), permissionAdminRoles, organizationCallers)
    } catch (_: ForbiddenProblem) {
      permissions.requireSelfOrInstanceAdmin(caller, data.userAuthSubjects(request.user), organizationCallers)
    }
  }
}

@Singleton
class PermissionUserListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): PermissionOperationInput = permissionInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    try {
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
      return
    } catch (_: ForbiddenProblem) {
      // The effective-permission query can return empty for an admin's unknown user filter.
    }
    permissions.requireSelfOrInstanceAdmin(caller, data.userAuthSubjects((input as PermissionOperationInput.User).id), organizationCallers)
  }
}

@Singleton
class PermissionDiagnosticRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): PermissionOperationInput = permissionInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    // IDs and the requested role are the diagnostic question, not extra caller grants or required existing records.
    permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)
  }
}
