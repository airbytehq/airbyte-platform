/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.Group
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class GroupPermissionValidator(
  private val workspaceService: WorkspaceService,
  private val organizationService: OrganizationService,
  private val permissionService: PermissionService,
  private val roleResolver: RoleResolver,
) {
  fun validateScope(
    group: Group,
    permissionType: Permission.PermissionType,
    workspaceId: UUID?,
    organizationId: UUID?,
  ) {
    // Reject dual scope up front: the branches below validate exactly one scope, so a request
    // carrying both would persist an organizationId no branch ever inspected (cross-tenant write).
    if (workspaceId != null && organizationId != null) {
      throw BadRequestProblem(
        ProblemMessageData().message("Provide exactly one of workspaceId or organizationId, not both."),
      )
    }

    // Validate that the user has access to the workspace/organization they're trying to grant permissions for
    if (workspaceId != null) {
      // The permission type must match the scope it is granted on: an organization-typed row
      // scoped to a workspace (or vice versa) is an incoherent RBAC row no constraint rejects.
      // INSTANCE_ADMIN and DATAPLANE match neither scope and are rejected on both branches.
      if (!isWorkspaceScopedType(permissionType)) {
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Permission type $permissionType cannot be granted on a workspace. Use a workspace-scoped permission type.",
          ),
        )
      }

      // Validate the workspace exists
      val workspace =
        try {
          workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false)
        } catch (_: ConfigNotFoundException) {
          throw ResourceNotFoundProblem(
            ProblemResourceData()
              .resourceType("workspace")
              .resourceId(workspaceId.toString()),
          )
        }

      // Prevent cross-organization scope escalation. Throw the same ResourceNotFoundProblem as the
      // absent-workspace case above so a workspace in a foreign organization is indistinguishable
      // from one that does not exist at all — otherwise the response shape (400 vs 404) lets a
      // caller probe for the existence of workspaces in organizations they cannot access, and a
      // message naming the foreign organization would leak that organization's id.
      if (workspace.organizationId != group.organizationId.value) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("workspace")
            .resourceId(workspaceId.toString()),
        )
      }

      // User must have workspace_admin or higher role for the target workspace
      roleResolver
        .newRequest()
        .withCurrentUser()
        .withRef(AuthenticationId.WORKSPACE_ID, workspaceId.toString())
        .requireRole(AuthRoleConstants.WORKSPACE_ADMIN)
    } else if (organizationId != null) {
      if (!isOrganizationScopedType(permissionType)) {
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Permission type $permissionType cannot be granted on an organization. Use an organization-scoped permission type.",
          ),
        )
      }

      // Prevent cross-organization scope escalation. Check this BEFORE looking the organization up:
      // a foreign organization that exists must be indistinguishable from one that does not, using
      // the same ResourceNotFoundProblem shape (404) in both cases. Looking the organization up first
      // would let a caller distinguish "exists but is not yours" (400) from "does not exist" (404),
      // effectively an existence oracle for other tenants' organizations. The message below repeats
      // only the id the caller supplied, never anything about the group's own organization.
      if (organizationId != group.organizationId.value) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("organization")
            .resourceId(organizationId.toString()),
        )
      }

      // Validate the organization exists
      val organization = organizationService.getOrganization(organizationId)
      if (organization.isEmpty) {
        throw ResourceNotFoundProblem(
          ProblemResourceData()
            .resourceType("organization")
            .resourceId(organizationId.toString()),
        )
      }

      // User must have organization_admin or higher role for the target organization
      roleResolver
        .newRequest()
        .withCurrentUser()
        .withRef(AuthenticationId.ORGANIZATION_ID, organizationId.toString())
        .requireRole(AuthRoleConstants.ORGANIZATION_ADMIN)
    } else {
      throw BadRequestProblem(
        ProblemMessageData().message("Workspace ID or Organization ID must be provided in order to create a group permission."),
      )
    }
  }

  fun isDuplicate(
    groupId: UUID,
    permissionType: Permission.PermissionType,
    workspaceId: UUID?,
    organizationId: UUID?,
  ): Boolean =
    when {
      workspaceId != null -> permissionService.groupPermissionExistsForWorkspace(groupId, permissionType, workspaceId)
      organizationId != null -> permissionService.groupPermissionExistsForOrganization(groupId, permissionType, organizationId)
      else -> false
    }

  // Exhaustive over every Permission.PermissionType constant with no `else` branch, so a future
  // enum constant fails to compile here instead of silently falling through to "not workspace-scoped".
  private fun isWorkspaceScopedType(permissionType: Permission.PermissionType): Boolean =
    when (permissionType) {
      Permission.PermissionType.WORKSPACE_OWNER,
      Permission.PermissionType.WORKSPACE_ADMIN,
      Permission.PermissionType.WORKSPACE_EDITOR,
      Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
      Permission.PermissionType.WORKSPACE_RUNNER,
      Permission.PermissionType.WORKSPACE_READER,
      -> true
      Permission.PermissionType.ORGANIZATION_ADMIN,
      Permission.PermissionType.ORGANIZATION_EDITOR,
      Permission.PermissionType.ORGANIZATION_RUNNER,
      Permission.PermissionType.ORGANIZATION_READER,
      Permission.PermissionType.ORGANIZATION_MEMBER,
      Permission.PermissionType.INSTANCE_ADMIN,
      Permission.PermissionType.DATAPLANE,
      -> false
    }

  // Exhaustive over every Permission.PermissionType constant with no `else` branch, so a future
  // enum constant fails to compile here instead of silently falling through to "not organization-scoped".
  private fun isOrganizationScopedType(permissionType: Permission.PermissionType): Boolean =
    when (permissionType) {
      Permission.PermissionType.ORGANIZATION_ADMIN,
      Permission.PermissionType.ORGANIZATION_EDITOR,
      Permission.PermissionType.ORGANIZATION_RUNNER,
      Permission.PermissionType.ORGANIZATION_READER,
      Permission.PermissionType.ORGANIZATION_MEMBER,
      -> true
      Permission.PermissionType.WORKSPACE_OWNER,
      Permission.PermissionType.WORKSPACE_ADMIN,
      Permission.PermissionType.WORKSPACE_EDITOR,
      Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
      Permission.PermissionType.WORKSPACE_RUNNER,
      Permission.PermissionType.WORKSPACE_READER,
      Permission.PermissionType.INSTANCE_ADMIN,
      Permission.PermissionType.DATAPLANE,
      -> false
    }
}
