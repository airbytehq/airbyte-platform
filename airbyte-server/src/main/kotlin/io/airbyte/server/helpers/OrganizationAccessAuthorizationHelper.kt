/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.persistence.WorkspacePersistence
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

/**
 * Helper class for performing organization access authorization checks.
 *
 * This helper implements the pattern where users can access organization resources if they have:
 * 1. Organization-level membership (ORGANIZATION_MEMBER role), OR
 * 2. Access to at least one workspace within the organization
 */
@Singleton
class OrganizationAccessAuthorizationHelper(
  private val roleResolver: RoleResolver,
  private val workspacePersistence: WorkspacePersistence,
  private val currentUserService: CurrentUserService,
) {
  /**
   * Validates that the current user has access to the specified organization.
   *
   * Access is granted if the user has:
   * 1. Organization-level membership (ORGANIZATION_MEMBER role), OR
   * 2. Access to at least one workspace within the organization
   *
   * @param organizationId the UUID of the organization to validate access for
   * @throws ForbiddenProblem if the user does not have access to the organization
   */
  fun validateOrganizationOrWorkspaceAccess(organizationId: UUID) {
    // First try organization-level permissions
    val orgAuth =
      roleResolver
        .newRequest()
        .withCurrentAuthentication()
        .withOrg(organizationId)

    try {
      orgAuth.requireRole(AuthRoleConstants.ORGANIZATION_MEMBER)
      // User has org-level access, proceed
      return
    } catch (e: ForbiddenProblem) {
      // No org-level access, check workspace-level access
      val currentUser = currentUserService.getCurrentUser()
      val accessibleWorkspaces =
        workspacePersistence.listWorkspacesInOrganizationByUserId(
          organizationId,
          currentUser.userId,
          Optional.empty(),
        )

      if (accessibleWorkspaces.isEmpty()) {
        // User has no access to any workspace in the organization, re-throw the original exception
        throw e
      }
      // User has access to at least one workspace in the organization, allow access
    }
  }
}
