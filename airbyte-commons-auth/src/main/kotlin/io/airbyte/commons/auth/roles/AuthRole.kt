/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

enum class AuthRole(
  private val authority: Int,
  private val label: String,
) : AuthRoleInterface {
  OWNER(500, AuthRoleConstants.OWNER),
  ADMIN(400, AuthRoleConstants.ADMIN),
  EDITOR(300, AuthRoleConstants.EDITOR),
  READER(200, AuthRoleConstants.READER),
  AUTHENTICATED_USER(100, AuthRoleConstants.AUTHENTICATED_USER), // ONLY USE WITH INSTANCE RESOURCE!
  NONE(0, AuthRoleConstants.NONE),
  ;

  override fun getAuthority(): Int = authority

  override fun getLabel(): String = label

  companion object {
    /**
     * Builds the set of roles based on the provided [AuthRole] value.
     *
     * The generated set of auth roles contains the provided [AuthRole] (if not `null`) and
     * any other authentication roles with a lesser [getAuthority] value.
     *
     * @param authRole An [AuthRole] (may be `null`).
     * @return The set of [AuthRole] labels based on the provided [AuthRole].
     */
    @JvmStatic
    fun buildAuthRolesSet(authRole: AuthRole?): Set<String> {
      val authRoles = mutableSetOf<AuthRole>()

      if (authRole != null) {
        authRoles.add(authRole)
        authRoles.addAll(
          entries
            .filter { it != NONE && it.getAuthority() < authRole.getAuthority() },
        )
      }

      return authRoles
        .sortedBy { it.getAuthority() }
        .map { it.getLabel() }
        .toCollection(LinkedHashSet())
    }

    /**
     * Get all possible RBAC roles for instance admin users that should have global access.
     */
    @JvmStatic
    fun getInstanceAdminRoles(): Set<String> {
      val roles = buildAuthRolesSet(ADMIN).toMutableSet()
      roles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN))
      roles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN))
      roles.add(AuthRoleConstants.DATAPLANE)
      return roles
    }
  }
}
