/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

enum class WorkspaceAuthRole(
  private val authority: Int,
  private val label: String,
) : AuthRoleInterface {
  WORKSPACE_ADMIN(500, AuthRoleConstants.WORKSPACE_ADMIN),
  WORKSPACE_EDITOR(400, AuthRoleConstants.WORKSPACE_EDITOR),
  WORKSPACE_RUNNER(300, AuthRoleConstants.WORKSPACE_RUNNER),
  WORKSPACE_READER(200, AuthRoleConstants.WORKSPACE_READER),
  NONE(0, AuthRoleConstants.NONE),
  ;

  override fun getAuthority(): Int = authority

  override fun getLabel(): String = label

  companion object {
    /**
     * Builds the set of roles based on the provided [WorkspaceAuthRole] value.
     *
     * The generated set of auth roles contains the provided [WorkspaceAuthRole] (if not `null`)
     * and any other authentication roles with a lesser [getAuthority] value.
     *
     * @param authRole A [WorkspaceAuthRole] (may be `null`).
     * @return A set of [WorkspaceAuthRole] labels based on the provided role.
     */
    @JvmStatic
    fun buildWorkspaceAuthRolesSet(authRole: WorkspaceAuthRole?): Set<String> {
      val authRoles = mutableSetOf<WorkspaceAuthRole>()

      if (authRole != null) {
        authRoles.add(authRole)
        authRoles.addAll(
          entries.filter { it != NONE && it.getAuthority() < authRole.getAuthority() },
        )
      }

      return authRoles
        .sortedBy { it.getAuthority() }
        .map { it.getLabel() }
        .toCollection(LinkedHashSet())
    }
  }
}
