/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

enum class OrganizationAuthRole(
  private val authority: Int,
  private val label: String,
) : AuthRoleInterface {
  ORGANIZATION_ADMIN(500, AuthRoleConstants.ORGANIZATION_ADMIN),
  ORGANIZATION_EDITOR(400, AuthRoleConstants.ORGANIZATION_EDITOR),
  ORGANIZATION_RUNNER(300, AuthRoleConstants.ORGANIZATION_RUNNER),
  ORGANIZATION_READER(200, AuthRoleConstants.ORGANIZATION_READER),
  ORGANIZATION_MEMBER(100, AuthRoleConstants.ORGANIZATION_MEMBER),
  NONE(0, AuthRoleConstants.NONE),
  ;

  override fun getAuthority(): Int = authority

  override fun getLabel(): String = label

  companion object {
    /**
     * Builds the set of roles based on the provided [OrganizationAuthRole] value.
     *
     * The generated set of auth roles contains the provided [OrganizationAuthRole] (if not `null`)
     * and any other authentication roles with a lesser [getAuthority] value.
     *
     * @param authRole An [OrganizationAuthRole] (may be `null`).
     * @return The set of [OrganizationAuthRole] labels based on the provided role.
     */
    @JvmStatic
    fun buildOrganizationAuthRolesSet(authRole: OrganizationAuthRole?): Set<String> {
      val authRoles = mutableSetOf<OrganizationAuthRole>()

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
  }
}
