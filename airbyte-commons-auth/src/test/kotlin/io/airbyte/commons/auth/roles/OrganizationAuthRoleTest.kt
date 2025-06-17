/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class OrganizationAuthRoleTest {
  @Test
  fun `buildOrganizationAuthRolesSet returns all lower roles for ORGANIZATION_ADMIN`() {
    val result = OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN)
    assertEquals(5, result.size)
    assertEquals(
      linkedSetOf(
        OrganizationAuthRole.ORGANIZATION_ADMIN.getLabel(),
        OrganizationAuthRole.ORGANIZATION_EDITOR.getLabel(),
        OrganizationAuthRole.ORGANIZATION_RUNNER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_READER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_MEMBER.getLabel(),
      ),
      result,
    )
  }

  @Test
  fun `buildOrganizationAuthRolesSet returns appropriate roles for ORGANIZATION_EDITOR`() {
    val result = OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_EDITOR)
    assertEquals(4, result.size)
    assertEquals(
      linkedSetOf(
        OrganizationAuthRole.ORGANIZATION_EDITOR.getLabel(),
        OrganizationAuthRole.ORGANIZATION_RUNNER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_READER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_MEMBER.getLabel(),
      ),
      result,
    )
  }

  @Test
  fun `buildOrganizationAuthRolesSet returns only itself for ORGANIZATION_MEMBER`() {
    val result = OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_MEMBER)
    assertEquals(1, result.size)
    assertEquals(setOf(OrganizationAuthRole.ORGANIZATION_MEMBER.getLabel()), result)
  }

  @Test
  fun `buildOrganizationAuthRolesSet returns only NONE label for NONE`() {
    val result = OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.NONE)
    assertEquals(1, result.size)
    assertEquals(setOf(OrganizationAuthRole.NONE.getLabel()), result)
  }

  @Test
  fun `buildOrganizationAuthRolesSet returns empty set for null input`() {
    val result = OrganizationAuthRole.buildOrganizationAuthRolesSet(null)
    assertTrue(result.isEmpty())
  }
}
