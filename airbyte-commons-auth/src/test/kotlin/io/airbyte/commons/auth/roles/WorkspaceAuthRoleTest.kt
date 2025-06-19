/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WorkspaceAuthRoleTest {
  @Test
  fun `buildWorkspaceAuthRolesSet returns all lower roles for WORKSPACE_ADMIN`() {
    val result = WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN)
    assertEquals(4, result.size)
    assertEquals(
      linkedSetOf(
        WorkspaceAuthRole.WORKSPACE_ADMIN.getLabel(),
        WorkspaceAuthRole.WORKSPACE_EDITOR.getLabel(),
        WorkspaceAuthRole.WORKSPACE_RUNNER.getLabel(),
        WorkspaceAuthRole.WORKSPACE_READER.getLabel(),
      ),
      result,
    )
  }

  @Test
  fun `buildWorkspaceAuthRolesSet returns expected roles for WORKSPACE_EDITOR`() {
    val result = WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_EDITOR)
    assertEquals(3, result.size)
    assertEquals(
      linkedSetOf(
        WorkspaceAuthRole.WORKSPACE_EDITOR.getLabel(),
        WorkspaceAuthRole.WORKSPACE_RUNNER.getLabel(),
        WorkspaceAuthRole.WORKSPACE_READER.getLabel(),
      ),
      result,
    )
  }

  @Test
  fun `buildWorkspaceAuthRolesSet returns expected roles for WORKSPACE_RUNNER`() {
    val result = WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_RUNNER)
    assertEquals(2, result.size)
    assertEquals(
      linkedSetOf(
        WorkspaceAuthRole.WORKSPACE_RUNNER.getLabel(),
        WorkspaceAuthRole.WORKSPACE_READER.getLabel(),
      ),
      result,
    )
  }

  @Test
  fun `buildWorkspaceAuthRolesSet returns only itself for WORKSPACE_READER`() {
    val result = WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_READER)
    assertEquals(1, result.size)
    assertEquals(
      setOf(WorkspaceAuthRole.WORKSPACE_READER.getLabel()),
      result,
    )
  }

  @Test
  fun `buildWorkspaceAuthRolesSet returns only NONE label for NONE`() {
    val result = WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.NONE)
    assertEquals(1, result.size)
    assertEquals(setOf(WorkspaceAuthRole.NONE.getLabel()), result)
  }

  @Test
  fun `buildWorkspaceAuthRolesSet returns empty set when input is null`() {
    val result = WorkspaceAuthRole.buildWorkspaceAuthRolesSet(null)
    assertTrue(result.isEmpty())
  }
}
