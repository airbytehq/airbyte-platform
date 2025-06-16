/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.roles

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AuthRoleTest {
  @Test
  fun `OWNER auth role set contains correct roles`() {
    val ownerResult = AuthRole.buildAuthRolesSet(AuthRole.OWNER)
    assertEquals(5, ownerResult.size)
    assertEquals(
      setOf(
        AuthRole.OWNER.getLabel(),
        AuthRole.ADMIN.getLabel(),
        AuthRole.EDITOR.getLabel(),
        AuthRole.READER.getLabel(),
        AuthRole.AUTHENTICATED_USER.getLabel(),
      ),
      ownerResult,
    )
  }

  @Test
  fun `ADMIN auth role set contains correct roles`() {
    val adminResult = AuthRole.buildAuthRolesSet(AuthRole.ADMIN)
    assertEquals(4, adminResult.size)
    assertEquals(
      setOf(
        AuthRole.ADMIN.getLabel(),
        AuthRole.EDITOR.getLabel(),
        AuthRole.READER.getLabel(),
        AuthRole.AUTHENTICATED_USER.getLabel(),
      ),
      adminResult,
    )
  }

  @Test
  fun `EDITOR auth role set contains correct roles`() {
    val editorResult = AuthRole.buildAuthRolesSet(AuthRole.EDITOR)
    assertEquals(3, editorResult.size)
    assertEquals(
      setOf(
        AuthRole.EDITOR.getLabel(),
        AuthRole.READER.getLabel(),
        AuthRole.AUTHENTICATED_USER.getLabel(),
      ),
      editorResult,
    )
  }

  @Test
  fun `READER auth role set contains correct roles`() {
    val readerResult = AuthRole.buildAuthRolesSet(AuthRole.READER)
    assertEquals(2, readerResult.size)
    assertEquals(
      setOf(
        AuthRole.READER.getLabel(),
        AuthRole.AUTHENTICATED_USER.getLabel(),
      ),
      readerResult,
    )
  }

  @Test
  fun `AUTHENTICATED_USER role set contains correct roles`() {
    val authenticatedUserResult = AuthRole.buildAuthRolesSet(AuthRole.AUTHENTICATED_USER)
    assertEquals(1, authenticatedUserResult.size)
    assertEquals(
      setOf(
        AuthRole.AUTHENTICATED_USER.getLabel(),
      ),
      authenticatedUserResult,
    )
  }

  @Test
  fun `NONE auth role only contains NONE in the set`() {
    val noneResult = AuthRole.buildAuthRolesSet(AuthRole.NONE)
    assertEquals(1, noneResult.size)
    assertEquals(
      setOf(
        AuthRole.NONE.getLabel(),
      ),
      noneResult,
    )
  }

  @Test
  fun `null contains no roles in te set`() {
    val nullResult = AuthRole.buildAuthRolesSet(null)
    assertEquals(0, nullResult.size)
  }
}
