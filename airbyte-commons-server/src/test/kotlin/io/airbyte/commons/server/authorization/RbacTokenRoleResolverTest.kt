package io.airbyte.commons.server.authorization

import io.airbyte.commons.auth.AuthRole
import io.airbyte.commons.server.support.RbacRoleHelper
import io.micronaut.http.HttpRequest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class RbacTokenRoleResolverTest {
  private lateinit var rbacRoleHelper: RbacRoleHelper
  private lateinit var rbacTokenRoleResolver: RbacTokenRoleResolver

  @BeforeEach
  fun setup() {
    rbacRoleHelper = mockk()
    rbacTokenRoleResolver = RbacTokenRoleResolver(rbacRoleHelper)
  }

  @Test
  fun `test resolveRoles with null authUserId`() {
    val roles = rbacTokenRoleResolver.resolveRoles(null, HttpRequest.GET<Any>("/"))
    assertEquals(setOf<String>(), roles)
  }

  @Test
  fun `test resolveRoles with blank authUserId`() {
    val roles = rbacTokenRoleResolver.resolveRoles("", HttpRequest.GET<Any>("/"))
    assertEquals(setOf<String>(), roles)
  }

  @Test
  fun `test resolveRoles with valid authUserId`() {
    val authUserId = "test-user"
    val expectedRoles = setOf("ORGANIZATION_ADMIN", "WORKSPACE_EDITOR")
    every { rbacRoleHelper.getRbacRoles(authUserId, any(HttpRequest::class)) } returns expectedRoles

    val roles = rbacTokenRoleResolver.resolveRoles(authUserId, HttpRequest.GET<Any>("/"))
    assertEquals(setOf(AuthRole.AUTHENTICATED_USER.name).plus(expectedRoles), roles)
  }

  @Test
  fun `test resolveRoles with exception`() {
    val authUserId = "test-user"
    every { rbacRoleHelper.getRbacRoles(authUserId, any(HttpRequest::class)) } throws RuntimeException("Failed to resolve roles")

    val roles = rbacTokenRoleResolver.resolveRoles(authUserId, HttpRequest.GET<Any>("/"))
    assertEquals(setOf(AuthRole.AUTHENTICATED_USER.name), roles)
  }
}
