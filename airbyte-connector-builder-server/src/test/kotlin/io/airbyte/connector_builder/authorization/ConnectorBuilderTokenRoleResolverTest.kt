@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.authorization

import io.airbyte.commons.auth.AuthRole
import io.micronaut.http.HttpRequest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class ConnectorBuilderTokenRoleResolverTest {
  private lateinit var resolver: ConnectorBuilderTokenRoleResolver

  @BeforeEach
  fun setup() {
    resolver = ConnectorBuilderTokenRoleResolver()
  }

  @Test
  fun `test resolveRoles with null authUserId`() {
    val roles = resolver.resolveRoles(null, HttpRequest.GET<Any>("/"))
    assertEquals(setOf<String>(), roles)
  }

  @Test
  fun `test resolveRoles with blank authUserId`() {
    val roles = resolver.resolveRoles("", HttpRequest.GET<Any>("/"))
    assertEquals(setOf<String>(), roles)
  }

  @Test
  fun `test resolveRoles with valid authUserId`() {
    val authUserId = "test-user"
    val roles = resolver.resolveRoles(authUserId, HttpRequest.GET<Any>("/"))
    assertEquals(setOf(AuthRole.AUTHENTICATED_USER.name), roles)
  }
}
