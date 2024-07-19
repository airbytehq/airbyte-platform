package io.airbyte.server.config.community.auth

import io.airbyte.commons.server.support.RbacRoleHelper
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.config.InstanceAdminConfig
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.UsernamePasswordCredentials
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@MicronautTest
class CommunityAuthProviderTest {
  private val instanceAdminConfig = mockk<InstanceAdminConfig>()
  private val authProvider = CommunityAuthProvider<Any>(instanceAdminConfig)

  @Test
  fun `should authenticate successfully with valid credentials`() {
    val username = "admin"
    val password = "password"
    every { instanceAdminConfig.username } returns username
    every { instanceAdminConfig.password } returns password

    val authRequest = UsernamePasswordCredentials(username, password)
    val response = authProvider.authenticate(HttpRequest.GET("/"), authRequest)!!

    assertTrue(response.isAuthenticated)
    assertEquals(UserPersistence.DEFAULT_USER_ID.toString(), response.authentication.get().name)
    assertTrue(
      response.authentication
        .get()
        .roles
        .containsAll(RbacRoleHelper.getInstanceAdminRoles()),
    )
  }

  @Test
  fun `should fail authentication with invalid credentials`() {
    every { instanceAdminConfig.username } returns "admin"
    every { instanceAdminConfig.password } returns "password"

    val authRequest = UsernamePasswordCredentials("wrong", "credentials")
    val response = authProvider.authenticate(HttpRequest.GET("/"), authRequest)!!

    assertFalse(response.isAuthenticated)
  }
}
