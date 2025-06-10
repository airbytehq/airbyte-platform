/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config.community.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.auth.AuthRole
import io.airbyte.config.Organization
import io.airbyte.config.persistence.OrganizationPersistence
import io.airbyte.data.config.InstanceAdminConfig
import io.micronaut.http.HttpRequest
import io.micronaut.security.authentication.UsernamePasswordCredentials
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional

private const val EMAIL = "foo@airbyte.io"
private const val PASSWORD = "hunter2"

class CommunityAuthProviderTest {
  private val organizationPersistence = mockk<OrganizationPersistence>()
  private val instanceAdminConfig = mockk<InstanceAdminConfig>()
  private val authProvider = CommunityAuthProvider<Any>(instanceAdminConfig, organizationPersistence)

  @Test
  fun `should authenticate successfully with valid credentials`() {
    val defaultOrg = Organization().withEmail(EMAIL)
    every { instanceAdminConfig.password } returns PASSWORD
    every { organizationPersistence.defaultOrganization } returns Optional.of(defaultOrg)

    val authRequest = UsernamePasswordCredentials(EMAIL, PASSWORD)
    val response = authProvider.authenticate(HttpRequest.GET("/"), authRequest)!!

    assertTrue(response.isAuthenticated)
    assertEquals(DEFAULT_USER_ID.toString(), response.authentication.get().name)
    assertTrue(
      response.authentication
        .get()
        .roles
        .containsAll(AuthRole.getInstanceAdminRoles()),
    )
  }

  @Test
  fun `should fail authentication with invalid email`() {
    val defaultOrg = Organization().withEmail(EMAIL)
    every { instanceAdminConfig.password } returns PASSWORD
    every { organizationPersistence.defaultOrganization } returns Optional.of(defaultOrg)

    val authRequest = UsernamePasswordCredentials("wrong@airbyte.io", PASSWORD)
    val response = authProvider.authenticate(HttpRequest.GET("/"), authRequest)!!

    assertFalse(response.isAuthenticated)
  }

  @Test
  fun `should fail authentication with invalid password`() {
    val defaultOrg = Organization().withEmail(EMAIL)
    every { instanceAdminConfig.password } returns PASSWORD
    every { organizationPersistence.defaultOrganization } returns Optional.of(defaultOrg)

    val authRequest = UsernamePasswordCredentials(EMAIL, "wrong")
    val response = authProvider.authenticate(HttpRequest.GET("/"), authRequest)!!

    assertFalse(response.isAuthenticated)
  }

  @Test
  fun `should throw exception when default organization is not found`() {
    every { organizationPersistence.defaultOrganization } returns Optional.empty()

    val authRequest = UsernamePasswordCredentials(EMAIL, PASSWORD)

    assertThrows<ForbiddenProblem> {
      authProvider.authenticate(HttpRequest.GET("/"), authRequest)
    }
  }
}
