/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.micronaut

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.resources.Resources.read
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.services.impls.data.ApplicationServiceMicronautImpl
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.every
import io.mockk.mockk
import jakarta.ws.rs.BadRequestException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.Optional

internal class ApplicationServiceMicronautImplTests {
  private lateinit var instanceAdminConfig: AirbyteAuthConfig.AirbyteAuthInstanceAdminConfig

  private lateinit var airbyteAuthConfig: AirbyteAuthConfig

  private var tokenGenerator: JwtTokenGenerator? = null

  private var token: String? = null

  private val issuer = "https://example.com"

  @BeforeEach
  fun setup() {
    token = read("test.token")
    instanceAdminConfig =
      AirbyteAuthConfig.AirbyteAuthInstanceAdminConfig(
        clientId = "test-client-id",
        clientSecret = "test-client-secret",
        password = "test-password",
        username = "test",
      )
    tokenGenerator = mockk<JwtTokenGenerator>()
    airbyteAuthConfig = AirbyteAuthConfig(instanceAdmin = instanceAdminConfig, tokenIssuer = issuer)
    every { tokenGenerator!!.generateToken(any()) } returns Optional.of(token!!)
  }

  @Test
  fun testGetToken() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        airbyteAuthConfig,
        tokenGenerator!!,
      )

    // For some reason, this test suite loads a mock token from a resource instead of running the actual
    // code.
    // So these roles match the mock token, not the actual roles.
    val expectedRoles =
      mutableSetOf<String?>(
        "WORKSPACE_EDITOR",
        "ORGANIZATION_RUNNER",
        "WORKSPACE_READER",
        "ORGANIZATION_EDITOR",
        "WORKSPACE_ADMIN",
        "WORKSPACE_RUNNER",
        "EDITOR",
        "AUTHENTICATED_USER",
        "ORGANIZATION_MEMBER",
        "ORGANIZATION_READER",
        "READER",
        "ADMIN",
        "ORGANIZATION_ADMIN",
      )
    val token = applicationServer.getToken("test-client-id", "test-client-secret")
    val claims = getTokenClaims(token)

    assertEquals(expectedRoles, getRolesFromNode((claims.get("roles") as ArrayNode?)!!))
    assertEquals("airbyte-server", claims.get("iss").asText())
    assertEquals(ApplicationServiceMicronautImpl.Companion.DEFAULT_AUTH_USER_ID.toString(), claims.get("sub").asText())
  }

  @Test
  fun testGetTokenWithInvalidCredentials() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        airbyteAuthConfig,
        tokenGenerator!!,
      )

    assertThrows<BadRequestException> {
      applicationServer.getToken("test-client-id", "wrong-secret")
    }
  }

  @Test
  fun testListingApplications() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        airbyteAuthConfig,
        tokenGenerator!!,
      )

    val applications = applicationServer.listApplicationsByUser(AuthenticatedUser().withName("Test User"))
    assertEquals(1, applications.size)
  }

  @Test
  fun testCreateApplication() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        airbyteAuthConfig,
        tokenGenerator!!,
      )

    assertThrows<UnsupportedOperationException> {
      applicationServer.createApplication(AuthenticatedUser(), "Test Application")
    }
  }

  @Test
  fun testDeleteApplication() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        airbyteAuthConfig,
        tokenGenerator!!,
      )

    assertThrows<UnsupportedOperationException> {
      applicationServer.deleteApplication(AuthenticatedUser(), "Test Application")
    }
  }

  private fun getTokenClaims(token: String): JsonNode {
    val decodedPayload =
      String(
        Base64.getUrlDecoder().decode(token.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[1]),
        StandardCharsets.UTF_8,
      )
    return deserialize(decodedPayload)
  }

  private fun getRolesFromNode(claimsNode: ArrayNode): MutableSet<String?> {
    val roles: MutableSet<String?> = HashSet()
    for (role in claimsNode) {
      roles.add(role.asText())
    }
    return roles
  }
}
