/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.micronaut

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import io.airbyte.commons.auth.config.TokenExpirationConfig
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.resources.Resources.read
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.config.InstanceAdminConfig
import io.airbyte.data.services.impls.data.ApplicationServiceMicronautImpl
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import jakarta.ws.rs.BadRequestException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.Mockito.mock
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.Optional

internal class ApplicationServiceMicronautImplTests {
  private var instanceAdminConfig: InstanceAdminConfig? = null

  private var tokenExpirationConfig: TokenExpirationConfig? = null

  private var tokenGenerator: JwtTokenGenerator? = null

  private var token: String? = null

  private val issuer = "https://example.com"

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    token = read("test.token")
    instanceAdminConfig = InstanceAdminConfig()
    instanceAdminConfig!!.username = "test"
    instanceAdminConfig!!.password = "test-password"
    instanceAdminConfig!!.clientId = "test-client-id"
    instanceAdminConfig!!.clientSecret = "test-client-secret"
    tokenGenerator = mock<JwtTokenGenerator>()
    tokenExpirationConfig = TokenExpirationConfig()
    Mockito.`when`(tokenGenerator!!.generateToken(ArgumentMatchers.anyMap())).thenReturn(Optional.of(token!!))
  }

  @Test
  fun testGetToken() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        instanceAdminConfig!!,
        tokenExpirationConfig!!,
        tokenGenerator!!,
        issuer,
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

    Assertions.assertEquals(expectedRoles, getRolesFromNode((claims.get("roles") as com.fasterxml.jackson.databind.node.ArrayNode?)!!))
    Assertions.assertEquals("airbyte-server", claims.get("iss").asText())
    Assertions.assertEquals(ApplicationServiceMicronautImpl.Companion.DEFAULT_AUTH_USER_ID.toString(), claims.get("sub").asText())
  }

  @Test
  fun testGetTokenWithInvalidCredentials() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        instanceAdminConfig!!,
        tokenExpirationConfig!!,
        tokenGenerator!!,
        issuer,
      )

    Assertions.assertThrows<BadRequestException?>(
      BadRequestException::class.java,
      Executable { applicationServer.getToken("test-client-id", "wrong-secret") },
    )
  }

  @Test
  fun testListingApplications() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        instanceAdminConfig!!,
        tokenExpirationConfig!!,
        tokenGenerator!!,
        issuer,
      )

    val applications = applicationServer.listApplicationsByUser(AuthenticatedUser().withName("Test User"))
    Assertions.assertEquals(1, applications.size)
  }

  @Test
  fun testCreateApplication() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        instanceAdminConfig!!,
        tokenExpirationConfig!!,
        tokenGenerator!!,
        issuer,
      )

    Assertions.assertThrows<UnsupportedOperationException?>(
      UnsupportedOperationException::class.java,
      Executable { applicationServer.createApplication(AuthenticatedUser(), "Test Application") },
    )
  }

  @Test
  fun testDeleteApplication() {
    val applicationServer =
      ApplicationServiceMicronautImpl(
        instanceAdminConfig!!,
        tokenExpirationConfig!!,
        tokenGenerator!!,
        issuer,
      )

    Assertions.assertThrows<UnsupportedOperationException?>(
      UnsupportedOperationException::class.java,
      Executable { applicationServer.deleteApplication(AuthenticatedUser(), "Test Application") },
    )
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
    val roles: MutableSet<String?> = HashSet<String?>()
    for (role in claimsNode) {
      roles.add(role.asText())
    }
    return roles
  }
}
