/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.EnableScimRequestBody
import io.airbyte.api.server.generated.models.ScimConfigResponse
import io.airbyte.api.server.generated.models.ScimConfigStatus
import io.airbyte.api.server.generated.models.ScimIdpProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.support.AuthenticationHttpHeaders
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID
import io.airbyte.domain.models.scim.ScimIdpProvider as DomainScimIdpProvider

private const val SCIM_SECURITY_SPEC = "ScimConfigApiSecurityTest"
private const val TEST_ADMIN_ORGANIZATION_HEADER = "X-Test-Admin-Organization-Id"
private const val TEST_PREAUTHORIZED_ADMIN_HEADER = "X-Test-Preauthorized-Admin"
private const val TEST_SUBJECT = "scim-security-test-user"
private val ADMIN_ORGANIZATION_ID = UUID.fromString("9e0cbf2c-f2d8-4b04-b936-3a212f74f902")
private val OTHER_ORGANIZATION_ID = UUID.fromString("18d58fe9-8c72-42f1-a3b7-2edc5e9ead8e")
private val TEST_USER_ID = UUID.fromString("a068dff5-75cd-4f9a-b58f-cbd19a5d6225")

@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SCIM_SECURITY_SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class ScimConfigApiSecurityTest {
  @Requires(property = "spec.name", value = SCIM_SECURITY_SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun scimConfigurationService(): ScimConfigurationService = mockk()

    @Singleton
    @Primary
    fun currentUserService(): CurrentUserService = mockk()
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var service: ScimConfigurationService

  @Inject
  lateinit var currentUserService: CurrentUserService

  @BeforeEach
  fun setUp() {
    clearMocks(service, currentUserService)
    every { currentUserService.getCurrentUser() } returns
      mockk {
        every { userId } returns TEST_USER_ID
      }
  }

  @Test
  fun `organization admin can enable SCIM for their organization`() {
    every {
      service.enable(
        OrganizationId(ADMIN_ORGANIZATION_ID),
        DomainScimIdpProvider.OKTA,
        UserId(TEST_USER_ID),
      )
    } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = DomainScimIdpProvider.OKTA,
        token = "airbyte_scim_token",
      )

    val response =
      client.toBlocking().exchange(
        enableRequest(ADMIN_ORGANIZATION_ID).header(TEST_ADMIN_ORGANIZATION_HEADER, ADMIN_ORGANIZATION_ID.toString()),
        ScimConfigResponse::class.java,
      )

    assertThat(response.status).isEqualTo(HttpStatus.OK)
    assertThat(response.body()!!.status).isEqualTo(ScimConfigStatus.ENABLED)
  }

  @Test
  fun `non-admin cannot enable SCIM`() {
    val error =
      assertThrows<HttpClientResponseException> {
        client.toBlocking().exchange<EnableScimRequestBody, Any>(enableRequest(ADMIN_ORGANIZATION_ID))
      }

    assertThat(error.status).isEqualTo(HttpStatus.FORBIDDEN)
    verify(exactly = 0) { service.enable(any(), any(), any()) }
  }

  @Test
  fun `organization admin cannot enable SCIM for another organization`() {
    val error =
      assertThrows<HttpClientResponseException> {
        client
          .toBlocking()
          .exchange<EnableScimRequestBody, Any>(
            enableRequest(OTHER_ORGANIZATION_ID).header(TEST_ADMIN_ORGANIZATION_HEADER, ADMIN_ORGANIZATION_ID.toString()),
          )
      }

    assertThat(error.status).isEqualTo(HttpStatus.FORBIDDEN)
    verify(exactly = 0) { service.enable(any(), any(), any()) }
  }

  @Test
  fun `organization-scoped authorization rejects invalid organization IDs before validation`() {
    invalidEnableRequestBodies.forEach { body ->
      val error =
        assertThrows<HttpClientResponseException> {
          client
            .toBlocking()
            .exchange<Map<String, String>, Any>(
              HttpRequest
                .POST("/api/v1/scim_config/enable", body)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .header(TEST_ADMIN_ORGANIZATION_HEADER, ADMIN_ORGANIZATION_ID.toString()),
            )
        }

      assertThat(error.status).isEqualTo(HttpStatus.FORBIDDEN)
    }
    verify(exactly = 0) { service.enable(any(), any(), any()) }
  }

  @Test
  fun `preauthorized admin receives validation errors for invalid organization IDs`() {
    invalidEnableRequestBodies.forEach { body ->
      val error =
        assertThrows<HttpClientResponseException> {
          client
            .toBlocking()
            .exchange<Map<String, String>, Any>(
              HttpRequest
                .POST("/api/v1/scim_config/enable", body)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .header(TEST_PREAUTHORIZED_ADMIN_HEADER, "true"),
            )
        }

      assertThat(error.status).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY)
    }
    verify(exactly = 0) { service.enable(any(), any(), any()) }
  }

  private val invalidEnableRequestBodies =
    listOf(
      mapOf("idpProvider" to "okta"),
      mapOf("organizationId" to "not-a-uuid", "idpProvider" to "okta"),
    )

  private fun enableRequest(organizationId: UUID): MutableHttpRequest<EnableScimRequestBody> =
    HttpRequest.POST(
      "/api/v1/scim_config/enable",
      EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
    )
}

@Requires(property = "spec.name", value = SCIM_SECURITY_SPEC)
@Singleton
class ScimSecurityAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val requestedOrganization = request.headers[AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER]
    val adminOrganization = request.headers[TEST_ADMIN_ORGANIZATION_HEADER]
    val roles =
      when {
        request.headers[TEST_PREAUTHORIZED_ADMIN_HEADER] == "true" -> listOf(AuthRoleConstants.ORGANIZATION_ADMIN)
        requestedOrganization != null && requestedOrganization == adminOrganization -> listOf(AuthRoleConstants.ORGANIZATION_ADMIN)
        else -> listOf(AuthRoleConstants.AUTHENTICATED_USER)
      }
    return Flux.just(Authentication.build(TEST_SUBJECT, roles))
  }
}
