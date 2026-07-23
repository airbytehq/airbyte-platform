/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.EnableScimRequestBody
import io.airbyte.api.server.generated.models.KnownExceptionInfo
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.ScimConfigResponse
import io.airbyte.api.server.generated.models.ScimConfigStatus
import io.airbyte.api.server.generated.models.ScimIdpProvider
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.models.scim.ScimConfigurationConflictException
import io.airbyte.domain.models.scim.ScimConfigurationRead
import io.airbyte.domain.models.scim.ScimConfigurationStatus
import io.airbyte.domain.models.scim.ScimOrganizationNotFoundException
import io.airbyte.domain.services.scim.ScimConfigurationService
import io.airbyte.server.assertStatus
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import io.airbyte.domain.models.scim.ScimIdpProvider as DomainScimIdpProvider

private const val SCIM_CONFIG_PATH = "/api/v1/scim_config"

@MicronautTest(rebuildContext = true)
internal class ScimConfigApiHttpTest {
  @Factory
  class TestFactory {
    @Singleton
    @Replaces(ScimConfigurationService::class)
    fun scimConfigurationService(): ScimConfigurationService = mockk()
  }

  @Inject
  lateinit var service: ScimConfigurationService

  @Inject
  lateinit var currentUserService: CurrentUserService

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(CurrentUserService::class)
  fun currentUserService(): CurrentUserService = mockk(relaxed = true)

  private val organizationId = UUID.randomUUID()
  private val userId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    clearMocks(service)
    every { currentUserService.getCurrentUser() } returns
      mockk {
        every { this@mockk.userId } returns this@ScimConfigApiHttpTest.userId
      }
  }

  @Test
  fun `get returns a serialized 200 response`() {
    every { service.getConfiguration(OrganizationId(organizationId)) } returns
      ScimConfigurationRead(status = ScimConfigurationStatus.NOT_CONFIGURED)

    val response =
      client.toBlocking().exchange(
        HttpRequest.POST("$SCIM_CONFIG_PATH/get", OrganizationIdRequestBody(organizationId)),
        ScimConfigResponse::class.java,
      )

    assertStatus(HttpStatus.OK, response.status)
    assertThat(response.body()).isNotNull
    assertThat(response.body()!!.status).isEqualTo(ScimConfigStatus.NOT_CONFIGURED)
    assertThat(response.body()!!.scimBaseUrl).endsWith("/scim/v2")
    assertThat(response.body()!!.token).isNull()
  }

  @Test
  fun `enable returns a serialized 200 response with the one-time token`() {
    val createdAt = OffsetDateTime.of(2026, 7, 14, 1, 2, 3, 0, ZoneOffset.UTC)
    every {
      service.enable(OrganizationId(organizationId), DomainScimIdpProvider.OKTA, UserId(userId))
    } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = DomainScimIdpProvider.OKTA,
        createdAt = createdAt,
        updatedAt = createdAt,
        token = "airbyte_scim_initial_token",
      )

    val response =
      client.toBlocking().exchange(
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
        ),
        ScimConfigResponse::class.java,
      )

    assertStatus(HttpStatus.OK, response.status)
    assertThat(response.body()).isNotNull
    assertThat(response.body()!!.status).isEqualTo(ScimConfigStatus.ENABLED)
    assertThat(response.body()!!.idpProvider).isEqualTo(ScimIdpProvider.OKTA)
    assertThat(response.body()!!.createdAt).isEqualTo(createdAt.toEpochSecond())
    assertThat(response.body()!!.token).isEqualTo("airbyte_scim_initial_token")
  }

  @Test
  fun `same-provider enable returns 200 and omits the token from serialized JSON`() {
    every {
      service.enable(OrganizationId(organizationId), DomainScimIdpProvider.OKTA, UserId(userId))
    } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = DomainScimIdpProvider.OKTA,
      )

    val response =
      client.toBlocking().exchange(
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
        ),
        JsonNode::class.java,
      )

    assertStatus(HttpStatus.OK, response.status)
    assertThat(response.body()!!["status"].asText()).isEqualTo("enabled")
    assertThat(response.body()!!["idpProvider"].asText()).isEqualTo("okta")
    assertThat(response.body()!!.has("token")).isFalse()
  }

  @Test
  fun `different-provider enable returns 409 with the known error body`() {
    val message = "SCIM is already enabled with a different identity provider"
    every { service.enable(any(), DomainScimIdpProvider.MICROSOFT_ENTRA_ID, any()) } throws
      ScimConfigurationConflictException(message)

    val error =
      exchangeError(
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.MICROSOFT_ENTRA_ID),
        ),
      )

    assertStatus(HttpStatus.CONFLICT, error.status)
    assertThat(
      error.response
        .getBody(KnownExceptionInfo::class.java)
        .orElseThrow()
        .message,
    ).isEqualTo(message)
  }

  @Test
  fun `enable from disabled returns 409 with the known error body`() {
    val message = "Disabled SCIM configurations cannot be re-enabled by this operation"
    every { service.enable(any(), DomainScimIdpProvider.OKTA, any()) } throws ScimConfigurationConflictException(message)

    val error =
      exchangeError(
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
        ),
      )

    assertStatus(HttpStatus.CONFLICT, error.status)
    assertThat(
      error.response
        .getBody(KnownExceptionInfo::class.java)
        .orElseThrow()
        .message,
    ).isEqualTo(message)
  }

  @Test
  fun `rotation returns a serialized 200 response with the replacement token`() {
    every { service.rotateToken(OrganizationId(organizationId), UserId(userId)) } returns
      ScimConfigurationRead(
        status = ScimConfigurationStatus.ENABLED,
        idpProvider = DomainScimIdpProvider.MICROSOFT_ENTRA_ID,
        token = "airbyte_scim_replacement_token",
      )

    val response =
      client.toBlocking().exchange(
        HttpRequest.POST("$SCIM_CONFIG_PATH/rotate_token", OrganizationIdRequestBody(organizationId)),
        ScimConfigResponse::class.java,
      )

    assertStatus(HttpStatus.OK, response.status)
    assertThat(response.body()).isNotNull
    assertThat(response.body()!!.status).isEqualTo(ScimConfigStatus.ENABLED)
    assertThat(response.body()!!.idpProvider).isEqualTo(ScimIdpProvider.MICROSOFT_ENTRA_ID)
    assertThat(response.body()!!.token).isEqualTo("airbyte_scim_replacement_token")
  }

  @Test
  fun `rotation from an absent configuration returns 409`() {
    assertRotationConflict("SCIM must be enabled before its token can be rotated")
  }

  @Test
  fun `rotation from a disabled configuration returns 409`() {
    assertRotationConflict("SCIM must be enabled before its token can be rotated")
  }

  @Test
  fun `gate denial returns 403 with no token`() {
    val message = "SCIM is not available for organization $organizationId"
    every { service.enable(any(), any(), any()) } throws ScimAccessDeniedException(message)

    val error =
      exchangeError(
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
        ),
      )

    assertStatus(HttpStatus.FORBIDDEN, error.status)
    val body = error.response.getBody(JsonNode::class.java).orElseThrow()
    assertThat(body["message"].asText()).isEqualTo(message)
    assertThat(body.has("token")).isFalse()
  }

  @Test
  fun `missing organizations return 404 from every mutating endpoint`() {
    val missing = ScimOrganizationNotFoundException(organizationId)
    every { service.enable(any(), any(), any()) } throws missing
    every { service.rotateToken(any(), any()) } throws missing
    every { service.disable(any(), any()) } throws missing
    val requests =
      listOf(
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          EnableScimRequestBody(organizationId, ScimIdpProvider.OKTA),
        ),
        HttpRequest.POST("$SCIM_CONFIG_PATH/rotate_token", OrganizationIdRequestBody(organizationId)),
        HttpRequest.POST("$SCIM_CONFIG_PATH/disable", OrganizationIdRequestBody(organizationId)),
      )

    requests.forEach { request ->
      val error = exchangeError(request)
      assertStatus(HttpStatus.NOT_FOUND, error.status)
      val body = error.response.getBody(JsonNode::class.java).orElseThrow()
      assertThat(body.hasNonNull("errorId")).isTrue()
    }
  }

  @Test
  fun `missing and malformed request fields return 422`() {
    val invalidRequests =
      listOf(
        HttpRequest.POST("$SCIM_CONFIG_PATH/enable", mapOf("idpProvider" to "okta")),
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          mapOf("organizationId" to "not-a-uuid", "idpProvider" to "okta"),
        ),
        HttpRequest.POST("$SCIM_CONFIG_PATH/enable", mapOf("organizationId" to organizationId.toString())),
        HttpRequest.POST(
          "$SCIM_CONFIG_PATH/enable",
          mapOf("organizationId" to organizationId.toString(), "idpProvider" to "unsupported"),
        ),
        HttpRequest.POST("$SCIM_CONFIG_PATH/rotate_token", emptyMap<String, String>()),
        HttpRequest.POST("$SCIM_CONFIG_PATH/disable", mapOf("organizationId" to "not-a-uuid")),
      )

    invalidRequests.forEach { request ->
      request.contentType(MediaType.APPLICATION_JSON_TYPE)
      assertStatus(HttpStatus.UNPROCESSABLE_ENTITY, exchangeError(request).status)
    }

    verify(exactly = 0) { service.enable(any(), any(), any()) }
    verify(exactly = 0) { service.rotateToken(any(), any()) }
    verify(exactly = 0) { service.disable(any(), any()) }
  }

  @Test
  fun `disable returns 204 with no response body`() {
    every { service.disable(OrganizationId(organizationId), UserId(userId)) } just Runs

    val response: HttpResponse<Any> =
      client.toBlocking().exchange(
        HttpRequest.POST("$SCIM_CONFIG_PATH/disable", OrganizationIdRequestBody(organizationId)),
      )

    assertStatus(HttpStatus.NO_CONTENT, response.status)
    assertThat(response.body()).isNull()
    verify(exactly = 1) { service.disable(OrganizationId(organizationId), UserId(userId)) }
  }

  @Test
  fun `disable remains a bodyless 204 when configuration is absent or already disabled`() {
    every { service.disable(OrganizationId(organizationId), UserId(userId)) } just Runs
    val request = HttpRequest.POST("$SCIM_CONFIG_PATH/disable", OrganizationIdRequestBody(organizationId))

    repeat(2) {
      val response: HttpResponse<Any> = client.toBlocking().exchange(request)
      assertStatus(HttpStatus.NO_CONTENT, response.status)
      assertThat(response.body()).isNull()
    }

    verify(exactly = 2) { service.disable(OrganizationId(organizationId), UserId(userId)) }
  }

  private fun assertRotationConflict(message: String) {
    every { service.rotateToken(any(), any()) } throws ScimConfigurationConflictException(message)

    val error =
      exchangeError(
        HttpRequest.POST("$SCIM_CONFIG_PATH/rotate_token", OrganizationIdRequestBody(organizationId)),
      )

    assertStatus(HttpStatus.CONFLICT, error.status)
    assertThat(
      error.response
        .getBody(KnownExceptionInfo::class.java)
        .orElseThrow()
        .message,
    ).isEqualTo(message)
  }

  private fun <I : Any> exchangeError(request: HttpRequest<I>): HttpClientResponseException =
    assertThrows {
      client.toBlocking().exchange<I, Any>(request)
    }
}
