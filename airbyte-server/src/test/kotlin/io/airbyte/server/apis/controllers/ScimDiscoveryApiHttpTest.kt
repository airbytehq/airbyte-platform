/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.scim.ScimAccessDeniedException
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimAuthenticationService
import io.airbyte.server.scim.SCIM_ERROR_SCHEMA
import io.airbyte.server.scim.ScimDiscoveryService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

private const val SCIM_DISCOVERY_SPEC = "ScimDiscoveryApiHttpTest"
private const val SCIM_CONTENT_TYPE = "application/scim+json"

@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SCIM_DISCOVERY_SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
class ScimDiscoveryApiHttpTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var authenticationService: ScimAuthenticationService

  @Inject
  lateinit var discoveryService: ScimDiscoveryService

  private val objectMapper = jacksonObjectMapper()
  private val rawToken = "airbyte_scim_${"a".repeat(64)}"
  private val context =
    ScimAuthenticationContext(
      configurationId = UUID.randomUUID(),
      organizationId = OrganizationId(UUID.randomUUID()),
      tokenHash = "b".repeat(64),
    )

  @BeforeEach
  fun setUp() {
    clearMocks(authenticationService, discoveryService)
    every { authenticationService.authenticate(rawToken) } returns context
  }

  @Test
  fun `authenticated discovery routes return the approved SCIM payloads`() {
    val expectedSchemas =
      objectMapper.valueToTree<JsonNode>(
        mapOf(
          "schemas" to listOf("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
          "totalResults" to 2,
          "Resources" to listOf(readFixture("schema-user.json"), readFixture("schema-group.json")),
          "startIndex" to 1,
          "itemsPerPage" to 2,
        ),
      )
    val cases =
      listOf(
        "/scim/v2/ServiceProviderConfig" to readFixture("service-provider-config.json"),
        "/scim/v2/ResourceTypes" to readFixture("resource-types.json"),
        "/scim/v2/ResourceTypes/User" to readFixture("resource-type-user.json"),
        "/scim/v2/ResourceTypes/Group" to readFixture("resource-type-group.json"),
        "/scim/v2/Schemas" to expectedSchemas,
        "/scim/v2/Schemas/urn:ietf:params:scim:schemas:core:2.0:User" to readFixture("schema-user.json"),
        "/scim/v2/Schemas/urn:ietf:params:scim:schemas:core:2.0:Group" to readFixture("schema-group.json"),
      )

    cases.forEach { (path, expectedBody) ->
      val response =
        client.toBlocking().exchange(
          HttpRequest.GET<Any>(path).bearerAuth(rawToken),
          JsonNode::class.java,
        )

      assertThat(response.status).isEqualTo(HttpStatus.OK)
      assertThat(response.contentType.orElseThrow().name).isEqualTo(SCIM_CONTENT_TYPE)
      assertThat(response.body()).isEqualTo(expectedBody)
    }
  }

  @Test
  fun `discovery collection query parameters do not change advertised resources`() {
    val expectedSchemas =
      objectMapper.valueToTree<JsonNode>(
        mapOf(
          "schemas" to listOf("urn:ietf:params:scim:api:messages:2.0:ListResponse"),
          "totalResults" to 2,
          "Resources" to listOf(readFixture("schema-user.json"), readFixture("schema-group.json")),
          "startIndex" to 1,
          "itemsPerPage" to 2,
        ),
      )
    val collectionCases =
      listOf(
        "/scim/v2/ResourceTypes" to readFixture("resource-types.json"),
        "/scim/v2/Schemas" to expectedSchemas,
      )
    val queryStrings =
      listOf(
        "filter=id%20eq%20%22User%22",
        "sortBy=name&sortOrder=descending&startIndex=2&count=1",
      )

    collectionCases.forEach { (path, expectedBody) ->
      queryStrings.forEach { queryString ->
        val response =
          client.toBlocking().exchange(
            HttpRequest.GET<Any>("$path?$queryString").bearerAuth(rawToken),
            JsonNode::class.java,
          )

        assertThat(response.status).isEqualTo(HttpStatus.OK)
        assertThat(response.contentType.orElseThrow().name).isEqualTo(SCIM_CONTENT_TYPE)
        assertThat(response.body()).isEqualTo(expectedBody)
      }
    }
  }

  @Test
  fun `discovery routes apply projections and reject conflicting parameters`() {
    val projected =
      client.toBlocking().exchange(
        HttpRequest.GET<Any>("/scim/v2/ServiceProviderConfig?attributes=patch.supported").bearerAuth(rawToken),
        JsonNode::class.java,
      )

    assertThat(projected.body())
      .isEqualTo(
        objectMapper.valueToTree<JsonNode>(
          mapOf(
            "schemas" to listOf("urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"),
            "patch" to mapOf("supported" to true),
          ),
        ),
      )
    assertScimError(
      exchangeError(
        HttpRequest.GET<Any>("/scim/v2/ResourceTypes?attributes=name&excludedAttributes=endpoint").bearerAuth(rawToken),
      ),
      status = HttpStatus.BAD_REQUEST,
      detail = "The SCIM projection is malformed or unsupported",
      bearerChallenge = false,
      scimType = "invalidValue",
    )
  }

  @Test
  fun `missing bearer credentials return a SCIM unauthorized response`() {
    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/ServiceProviderConfig")),
      status = HttpStatus.UNAUTHORIZED,
      detail = "Invalid bearer token",
      bearerChallenge = true,
    )
  }

  @Test
  fun `gate denial returns a SCIM forbidden response`() {
    every { authenticationService.authenticate(rawToken) } throws ScimAccessDeniedException("SCIM access is denied")

    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/ServiceProviderConfig").bearerAuth(rawToken)),
      status = HttpStatus.FORBIDDEN,
      detail = "SCIM access is denied",
      bearerChallenge = false,
    )
  }

  @Test
  fun `unknown discovery resources return SCIM not found responses`() {
    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/ResourceTypes/unknown").bearerAuth(rawToken)),
      status = HttpStatus.NOT_FOUND,
      detail = "ResourceType not found",
      bearerChallenge = false,
    )
    assertScimError(
      exchangeError(HttpRequest.GET<Any>("/scim/v2/Schemas/urn:example:unknown").bearerAuth(rawToken)),
      status = HttpStatus.NOT_FOUND,
      detail = "Schema not found",
      bearerChallenge = false,
    )
  }

  @Test
  fun `unexpected discovery failures remain internal server errors`() {
    every { discoveryService.serviceProviderConfig(null, null) } throws IllegalStateException("unexpected failure")

    val error = exchangeError(HttpRequest.GET<Any>("/scim/v2/ServiceProviderConfig").bearerAuth(rawToken))

    assertThat(error.status).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR)
  }

  private fun <I> exchangeError(request: HttpRequest<I>): HttpClientResponseException =
    assertThrows {
      client.toBlocking().exchange<I, JsonNode>(request)
    }

  private fun assertScimError(
    error: HttpClientResponseException,
    status: HttpStatus,
    detail: String,
    bearerChallenge: Boolean,
    scimType: String? = null,
  ) {
    val actualBody = error.response.getBody(JsonNode::class.java).orElse(null)
    val actualContentType =
      error.response.contentType
        .orElseThrow()
        .name
    assertThat(error.status)
      .withFailMessage("Expected %s but received %s with body %s", status, error.status, actualBody)
      .isEqualTo(status)
    assertThat(actualContentType)
      .withFailMessage("Expected SCIM content type but received %s with body %s", actualContentType, actualBody)
      .isEqualTo(SCIM_CONTENT_TYPE)
    val expectedBody =
      mutableMapOf<String, Any>(
        "schemas" to listOf(SCIM_ERROR_SCHEMA),
        "status" to status.code.toString(),
        "detail" to detail,
      )
    scimType?.let { expectedBody["scimType"] = it }
    assertThat(actualBody).isEqualTo(objectMapper.valueToTree<JsonNode>(expectedBody))
    assertThat(error.response.headers.contains(HttpHeaders.WWW_AUTHENTICATE)).isEqualTo(bearerChallenge)
    if (bearerChallenge) {
      assertThat(error.response.headers[HttpHeaders.WWW_AUTHENTICATE]).isEqualTo("Bearer")
    }
  }

  private fun readFixture(fixtureName: String): JsonNode =
    requireNotNull(javaClass.getResourceAsStream("/scim/discovery/$fixtureName")).use {
      objectMapper.readTree(it)
    }
}

@Requires(property = "spec.name", value = SCIM_DISCOVERY_SPEC)
@Factory
class ScimDiscoveryTestBeans {
  @Singleton
  @Primary
  fun scimAuthenticationService(): ScimAuthenticationService = mockk()

  @Singleton
  @Primary
  fun scimDiscoveryService(): ScimDiscoveryService = spyk(ScimDiscoveryService())
}
