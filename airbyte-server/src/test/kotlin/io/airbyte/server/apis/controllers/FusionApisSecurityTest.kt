/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.airbyte.server.auth.OperationPermissions
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
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.Optional
import java.util.UUID

private const val SPEC = "FusionApisSecurityTest"
private const val ANONYMOUS_HEADER = "X-Test-Anonymous"

/**
 * Calls every Fusion route over HTTP with security enabled, at the exact path clients call. A route
 * moved by the controller's generated interface fails here rather than as a 404 in prod. The OSS
 * controllers are stubs, so a logged-in caller gets 501.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class FusionApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun authorizationDataService(): AuthorizationDataService = mockk()

    @Singleton
    @Primary
    fun operationPermissions(): OperationPermissions = mockk(relaxed = true)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Inject
  lateinit var authorizationDataService: AuthorizationDataService

  @BeforeEach
  fun configureSavedResources() {
    val resourceId = UUID.fromString(id)
    val owner = WorkspaceAndOrganization(resourceId, resourceId)
    every { authorizationDataService.savedSourceWorkspaceAndOrganization(resourceId) } returns owner
    every { authorizationDataService.savedDestinationWorkspaceAndOrganization(resourceId) } returns owner
    every { authorizationDataService.savedConnectionReferences(resourceId) } returns
      SavedConnectionReferences(resourceId, resourceId, Optional.empty(), Optional.empty(), emptyList())
    every { authorizationDataService.canUseDataplaneGroup(resourceId, resourceId) } returns true
    every {
      authorizationDataService.isAssignedSyncWorkload(resourceId, resourceId, resourceId, resourceId, resourceId, any(), resourceId, resourceId)
    } returns
      true
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route answers a logged in caller with not implemented`(route: Route) {
    val status = statusOf(route.request())
    assertThat(status)
      .withFailMessage { "$route returned $status, expected ${HttpStatus.NOT_IMPLEMENTED}" }
      .isEqualTo(HttpStatus.NOT_IMPLEMENTED)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route rejects an anonymous caller`(route: Route) {
    assertThat(statusOf(route.request().header(ANONYMOUS_HEADER, "true"))).isEqualTo(HttpStatus.UNAUTHORIZED)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routesWithRequiredFields")
  fun `missing required field is a client error`(route: Route) {
    val status = statusOf(route.request(body = emptyMap()))
    assertThat(status.code)
      .withFailMessage { "$route with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
  }

  private fun statusOf(request: MutableHttpRequest<*>): HttpStatus =
    try {
      client.toBlocking().exchange(request, Any::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }

  data class Route(
    val method: String,
    val path: String,
    val body: Map<String, Any> = emptyMap(),
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> =
      when (method) {
        "GET" -> HttpRequest.GET<Any>(path)
        "DELETE" -> HttpRequest.DELETE<Any>(path)
        "PUT" -> HttpRequest.PUT(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)
        else -> HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)
      }

    override fun toString() = "$method $path"
  }

  companion object {
    internal val id = UUID.randomUUID().toString()

    private val actorFlags = mapOf("enable_agent_access" to true, "enable_indexing" to true)
    private val destinationFlags = actorFlags + ("enable_backfill" to false)
    private val connectionFlags = mapOf("enable_indexing" to true)
    private val search = mapOf("type" to "keyword", "prompt" to "test")

    private val routes =
      listOf(
        Route("GET", "/api/v1/sources/$id/enablement"),
        Route("POST", "/api/v1/sources/$id/enablement", actorFlags),
        Route("PUT", "/api/v1/sources/$id/enablement", actorFlags + ("expectedState" to actorFlags)),
        Route("DELETE", "/api/v1/sources/$id/enablement"),
        Route("GET", "/api/v1/destinations/$id/enablement"),
        Route("POST", "/api/v1/destinations/$id/enablement", destinationFlags),
        Route("PUT", "/api/v1/destinations/$id/enablement", destinationFlags + ("expectedState" to destinationFlags)),
        Route("DELETE", "/api/v1/destinations/$id/enablement"),
        Route("GET", "/api/v1/connections/$id/enablement"),
        Route("POST", "/api/v1/connections/$id/enablement", connectionFlags),
        Route("PUT", "/api/v1/connections/$id/enablement", connectionFlags + ("expectedState" to connectionFlags)),
        Route("DELETE", "/api/v1/connections/$id/enablement"),
        Route("POST", "/api/v1/connections/$id/backfill"),
        Route(
          "POST",
          "/api/v1/connections/resolve_enablement_for_sync",
          listOf("organizationId", "workspaceId", "connectionId", "sourceId", "destinationId", "dataplaneGroupId", "workloadId")
            .associateWith { id },
        ),
        Route("POST", "/api/v1/sources/$id/search", search),
        Route("POST", "/api/v1/destinations/$id/search", search),
        Route("GET", "/api/v1/sources/$id/search-status"),
        Route("GET", "/api/v1/destinations/$id/search-status"),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.body.isNotEmpty() }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class FusionApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> =
    if (request.headers.contains(ANONYMOUS_HEADER)) {
      Flux.empty()
    } else {
      Flux.just(Authentication.build(FusionApisSecurityTest.id, listOf(AuthRoleConstants.AUTHENTICATED_USER)))
    }
}
