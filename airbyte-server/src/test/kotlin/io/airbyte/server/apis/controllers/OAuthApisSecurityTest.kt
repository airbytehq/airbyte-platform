/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.OAuthScopeItem
import io.airbyte.api.server.generated.models.SourceOAuthScopesRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.OAuthHandler
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID

private const val SPEC = "OAuthApisSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Calls every source and destination OAuth route over HTTP with security enabled, at the exact path clients call. A
 * route moved by the controller's generated interface fails here rather than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"])
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class OAuthApisSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun oAuthHandler(): OAuthHandler =
      mockk(relaxed = true) {
        every { completeSourceOAuthHandleReturnSecret(any()) } returns completeOAuthResponse()
        every { completeDestinationOAuth(any()) } returns completeOAuthResponse()
        every { getSourceOAuthConsent(any()) } returns OAuthConsentRead(consentUrl = "https://example.com/consent")
        every { getDestinationOAuthConsent(any()) } returns OAuthConsentRead(consentUrl = "https://example.com/consent")
        every { getSourceOAuthScopes(any()) } returns
          SourceOAuthScopesRead(scopes = listOf(OAuthScopeItem(scope = "read")), optionalScopes = emptyList())
      }
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route succeeds with its role`(route: Route) {
    val status = statusOf(route.request(), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with ${route.allowedRole} returned $status" }
      .isBetween(200, 299)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route is forbidden without its role`(route: Route) {
    assertThat(statusOf(route.request(), AuthRoleConstants.AUTHENTICATED_USER)).isEqualTo(HttpStatus.FORBIDDEN)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `missing required field is a client error`(route: Route) {
    val status = statusOf(route.request(body = emptyMap()), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
  }

  // redirectUrl is optional in the spec, so the controller checks it before any OAuth flow runs
  @ParameterizedTest(name = "{0}")
  @MethodSource("completeRoutes")
  fun `complete without redirectUrl is a bad request`(route: Route) {
    val status = statusOf(route.request(body = route.body - "redirectUrl"), route.allowedRole)
    assertThat(status).isEqualTo(HttpStatus.BAD_REQUEST)
  }

  private fun statusOf(
    request: MutableHttpRequest<*>,
    role: String,
  ): HttpStatus =
    try {
      client.toBlocking().exchange(request.header(TEST_ROLES_HEADER, role), Any::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }

  data class Route(
    val path: String,
    val allowedRole: String,
    val body: Map<String, Any>,
  ) {
    fun request(body: Map<String, Any> = this.body): MutableHttpRequest<*> = HttpRequest.POST(path, body).contentType(MediaType.APPLICATION_JSON_TYPE)

    override fun toString() = "POST $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()

    // RunOAuthFlow intent on the source OAuth controller
    private const val SOURCE_EDITOR = AuthRoleConstants.WORKSPACE_SOURCE_EDITOR
    private const val DESTINATION_EDITOR = AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR

    private const val REDIRECT_URL = "https://example.com/callback"
    private val params = mapOf("client_id" to "id")

    private val routes =
      listOf(
        Route(
          "/api/v1/source_oauths/complete_oauth",
          SOURCE_EDITOR,
          mapOf("sourceDefinitionId" to id, "workspaceId" to id, "redirectUrl" to REDIRECT_URL),
        ),
        Route(
          "/api/v1/source_oauths/get_embedded_consent_url",
          SOURCE_EDITOR,
          mapOf("sourceDefinitionId" to id, "workspaceId" to id, "redirectUrl" to REDIRECT_URL),
        ),
        Route(
          "/api/v1/source_oauths/get_consent_url",
          SOURCE_EDITOR,
          mapOf("sourceDefinitionId" to id, "workspaceId" to id, "redirectUrl" to REDIRECT_URL),
        ),
        Route("/api/v1/source_oauths/get_scopes", SOURCE_EDITOR, mapOf("sourceDefinitionId" to id, "workspaceId" to id)),
        Route("/api/v1/source_oauths/revoke", SOURCE_EDITOR, mapOf("sourceDefinitionId" to id, "workspaceId" to id, "sourceId" to id)),
        Route("/api/v1/source_oauths/oauth_params/create", AuthRoleConstants.ADMIN, mapOf("sourceDefinitionId" to id, "params" to params)),
        Route(
          "/api/v1/destination_oauths/complete_oauth",
          DESTINATION_EDITOR,
          mapOf("destinationDefinitionId" to id, "workspaceId" to id, "redirectUrl" to REDIRECT_URL),
        ),
        Route(
          "/api/v1/destination_oauths/get_consent_url",
          DESTINATION_EDITOR,
          mapOf("destinationDefinitionId" to id, "workspaceId" to id, "redirectUrl" to REDIRECT_URL),
        ),
        Route(
          "/api/v1/destination_oauths/oauth_params/create",
          AuthRoleConstants.ADMIN,
          mapOf("destinationDefinitionId" to id, "params" to params),
        ),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun completeRoutes() = routes.filter { it.path.endsWith("/complete_oauth") }
  }
}

private fun completeOAuthResponse() = CompleteOAuthResponse(requestSucceeded = true, authPayload = mapOf("access_token" to "token"))

@Requires(property = "spec.name", value = SPEC)
@Singleton
class OAuthApisAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("oauth-apis-security-test-user", roles))
  }
}
