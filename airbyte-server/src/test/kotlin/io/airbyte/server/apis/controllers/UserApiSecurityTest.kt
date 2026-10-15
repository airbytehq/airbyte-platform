/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.UserHandler
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
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import java.util.UUID

private const val SPEC = "UserApiSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Exercises every user route over HTTP with security enabled, at the exact path clients call, so a
 * route moved by the controller's generated interface fails here rather than as a generic 403 in prod.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class UserApiSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun userHandler(): UserHandler = mockk(relaxed = true)
  }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @ParameterizedTest(name = "{0}")
  @MethodSource("routes")
  fun `route is reachable with its role`(route: Route) {
    assertThat(statusOf(route.request(), route.allowedRole))
      .withFailMessage { "${route.path} with ${route.allowedRole} should be routed and authorized" }
      .isNotIn(HttpStatus.UNAUTHORIZED, HttpStatus.FORBIDDEN, HttpStatus.NOT_FOUND, HttpStatus.METHOD_NOT_ALLOWED)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("securedRoutes")
  fun `route is forbidden without its role`(route: Route) {
    assertThat(statusOf(route.request(), AuthRoleConstants.AUTHENTICATED_USER)).isEqualTo(HttpStatus.FORBIDDEN)
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("routesWithRequiredFields")
  fun `missing required field is a client error`(route: Route) {
    val status = statusOf(route.request(body = emptyMap()), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
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

    private val routes =
      listOf(
        Route("/api/v1/users/get", AuthRoleConstants.ADMIN, mapOf("userId" to id)),
        Route("/api/v1/users/get_by_auth_id", AuthRoleConstants.ADMIN, mapOf("authUserId" to "auth-user-id")),
        Route("/api/v1/users/get_by_email", AuthRoleConstants.ADMIN, mapOf("email" to "a@example.com")),
        Route("/api/v1/users/delete", AuthRoleConstants.ADMIN, mapOf("userId" to id)),
        Route("/api/v1/users/update", AuthRoleConstants.ADMIN, mapOf("userId" to id)),
        Route("/api/v1/users/list_by_organization_id", AuthRoleConstants.ORGANIZATION_MEMBER, mapOf("organizationId" to id)),
        Route("/api/v1/users/list_access_info_by_workspace_id", AuthRoleConstants.WORKSPACE_READER, mapOf("workspaceId" to id)),
        Route("/api/v1/users/list_instance_admin", AuthRoleConstants.ADMIN, emptyMap()),
        Route("/api/v1/users/get_or_create_by_auth_id", AuthRoleConstants.AUTHENTICATED_USER, mapOf("authUserId" to "auth-user-id")),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AuthRoleConstants.AUTHENTICATED_USER }

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.body.isNotEmpty() }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class UserApiAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("user-security-test-user", roles))
  }
}
