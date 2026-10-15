/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.server.handlers.UserInvitationHandler
import io.airbyte.server.helpers.UserInvitationAuthorizationHelper
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
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

private const val SPEC = "PermissionAndUserInvitationApiSecurityTest"
private const val TEST_ROLES_HEADER = "X-Test-Roles"

/**
 * Exercises every permission and user-invitation route over HTTP with security enabled, at the exact
 * path clients call. Switching a controller between generated interfaces can change which routes
 * Micronaut registers (an unmatched route surfaces as a generic 403), so each route must be reachable
 * with its role and rejected without it.
 */
@MicronautTest(environments = ["test"], rebuildContext = true)
@Property(name = "spec.name", value = SPEC)
@Property(name = "micronaut.security.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.enabled", value = "true")
@Property(name = "micronaut.security.token.jwt.signatures.secret.generator.secret", value = "test-jwt-signature-secret-that-is-long-enough-for-hs256")
internal class PermissionAndUserInvitationApiSecurityTest {
  @Requires(property = "spec.name", value = SPEC)
  @Factory
  class TestFactory {
    @Singleton
    @Primary
    fun permissionHandler(): PermissionHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun userInvitationHandler(): UserInvitationHandler = mockk(relaxed = true)

    @Singleton
    @Primary
    fun userInvitationAuthorizationHelper(): UserInvitationAuthorizationHelper = mockk(relaxed = true)

    @Singleton
    @Primary
    fun currentUserService(): CurrentUserService = mockk(relaxed = true)
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
    val status = statusOf(route.request(body = emptyMap<String, Any>()), route.allowedRole)
    assertThat(status.code)
      .withFailMessage { "${route.path} with an empty body returned $status" }
      .isBetween(400, 499)
      .isNotIn(HttpStatus.UNAUTHORIZED.code, HttpStatus.FORBIDDEN.code, HttpStatus.NOT_FOUND.code)
  }

  private fun statusOf(
    request: io.micronaut.http.MutableHttpRequest<*>,
    role: String,
  ): HttpStatus =
    try {
      client.toBlocking().exchange(request.header(TEST_ROLES_HEADER, role), Any::class.java).status
    } catch (e: HttpClientResponseException) {
      e.status
    }

  data class Route(
    val method: String,
    val path: String,
    val allowedRole: String,
    val body: Map<String, Any>?,
  ) {
    fun request(body: Map<String, Any>? = this.body): io.micronaut.http.MutableHttpRequest<*> =
      if (method == "GET") {
        HttpRequest.GET<Any>(path)
      } else {
        HttpRequest.POST(path, body ?: emptyMap<String, Any>()).contentType(MediaType.APPLICATION_JSON_TYPE)
      }

    override fun toString() = "$method $path"
  }

  companion object {
    private val id = UUID.randomUUID().toString()
    private const val AUTHENTICATED = AuthRoleConstants.AUTHENTICATED_USER

    private val routes =
      listOf(
        Route(
          "POST",
          "/api/v1/permissions/create",
          AuthRoleConstants.ORGANIZATION_ADMIN,
          mapOf("permissionType" to "workspace_admin", "userId" to id, "workspaceId" to id),
        ),
        Route("POST", "/api/v1/permissions/get", AuthRoleConstants.ORGANIZATION_READER, mapOf("permissionId" to id)),
        Route(
          "POST",
          "/api/v1/permissions/update",
          AuthRoleConstants.ORGANIZATION_ADMIN,
          mapOf("permissionId" to id, "permissionType" to "workspace_reader"),
        ),
        Route("POST", "/api/v1/permissions/delete", AuthRoleConstants.ORGANIZATION_ADMIN, mapOf("permissionId" to id)),
        Route(
          "POST",
          "/api/v1/permissions/delete_user_from_workspace",
          AuthRoleConstants.ORGANIZATION_ADMIN,
          mapOf("userId" to id, "workspaceId" to id),
        ),
        Route("POST", "/api/v1/permissions/list_by_user", AuthRoleConstants.ADMIN, mapOf("userId" to id)),
        Route(
          "POST",
          "/api/v1/permissions/check",
          AuthRoleConstants.ADMIN,
          mapOf("permissionType" to "workspace_admin", "userId" to id, "workspaceId" to id),
        ),
        Route(
          "POST",
          "/api/v1/permissions/check_multiple_workspaces",
          AuthRoleConstants.ADMIN,
          mapOf("permissionType" to "workspace_admin", "userId" to id, "workspaceIds" to listOf(id)),
        ),
        Route("GET", "/api/v1/user_invitations/by_code/some-code", AUTHENTICATED, null),
        Route(
          "POST",
          "/api/v1/user_invitations/list_pending",
          AuthRoleConstants.WORKSPACE_READER,
          mapOf("scopeType" to "workspace", "scopeId" to id),
        ),
        Route(
          "POST",
          "/api/v1/user_invitations/create",
          AuthRoleConstants.WORKSPACE_ADMIN,
          mapOf("invitedEmail" to "a@example.com", "permissionType" to "workspace_admin", "scopeType" to "workspace", "scopeId" to id),
        ),
        Route("POST", "/api/v1/user_invitations/accept", AUTHENTICATED, mapOf("inviteCode" to "some-code")),
        Route("POST", "/api/v1/user_invitations/decline", AUTHENTICATED, mapOf("inviteCode" to "some-code")),
        Route("POST", "/api/v1/user_invitations/cancel", AUTHENTICATED, mapOf("id" to id)),
      )

    @JvmStatic
    fun routes() = routes

    @JvmStatic
    fun securedRoutes() = routes.filter { it.allowedRole != AUTHENTICATED }

    @JvmStatic
    fun routesWithRequiredFields() = routes.filter { it.body != null && it.path != "/api/v1/user_invitations/cancel" }
  }
}

@Requires(property = "spec.name", value = SPEC)
@Singleton
class PermissionAndUserInvitationAuthenticationFetcher : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val roles = listOfNotNull(AuthRoleConstants.AUTHENTICATED_USER, request.headers[TEST_ROLES_HEADER]).distinct()
    return Flux.just(Authentication.build("permission-security-test-user", roles))
  }
}
