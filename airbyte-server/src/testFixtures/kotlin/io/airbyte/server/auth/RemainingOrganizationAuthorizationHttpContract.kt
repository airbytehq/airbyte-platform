/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.OrganizationAccessService
import io.airbyte.server.apis.controllers.EntitlementsApiController
import io.airbyte.server.helpers.OrganizationAccessAuthorizationHelper
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID

/** Real production entrypoints and shared policies; ownership and business responses are isolated. */
abstract class RemainingOrganizationAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `entitlement denial is 403 before business even after earlier allowed`() = request(403)

  @Test
  fun `organization member and inherited grants return entitlements`() {
    for (grant in listOf(
      Permission.PermissionType.ORGANIZATION_MEMBER,
      Permission.PermissionType.ORGANIZATION_ADMIN,
      Permission.PermissionType.INSTANCE_ADMIN,
    )) {
      request(200, grant = grant)
    }
  }

  @Test
  fun `workspace access inside the requested organization permits entitlement read`() = request(200, workspaceAccess = true)

  @Test
  fun `internal caller needs no database user for organization access`() = request(200, token = "internal")

  @Test
  fun `scoped and unsupported callers reject despite inherited administrator grants`() {
    for (token in listOf("scoped", "embedded", "workload")) request(403, token = token, grant = Permission.PermissionType.INSTANCE_ADMIN)
  }

  @Test
  fun `missing saved organization and dependency failures stop business`() {
    request(404, body = """{"organization_id":"${UUID(0, 999)}"}""")
    request(500, failure = true)
  }

  @Test
  fun `missing and null organizations reject parsed input`() {
    for (body in listOf("{}", "null", """{"organization_id":null}""", """{"organization_id":"invalid"}""")) request(400, body = body)
  }

  @Test
  fun `authentication remains required`() = request(401, token = "", earlierAllow = false)

  @Test
  fun `shared checker rejects absent principal after earlier allowed`() = request(403, token = "")

  @Test
  fun `no auth keeps ordinary behavior without permission lookups`() = request(200, security = false)

  private fun request(
    expected: Int,
    grant: Permission.PermissionType? = null,
    workspaceAccess: Boolean = false,
    token: String = "valid",
    security: Boolean = true,
    failure: Boolean = false,
    earlierAllow: Boolean = true,
    body: String = """{"organization_id":"$ORGANIZATION"}""",
  ) {
    val allowed =
      listOf(
        "io.airbyte.server.apis.controllers.EntitlementsApiController",
        "io.airbyte.server.wrapped.apis.controllers.EntitlementsWrappedController",
        "io.airbyte.server.auth.RemainingOrganization",
        "io.airbyte.server.auth.OrganizationEntitlementsRule",
        "io.airbyte.server.auth.AuthorizedRequest",
        "io.airbyte.server.auth.AuthorizationInterceptor",
        "io.airbyte.server.auth.RouteAccess",
        "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
        "io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler",
      )
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

        override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

        override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
      }
    ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .environments(if (cloud) "cloud" else "test")
      .beanConfigurations(isolated)
      .properties(
        mapOf(
          "spec.name" to "RemainingOrganizationAuthorizationHttpTest",
          "micronaut.security.enabled" to security,
          "micronaut.security.filter.enabled" to security,
          "test.organization.earlier-allow" to earlierAllow,
          "micronaut.server.port" to -1,
          "micronaut.metrics.enabled" to false,
        ),
      ).build()
      .use { context ->
        val entitlements = mockk<EntitlementService>()
        every { entitlements.getEntitlements(OrganizationId(ORGANIZATION)) } returns emptyList()
        val legacy = mockk<OrganizationAccessAuthorizationHelper>()
        every { legacy.validateOrganizationOrWorkspaceAccess(ORGANIZATION) } throws ForbiddenProblem()
        val grants = mockk<PermissionService>()
        every { grants.getPermissionsByAuthUserId("caller") } returns
          if (grant == null) emptyList() else listOf(Permission().withPermissionType(grant).withOrganizationId(ORGANIZATION))
        if (failure) every { grants.getPermissionsByAuthUserId("caller") } throws java.io.IOException("permissions unavailable")
        val data = mockk<AuthorizationDataService>()
        every { data.organization(any()) } throws AuthorizationResourceNotFoundException()
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        val access = mockk<OrganizationAccessService>()
        every { access.hasAccessibleWorkspace("caller", ORGANIZATION) } returns workspaceAccess
        context.registerSingleton(EntitlementService::class.java, entitlements)
        context.registerSingleton(OrganizationAccessAuthorizationHelper::class.java, legacy)
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(OrganizationAccessService::class.java, access)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
        if (cloud) {
          val helper = Class.forName("io.airbyte.server.wrapped.apis.ApiHelper")
          @Suppress("UNCHECKED_CAST")
          context.registerSingleton(helper as Class<Any>, io.mockk.mockkClass(helper.kotlin, relaxed = true))
        }
        val controller =
          if (cloud) {
            Class.forName("io.airbyte.server.wrapped.apis.controllers.EntitlementsWrappedController")
          } else {
            EntitlementsApiController::class.java
          }
        registerSiblingOperationRules(context, controller, setOf(OrganizationEntitlementsRule::class.java))
        context.start()
        org.junit.jupiter.api.Assertions
          .assertFalse(io.mockk.isMockKMock(context.getBean(OrganizationEntitlementsRule::class.java)))
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          HttpClient.newHttpClient().use { client ->
            val request =
              java.net.http.HttpRequest
                .newBuilder(URI("${server.url}/api/v1/entitlements/get_entitlements"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer $token")
                .POST(
                  java.net.http.HttpRequest.BodyPublishers
                    .ofString(body),
                ).build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(expected, response.statusCode(), response.body())
            verify(exactly = if (expected == 200) 1 else 0) { entitlements.getEntitlements(any()) }
            verify(exactly = 0) { legacy.validateOrganizationOrWorkspaceAccess(any()) }
            if (expected == 200) assertEquals("{\"entitlements\":[]}", response.body())
            if (!security) verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
            assertEquals(true, context.containsBean(EntitlementsApiController::class.java))
          }
        }
      }
  }

  companion object {
    val ORGANIZATION: UUID = UUID(0, 1512)
  }
}

@Singleton
@Requires(property = "spec.name", value = "RemainingOrganizationAuthorizationHttpTest")
class RemainingOrganizationAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes =
      when (token) {
        "valid" -> emptyMap()
        "internal" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        "embedded" -> mapOf(TokenType.EMBEDDED_V1.toClaim())
        "workload" -> mapOf(TokenType.WORKLOAD_API.toClaim())
        "scoped" -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(UUID(0, 1).toString()))
        else -> return Publishers.empty()
      }
    return Publishers.just(Authentication.build("caller", listOf(AuthRoleConstants.ADMIN), attributes))
  }
}

@Singleton
@Requires(property = "spec.name", value = "RemainingOrganizationAuthorizationHttpTest")
@Requires(property = "test.organization.earlier-allow", value = "true")
class RemainingOrganizationEarlierAllow : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> = Publishers.just(SecurityRuleResult.ALLOWED)
}
