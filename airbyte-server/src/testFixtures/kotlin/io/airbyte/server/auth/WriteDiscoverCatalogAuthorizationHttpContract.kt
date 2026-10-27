/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.server.apis.controllers.SourceApiController
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.filters.SecurityFilter
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.mockk.every
import io.mockk.isMockKMock
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.spyk
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Production controller, declared rule, ownership, permission calculation, handler and catalog conversion. */
abstract class WriteDiscoverCatalogAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `human admin cannot write discovery results while the supported dataplane can`() {
    request(token = "dataplane", status = 200)
    request(token = "human-admin", status = 403)
  }

  @Test
  fun `humans application owners and unsupported token kinds cannot write even with admin claims and grants`() {
    for (token in listOf("human", "application-owner", "human-admin", "embedded", "workload", "missing-type", "unknown-type", "non-string-type")) {
      request(token = token, status = 403, grant = Permission.PermissionType.INSTANCE_ADMIN, grantScope = "global")
    }
  }

  @Test
  fun `supported accounts need a saved dataplane action on the actual source owner`() {
    for (token in listOf("dataplane", "service-account")) {
      for (scope in listOf("workspace", "organization", "global")) request(token = token, status = 200, grantScope = scope)
      request(token = token, status = 200, grant = Permission.PermissionType.INSTANCE_ADMIN, grantScope = "global")
      request(token = token, status = 403, granted = false)
      request(token = token, status = 403, grantScope = "other-workspace")
      request(token = token, status = 403, grantScope = "other-organization")
      request(token = token, status = 403, grant = Permission.PermissionType.WORKSPACE_EDITOR, grantScope = "workspace")
      request(token = token, status = 403, grant = Permission.PermissionType.WORKSPACE_READER, grantScope = "workspace")
    }
  }

  @Test
  fun `internal clients explicitly retain global privilege and token workspace scope still applies`() {
    request(token = "internal", status = 200, granted = false)
    request(token = "scoped", status = 200)
    request(token = "other-scope", status = 403, grantScope = "global")
    request(token = "internal-other-scope", status = 403, granted = false)
  }

  @Test
  fun `binding rejects missing null and malformed source before saved metadata or writes`() {
    for (body in listOf(
      "{}",
      "null",
      "",
      "{",
      """{"catalog":{"streams":[]}}""",
      """{"sourceId":null,"catalog":{"streams":[]}}""",
      """{"sourceId":"bad","catalog":{"streams":[]}}""",
    )) {
      request(status = 400, body = body)
    }
  }

  @Test
  fun `missing source destination as source and broken ownership graph return not found`() {
    for (missing in listOf("source", "workspace", "organization", "mismatched-workspace")) request(status = 404, missing = missing)
    request(status = 404, source = DESTINATION)
    request(status = 404, source = UUID(0, 0))
  }

  @Test
  fun `ownership and permission dependency failures preserve server errors and never write`() {
    for (failure in listOf("source", "workspace", "permission", "unexpected-source")) request(status = 500, failure = failure)
  }

  @Test
  fun `no auth still requires a saved source and owner but skips caller and permission checks`() {
    request(status = 200, security = false, token = "", granted = false)
    request(status = 404, security = false, token = "", missing = "source")
    request(status = 404, security = false, token = "", source = DESTINATION)
    request(status = 400, security = false, token = "", body = "{}")
  }

  @Test
  fun `authentication is required`() {
    request(status = 401, token = "")
  }

  @Test
  fun `earlier allowed security result cannot bypass worker permission or scope checks with valid auth`() {
    request(status = 403, token = "human-admin", earlierAllow = true)
    request(status = 403, granted = false, earlierAllow = true)
    request(status = 403, token = "other-scope", earlierAllow = true)
    request(status = 404, source = DESTINATION, earlierAllow = true)
  }

  private fun request(
    status: Int,
    token: String = "dataplane",
    source: UUID = SOURCE,
    body: String = catalogBody(source),
    missing: String = "",
    failure: String = "",
    grant: Permission.PermissionType = Permission.PermissionType.DATAPLANE,
    granted: Boolean = true,
    grantScope: String = "organization",
    security: Boolean = true,
    earlierAllow: Boolean = false,
  ) {
    withCatalogServer(security = security, earlierAllow = earlierAllow, configure = { fixture ->
      val permission = Permission().withPermissionType(grant)
      when (grantScope) {
        "workspace" -> permission.withWorkspaceId(WORKSPACE)
        "organization" -> permission.withOrganizationId(ORGANIZATION)
        "other-workspace" -> permission.withWorkspaceId(OTHER_WORKSPACE)
        "other-organization" -> permission.withOrganizationId(OTHER_ORGANIZATION)
      }
      val grants = if (granted) listOf(permission) else emptyList()
      every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns grants
      every { fixture.permissions.getPermissionsByAuthUserId(any()) } returns grants
      if (missing == "source") every { fixture.actors.findSourceWorkspaceId(SOURCE) } returns Optional.empty()
      if (missing ==
        "workspace"
      ) {
        every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } throws
          ConfigNotFoundException(ConfigNotFoundType.STANDARD_WORKSPACE, WORKSPACE)
      }
      if (missing ==
        "organization"
      ) {
        every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
          StandardWorkspace().withWorkspaceId(WORKSPACE)
      }
      if (missing ==
        "mismatched-workspace"
      ) {
        every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
          StandardWorkspace().withWorkspaceId(OTHER_WORKSPACE).withOrganizationId(ORGANIZATION)
      }
      if (failure == "source") every { fixture.actors.findSourceWorkspaceId(SOURCE) } throws IOException("source unavailable")
      if (failure == "unexpected-source") every { fixture.actors.findSourceWorkspaceId(SOURCE) } throws IllegalStateException("source failed")
      if (failure ==
        "workspace"
      ) {
        every { fixture.workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } throws IOException("workspace unavailable")
      }
      if (failure ==
        "permission"
      ) {
        every { fixture.permissions.getPermissionsByServiceAccountId(ACCOUNT) } throws IOException("permissions unavailable")
      }
    }) { fixture ->
      val response = post(fixture.server, "/api/v1/sources/write_discover_catalog_result", body, token)
      assertEquals(status, response.statusCode(), response.body())
      if (earlierAllow) {
        assertTrue(
          fixture.context
            .getBean(WriteDiscoverCatalogEarlierAllow::class.java)
            .calls
            .get() > 0,
        )
      }
      if (!security) {
        verify(exactly = 0) { fixture.permissions.getPermissionsByAuthUserId(any()) }
        verify(exactly = 0) { fixture.permissions.getPermissionsByServiceAccountId(any()) }
      }
      if (status == 400 || status == 401) verify(exactly = 0) { fixture.actors.findSourceWorkspaceId(any()) }
      if (status == 200) {
        assertEquals(CATALOG.toString(), Jsons.deserialize(response.body())["catalogId"].asText())
        verify(exactly = 1) { fixture.handler.writeDiscoverCatalogResult(any()) }
        verify(exactly = 1) {
          fixture.catalogs.writeActorCatalogWithFetchEvent(match<AirbyteCatalog> { it.streams.isEmpty() }, SOURCE, "1.2.3", "config-hash")
        }
      } else {
        verify(exactly = 0) { fixture.handler.writeDiscoverCatalogResult(any()) }
        verify(exactly = 0) { fixture.catalogs.writeActorCatalogWithFetchEvent(any<AirbyteCatalog>(), any(), any(), any()) }
      }
    }
  }

  protected fun withCatalogServer(
    security: Boolean = true,
    earlierAllow: Boolean = false,
    signed: Boolean = false,
    configure: (CatalogFixture) -> Unit = {},
    action: (CatalogFixture) -> Unit,
  ) {
    catalogContext(cloud, security, earlierAllow, signed).use { context ->
      val catalogs = mockk<CatalogService>()
      every { catalogs.writeActorCatalogWithFetchEvent(any<AirbyteCatalog>(), SOURCE, "1.2.3", "config-hash") } returns CATALOG
      val permissions = mockk<PermissionService>()
      every { permissions.getPermissionsByServiceAccountId(ACCOUNT) } returns
        listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withOrganizationId(ORGANIZATION))
      every { permissions.getPermissionsByAuthUserId(any()) } returns
        listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))
      val workspaces = mockk<WorkspaceService>()
      every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
        StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(ORGANIZATION)
      val actors = mockk<ActorRepository>()
      every { actors.findSourceWorkspaceId(any()) } returns Optional.empty()
      every { actors.findSourceWorkspaceId(SOURCE) } returns Optional.of(WORKSPACE)
      val data =
        AuthorizationDataService(
          workspaces = workspaces,
          permissions = permissions,
          actors = mockk(),
          connections = mockk(),
          operations = mockk(),
          jobs = mockk(),
          organizations = mockk(),
          dataplanes = mockk(),
          dataplaneGroups = mockk(),
          groups = mockk(),
          verifications = mockk(),
          secretStorages = mockk(),
          tags = mockk(),
          projects = mockk(),
          commands = mockk(),
          users = mockk(),
          invitations = mockk(),
          privateLinks = mockk(),
          connectionEvents = mockk(),
          actorRepository = actors,
          attempts = mockk(),
        )
      val converterConstructor = CatalogConverter::class.java.declaredConstructors.single { !it.isSynthetic }
      val converter =
        converterConstructor.newInstance(
          mockkClass(converterConstructor.parameterTypes[0].kotlin),
          emptyList<Any>(),
        ) as CatalogConverter
      val constructor = SourceHandler::class.java.declaredConstructors.single { !it.isSynthetic }
      val handler =
        spyk(
          constructor.newInstance(
            *constructor.parameters
              .map { parameter ->
                when (parameter.type) {
                  CatalogService::class.java -> catalogs
                  CatalogConverter::class.java -> converter
                  else -> mockkClass(parameter.type.kotlin, relaxed = true)
                }
              }.toTypedArray(),
          ) as SourceHandler,
        )
      val controller = if (cloud) Class.forName("io.airbyte.server.wrapped.apis.SourceApiWrappedController") else SourceApiController::class.java
      for (type in listOf(controller, SourceApiController::class.java).distinct()) {
        for (parameter in type.declaredConstructors.single { !it.isSynthetic }.parameters) {
          @Suppress("UNCHECKED_CAST")
          val dependency = parameter.type as Class<Any>
          // The Cloud constructor uses the same mapper as HTTP body binding.
          if (dependency != SourceHandler::class.java && dependency != ObjectMapper::class.java) {
            context.registerSingleton(dependency, mockkClass(dependency.kotlin, relaxed = true))
          }
        }
      }
      context.registerSingleton(SourceHandler::class.java, handler)
      context.registerSingleton(ActorRepository::class.java, actors)
      context.registerSingleton(WorkspaceService::class.java, workspaces)
      context.registerSingleton(PermissionService::class.java, permissions)
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(
        OperationPermissions::class.java,
        OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
      )
      val fixture = CatalogFixture(context, catalogs, handler, actors, workspaces, permissions)
      configure(fixture)
      context.start()
      if (cloud) assertFalse(isMockKMock(context.getBean(ObjectMapper::class.java)), "Cloud HTTP binding must use the real JSON mapper")
      assertEquals(security, context.containsBean(SecurityFilter::class.java))
      assertTrue(context.containsBean(UncaughtExceptionHandler::class.java))
      assertTrue(controller.isInstance(context.getBean(SourceApiController::class.java)))
      assertEquals(
        SourceApiController::class.java,
        controller.getMethod("writeDiscoverCatalogResult", SourceDiscoverSchemaWriteRequestBody::class.java).declaringClass,
      )
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        fixture.server = server
        action(fixture)
      }
    }
  }

  protected class CatalogFixture(
    val context: ApplicationContext,
    val catalogs: CatalogService,
    val handler: SourceHandler,
    val actors: ActorRepository,
    val workspaces: WorkspaceService,
    val permissions: PermissionService,
  ) {
    lateinit var server: EmbeddedServer
  }

  protected fun post(
    server: EmbeddedServer,
    path: String,
    body: String,
    token: String = "",
  ): HttpResponse<String> =
    HttpClient.newHttpClient().use { client ->
      val builder =
        java.net.http.HttpRequest
          .newBuilder(URI("${server.url}$path"))
          .header("Content-Type", "application/json")
      if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
      client.send(
        builder
          .POST(
            java.net.http.HttpRequest.BodyPublishers
              .ofString(body),
          ).build(),
        HttpResponse.BodyHandlers.ofString(),
      )
    }

  companion object {
    val SOURCE: UUID = UUID(0, 1)
    val WORKSPACE: UUID = UUID(0, 2)
    val ORGANIZATION: UUID = UUID(0, 3)
    val CATALOG: UUID = UUID(0, 4)
    val ACCOUNT: UUID = UUID(0, 5)
    val OTHER_WORKSPACE: UUID = UUID(0, 6)
    val OTHER_ORGANIZATION: UUID = UUID(0, 7)
    val DESTINATION: UUID = UUID(0, 8)

    fun catalogBody(source: UUID = SOURCE): String =
      """{"sourceId":"$source","catalog":{"streams":[]},"connectorVersion":"1.2.3","configurationHash":"config-hash"}"""
  }
}

private fun catalogContext(
  cloud: Boolean,
  security: Boolean,
  earlierAllow: Boolean,
  signed: Boolean,
): ApplicationContext {
  val allowed =
    listOf(
      "io.airbyte.server.apis.controllers.SourceApiController",
      "io.airbyte.server.wrapped.apis.SourceApiWrappedController",
      "io.airbyte.server.auth.WriteDiscoverCatalog",
      "io.airbyte.server.auth.AuthorizedRequest",
      "io.airbyte.server.auth.AuthorizationInterceptor",
      "io.airbyte.server.auth.RouteAccess",
      "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
      "io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler",
    ) +
      if (signed) {
        listOf(
          "io.airbyte.server.apis.controllers.DataplaneController",
          "io.airbyte.server.apis.controllers.ServiceAccountsController",
          "io.airbyte.commons.server.authorization.AuthenticationFactory",
        )
      } else {
        emptyList()
      }
  val isolated =
    object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
      override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

      override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

      override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
    }
  return ApplicationContext
    .builder()
    .enableDefaultPropertySources(false)
    .deduceEnvironment(false)
    .environmentPropertySource(false)
    .environments(if (cloud) "cloud" else "test")
    .beanConfigurations(isolated)
    .properties(
      mapOf(
        "spec.name" to "WriteDiscoverCatalogAuthorizationHttpTest",
        "test.catalog.signed" to signed,
        "test.catalog.earlier-allow" to earlierAllow,
        "micronaut.security.enabled" to security,
        "micronaut.security.filter.enabled" to security,
        "micronaut.security.token.jwt.enabled" to signed,
        "micronaut.security.token.jwt.signatures.secret.generator.secret" to "catalog-test-signature-secret-long-enough-for-hs256",
        "micronaut.server.port" to -1,
        "micronaut.metrics.enabled" to false,
      ),
    ).build()
}

@Singleton
@Requires(property = "spec.name", value = "WriteDiscoverCatalogAuthorizationHttpTest")
@Requires(property = "test.catalog.signed", value = "false")
class WriteDiscoverCatalogAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes: MutableMap<String, Any> =
      when (token) {
        "dataplane", "scoped", "other-scope" -> mapOf(TokenType.DATAPLANE_V1.toClaim())
        "service-account" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim())
        "internal", "internal-other-scope" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        "human", "application-owner", "human-admin" -> mapOf(TokenType.USER.toClaim())
        "embedded" -> mapOf(TokenType.EMBEDDED_V1.toClaim())
        "workload" -> mapOf(TokenType.WORKLOAD_API.toClaim())
        "missing-type" -> emptyMap()
        "unknown-type" -> mapOf("typ" to "io.airbyte.auth.unknown")
        "non-string-type" -> mapOf("typ" to 7)
        else -> return Publishers.empty()
      }.toMutableMap()
    if (token in
      listOf("scoped", "other-scope", "internal-other-scope")
    ) {
      attributes[TokenScopeClaim.CLAIM_ID] =
        TokenScopeClaim(
          (
            if (token ==
              "scoped"
            ) {
              WriteDiscoverCatalogAuthorizationHttpContract.WORKSPACE
            } else {
              WriteDiscoverCatalogAuthorizationHttpContract.OTHER_WORKSPACE
            }
          ).toString(),
        )
    }
    return Publishers.just(
      Authentication.build(
        WriteDiscoverCatalogAuthorizationHttpContract.ACCOUNT.toString(),
        listOf(AuthRoleConstants.AUTHENTICATED_USER, AuthRoleConstants.DATAPLANE, AuthRoleConstants.ADMIN),
        attributes,
      ),
    )
  }
}

@Singleton
@Requires(property = "spec.name", value = "WriteDiscoverCatalogAuthorizationHttpTest")
@Requires(property = "test.catalog.earlier-allow", value = "true")
class WriteDiscoverCatalogEarlierAllow : SecurityRule<HttpRequest<*>> {
  val calls = AtomicInteger()

  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> {
    calls.incrementAndGet()
    return Publishers.just(SecurityRuleResult.ALLOWED)
  }
}
