/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectionEventIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Permission
import io.airbyte.config.StandardSync
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.ConnectionTimelineEventRepository
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.impls.data.ConnectionTimelineEventServiceDataImpl
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.ConnectionApiController
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
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Exercises the served production controller, event service, handler and shared authorization. */
abstract class GetConnectionEventAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `wrong connection is rejected even with both connections allowed in one workspace`() {
    request(connection = OTHER_CONNECTION, status = 422)
  }

  @Test
  fun `matching event uses ordinary payload and user response flow`() {
    request(status = 200)
  }

  @Test
  fun `wrong connection in another workspace also rejects despite both grants`() {
    request(connection = OTHER_CONNECTION, status = 422, sameWorkspace = false)
  }

  @Test
  fun `wrong connection cannot reveal an event without access to its saved connection`() {
    request(connection = OTHER_CONNECTION, status = 403, granted = false)
    request(connection = OTHER_CONNECTION, status = 403, token = "other-scope")
    request(connection = OTHER_CONNECTION, status = 403, sameWorkspace = false, permissionWorkspace = OTHER_WORKSPACE)
  }

  @Test
  fun `binding rejects malformed null and missing identifiers before ownership reads`() {
    for (body in listOf(
      "{}",
      "null",
      "",
      "{",
      """{"connectionId":"$CONNECTION"}""",
      """{"connectionEventId":"$EVENT"}""",
      """{"connectionEventId":null,"connectionId":"$CONNECTION"}""",
      """{"connectionEventId":"$EVENT","connectionId":null}""",
      """{"connectionEventId":"invalid","connectionId":"$CONNECTION"}""",
      """{"connectionEventId":"$EVENT","connectionId":"invalid"}""",
    )) {
      request(status = 400, body = body)
    }
  }

  @Test
  fun `zero UUID follows missing event or mismatched relationship contract`() {
    request(status = 404, body = """{"connectionEventId":"${UUID(0, 0)}","connectionId":"$CONNECTION"}""")
    request(status = 422, connection = UUID(0, 0))
  }

  @Test
  fun `missing saved event connection actor workspace and organization return not found`() {
    for (missing in listOf("event", "connection", "actor", "organization")) request(status = 404, missing = missing)
  }

  @Test
  fun `workspace and organization reader and inherited grants remain supported`() {
    for (grant in listOf(
      Permission.PermissionType.WORKSPACE_EDITOR,
      Permission.PermissionType.WORKSPACE_ADMIN,
      Permission.PermissionType.ORGANIZATION_READER,
      Permission.PermissionType.ORGANIZATION_EDITOR,
      Permission.PermissionType.ORGANIZATION_ADMIN,
      Permission.PermissionType.INSTANCE_ADMIN,
    )) {
      request(status = 200, grant = grant)
    }
  }

  @Test
  fun `missing unrelated and insufficient permissions reject before business work`() {
    request(status = 403, granted = false)
    request(status = 403, permissionWorkspace = OTHER_WORKSPACE)
    request(status = 403, grant = Permission.PermissionType.ORGANIZATION_READER, permissionOrganization = OTHER_ORGANIZATION)
    request(status = 403, grant = Permission.PermissionType.DATAPLANE)
  }

  @Test
  fun `saved ownership and permission lookup failures never fall back to legacy checks`() {
    for (failure in listOf("event", "connection", "actor", "permission", "unexpected-event")) request(status = 500, failure = failure)
  }

  @Test
  fun `supported callers and scoped tokens retain their read policy`() {
    for (token in listOf("internal", "worker", "dataplane", "scoped")) request(status = 200, token = token)
    for (token in listOf("embedded", "workload", "other-scope")) request(status = 403, token = token)
    request(status = 403, token = "worker", granted = false)
    request(status = 403, token = "dataplane", granted = false)
    request(status = 403, token = "other-scope", grant = Permission.PermissionType.INSTANCE_ADMIN)
  }

  @Test
  fun `configured no auth still verifies saved relationship and resources`() {
    request(status = 200, security = false, token = "", granted = false)
    request(connection = OTHER_CONNECTION, status = 422, security = false, token = "")
    request(status = 404, security = false, token = "", missing = "event")
  }

  @Test
  fun `authentication remains required`() {
    request(status = 401, token = "")
  }

  @Test
  fun `earlier allowed security result cannot bypass declared rule with valid authentication`() {
    request(connection = OTHER_CONNECTION, status = 422, earlierAllow = true)
    request(status = 403, granted = false, earlierAllow = true)
    request(status = 403, token = "other-scope", earlierAllow = true)
  }

  private fun request(
    connection: UUID = CONNECTION,
    status: Int,
    body: String = """{"connectionEventId":"$EVENT","connectionId":"$connection"}""",
    sameWorkspace: Boolean = true,
    missing: String = "",
    failure: String = "",
    grant: Permission.PermissionType = Permission.PermissionType.WORKSPACE_READER,
    granted: Boolean = true,
    permissionWorkspace: UUID = WORKSPACE,
    permissionOrganization: UUID = ORGANIZATION,
    token: String = "valid",
    security: Boolean = true,
    earlierAllow: Boolean = false,
  ) {
    eventContext(cloud, security, earlierAllow).use { context ->
      val repository = mockk<ConnectionTimelineEventRepository>()
      every { repository.findById(EVENT) } returns
        Optional.of(
          ConnectionTimelineEvent(EVENT, CONNECTION, USER, "SYNC_SUCCEEDED", "{\"recordsLoaded\":7}", OffsetDateTime.parse("2026-10-19T00:00:00Z")),
        )
      every { repository.findConnectionIdByEventId(any()) } returns Optional.empty()
      every { repository.findConnectionIdByEventId(EVENT) } returns if (missing == "event") Optional.empty() else Optional.of(CONNECTION)
      if (failure == "event") every { repository.findConnectionIdByEventId(EVENT) } throws IOException("ownership unavailable")
      if (failure == "unexpected-event") every { repository.findConnectionIdByEventId(EVENT) } throws IllegalStateException("ownership failed")
      val events = spyk(ConnectionTimelineEventServiceDataImpl(repository, MoreMappers.initMapper()))
      val users = mockk<UserPersistence>()
      every { users.getUser(USER) } returns Optional.of(User().withUserId(USER).withName("Event author").withEmail("user@example.com"))
      val permissionHandler = mockk<io.airbyte.commons.server.handlers.PermissionHandler>()
      every { permissionHandler.isUserInstanceAdmin(USER) } returns false
      val helper = spyk(ConnectionTimelineEventHelper(emptySet(), mockk(), mockk(), permissionHandler, users, mockk(), events))
      val connections = mockk<ConnectionService>()
      every { connections.getStandardSync(CONNECTION) } returns
        StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE).withDestinationId(DESTINATION)
      every { connections.getStandardSync(OTHER_CONNECTION) } returns
        StandardSync().withConnectionId(OTHER_CONNECTION).withSourceId(OTHER_SOURCE).withDestinationId(OTHER_DESTINATION)
      if (missing ==
        "connection"
      ) {
        every { connections.getStandardSync(CONNECTION) } throws ConfigNotFoundException(ConfigNotFoundType.STANDARD_SYNC, CONNECTION)
      }
      if (failure == "connection") every { connections.getStandardSync(CONNECTION) } throws IOException("connection unavailable")
      val actors = mockk<ActorDefinitionService>()
      for (actor in listOf(SOURCE, DESTINATION, OTHER_SOURCE, OTHER_DESTINATION)) {
        val workspace = if (!sameWorkspace && actor in listOf(OTHER_SOURCE, OTHER_DESTINATION)) OTHER_WORKSPACE else WORKSPACE
        every { actors.getIdsForActors(listOf(actor)) } returns listOf(ActorWorkspaceOrganizationIds(actor, workspace, ORGANIZATION))
      }
      if (missing == "actor") every { actors.getIdsForActors(listOf(SOURCE)) } returns emptyList()
      if (missing ==
        "organization"
      ) {
        every { actors.getIdsForActors(listOf(SOURCE)) } returns listOf(ActorWorkspaceOrganizationIds(SOURCE, WORKSPACE, null))
      }
      if (failure == "actor") every { actors.getIdsForActors(any()) } throws IOException("actor unavailable")
      val permissions = mockk<PermissionService>()
      val grants =
        if (!granted) {
          emptyList()
        } else {
          when {
            grant.name.startsWith("ORGANIZATION") -> listOf(Permission().withOrganizationId(permissionOrganization).withPermissionType(grant))
            grant == Permission.PermissionType.INSTANCE_ADMIN || grant == Permission.PermissionType.DATAPLANE ->
              listOf(
                Permission().withPermissionType(grant),
              )
            else -> listOf(permissionWorkspace, OTHER_WORKSPACE).distinct().map { Permission().withWorkspaceId(it).withPermissionType(grant) }
          }
        }
      every { permissions.getPermissionsByAuthUserId(any()) } returns grants
      every { permissions.getPermissionsByServiceAccountId(any()) } returns grants
      if (failure == "permission") every { permissions.getPermissionsByAuthUserId(any()) } throws IOException("permissions unavailable")
      val data =
        AuthorizationDataService(
          workspaces = mockk(),
          permissions = permissions,
          actors = actors,
          connections = connections,
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
          users = users,
          invitations = mockk(),
          privateLinks = mockk(),
          connectionEvents = events,
          actorRepository = mockk(),
          attempts = mockk(),
          entitlements = mockk(),
          featureFlags = mockk(),
        )
      val handlerConstructor = ConnectionsHandler::class.java.declaredConstructors.single { !it.isSynthetic }
      val handler =
        spyk(
          handlerConstructor.newInstance(
            *handlerConstructor.parameters
              .map { parameter ->
                when (parameter.type) {
                  ConnectionTimelineEventService::class.java -> events
                  ConnectionTimelineEventHelper::class.java -> helper
                  ConnectionService::class.java -> connections
                  else -> mockkClass(parameter.type.kotlin, relaxed = true)
                }
              }.toTypedArray(),
          ) as ConnectionsHandler,
        )
      val controller =
        if (cloud) {
          Class.forName(
            "io.airbyte.server.wrapped.apis.ConnectionApiWrappedController",
          )
        } else {
          ConnectionApiController::class.java
        }
      for (type in listOf(controller, ConnectionApiController::class.java).distinct()) {
        for (parameter in type.declaredConstructors.single { !it.isSynthetic }.parameters) {
          @Suppress("UNCHECKED_CAST")
          val dependency = parameter.type as Class<Any>
          if (dependency != ConnectionsHandler::class.java && dependency != ConnectionService::class.java) {
            context.registerSingleton(dependency, mockkClass(dependency.kotlin, relaxed = true))
          }
        }
      }
      context.registerSingleton(ConnectionsHandler::class.java, handler)
      context.registerSingleton(ConnectionService::class.java, connections)
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(
        OperationPermissions::class.java,
        OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
      )
      registerSiblingOperationRules(context, controller, setOf(GetConnectionEventRule::class.java))
      context.start()
      assertEquals(security, context.containsBean(SecurityFilter::class.java))
      assertTrue(context.containsBean(UncaughtExceptionHandler::class.java))
      assertTrue(controller.isInstance(context.getBean(ConnectionApiController::class.java)))
      assertEquals(
        ConnectionApiController::class.java,
        controller.getMethod("getConnectionEvent", ConnectionEventIdRequestBody::class.java).declaringClass,
      )
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val builder =
            java.net.http.HttpRequest
              .newBuilder(URI("${server.url}/api/v1/connections/events/get"))
              .header("Content-Type", "application/json")
          if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
          val response =
            client.send(
              builder
                .POST(
                  java.net.http.HttpRequest.BodyPublishers
                    .ofString(body),
                ).build(),
              HttpResponse.BodyHandlers.ofString(),
            )
          assertEquals(status, response.statusCode(), "${server.url} request=$body response=${response.body()}")
          if (earlierAllow) assertTrue(context.getBean(GetConnectionEventEarlierAllow::class.java).calls.get() > 0)
          if (!security) {
            verify(exactly = 0) { permissions.getPermissionsByAuthUserId(any()) }
            verify(exactly = 0) { permissions.getPermissionsByServiceAccountId(any()) }
          }
          if (status == 400 || status == 401) {
            verify(exactly = 0) { repository.findConnectionIdByEventId(any()) }
          }
          if (status == 422) {
            verify(exactly = 0) { connections.getStandardSync(OTHER_CONNECTION) }
          }
          if (status == 200) {
            val body = Jsons.deserialize(response.body())
            assertEquals(EVENT.toString(), body["id"].asText())
            assertEquals(CONNECTION.toString(), body["connectionId"].asText())
            assertEquals(7, body["summary"]["recordsLoaded"].asInt())
            assertEquals("Event author", body["user"]["name"].asText())
            verify(exactly = 1) { handler.getConnectionEvent(any()) }
            verify(exactly = 1) { events.getEvent(EVENT) }
            verify(exactly = 1) { users.getUser(USER) }
          } else {
            verify(exactly = 0) { handler.getConnectionEvent(any()) }
            verify(exactly = 0) { events.getEvent(any()) }
            verify(exactly = 0) { repository.findById(any()) }
            verify(exactly = 0) { helper.getUserReadInConnectionEvent(any(), any()) }
            verify(exactly = 0) { users.getUser(any()) }
            assertFalse(response.body().contains(CONNECTION.toString()))
            assertFalse(response.body().contains(WORKSPACE.toString()))
            assertFalse(response.body().contains(ORGANIZATION.toString()))
          }
        }
      }
    }
  }

  companion object {
    val EVENT: UUID = UUID(0, 1)
    val CONNECTION: UUID = UUID(0, 2)
    val OTHER_CONNECTION: UUID = UUID(0, 3)
    val WORKSPACE: UUID = UUID(0, 4)
    val ORGANIZATION: UUID = UUID(0, 5)
    val SOURCE: UUID = UUID(0, 6)
    val DESTINATION: UUID = UUID(0, 7)
    val USER: UUID = UUID(0, 8)
    val OTHER_WORKSPACE: UUID = UUID(0, 10)
    val OTHER_ORGANIZATION: UUID = UUID(0, 11)
    val OTHER_SOURCE: UUID = UUID(0, 12)
    val OTHER_DESTINATION: UUID = UUID(0, 13)
  }
}

private fun eventContext(
  cloud: Boolean,
  security: Boolean,
  earlierAllow: Boolean,
): ApplicationContext {
  val allowed =
    listOf(
      "io.airbyte.server.apis.controllers.ConnectionApiController",
      "io.airbyte.server.wrapped.apis.ConnectionApiWrappedController",
      "io.airbyte.server.auth.GetConnectionEvent",
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
  return ApplicationContext
    .builder()
    .enableDefaultPropertySources(false)
    .deduceEnvironment(false)
    .environmentPropertySource(false)
    .environments(if (cloud) "cloud" else "test")
    .beanConfigurations(isolated)
    .properties(
      mapOf(
        "spec.name" to "GetConnectionEventAuthorizationHttpTest",
        "micronaut.security.enabled" to security,
        "micronaut.security.filter.enabled" to security,
        "test.event.earlier-allow" to earlierAllow,
        "micronaut.server.port" to -1,
        "micronaut.metrics.enabled" to false,
      ),
    ).build()
}

@Singleton
@Requires(property = "spec.name", value = "GetConnectionEventAuthorizationHttpTest")
class GetConnectionEventAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes =
      when (token) {
        "internal" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        "worker" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim())
        "dataplane" -> mapOf(TokenType.DATAPLANE_V1.toClaim())
        "embedded" -> mapOf(TokenType.EMBEDDED_V1.toClaim())
        "workload" -> mapOf(TokenType.WORKLOAD_API.toClaim())
        "scoped" -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(GetConnectionEventAuthorizationHttpContract.WORKSPACE.toString()))
        "other-scope" -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(GetConnectionEventAuthorizationHttpContract.OTHER_WORKSPACE.toString()))
        "valid" -> emptyMap()
        else -> return Publishers.empty()
      }
    val subject = if (token in listOf("worker", "dataplane")) UUID(0, 9).toString() else "caller"
    return Publishers.just(Authentication.build(subject, listOf(AuthRoleConstants.WORKSPACE_READER), attributes))
  }
}

@Singleton
@Requires(property = "spec.name", value = "GetConnectionEventAuthorizationHttpTest")
@Requires(property = "test.event.earlier-allow", value = "true")
class GetConnectionEventEarlierAllow : SecurityRule<HttpRequest<*>> {
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
