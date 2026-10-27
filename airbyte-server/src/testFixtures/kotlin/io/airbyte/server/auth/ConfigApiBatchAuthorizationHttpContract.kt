/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.core.io.service.SoftServiceLoader
import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Controller
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.micronaut.web.router.Router
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.reactivestreams.Publisher
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID

/** Sends each operation's actual parsed input shape through the served production controller and rule. */
abstract class ConfigApiBatchAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `every batch operation rejects a different token workspace before business work even after ALLOWED`() {
    val references = SoftServiceLoader.load(BeanDefinitionReference::class.java).collectAll()
    val controllerNames =
      setOf(
        "SourceApiController",
        "DestinationApiController",
        "ConnectionApiController",
        "FusionSourceEnablementApiController",
        "FusionDestinationEnablementApiController",
        "FusionConnectionEnablementApiController",
        "FusionConnectionBackfillApiController",
        "FusionEnablementResolverApiController",
        "FusionSearchApiController",
        "SourceApiWrappedController",
        "DestinationApiWrappedController",
        "ConnectionApiWrappedController",
        "FusionSourceEnablementController",
        "FusionDestinationEnablementController",
        "FusionConnectionEnablementController",
        "FusionConnectionBackfillController",
        "FusionEnablementResolverController",
        "FusionSearchController",
      )
    val controllers =
      references
        .map { it.beanType }
        .filter {
          it.simpleName in controllerNames &&
            it.isAnnotationPresent(
              Controller::class.java,
            )
        }.toSet()
    val rules = references.filter { OperationRule::class.java.isAssignableFrom(it.beanType) }.map { it.beanType.name }
    val allowed =
      controllers.map { it.name } + rules +
        listOf(
          "io.airbyte.server.auth.AuthorizedRequest",
          "io.airbyte.server.auth.AuthorizationInterceptor",
          "io.airbyte.server.auth.RouteAccess",
          "io.airbyte.server.auth.ConfigApiBatchAuthentication",
          "io.airbyte.server.auth.ConfigApiBatchEarlierAllow",
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
          "spec.name" to "ConfigApiBatchAuthorizationHttpTest",
          "micronaut.security.enabled" to true,
          "micronaut.security.filter.enabled" to true,
          "micronaut.server.port" to -1,
          "micronaut.metrics.enabled" to false,
          "micronaut.executors.io.type" to "cached",
          "micronaut.executors.scheduler.type" to "cached",
          "micronaut.executors.sonar-forwarding.type" to "cached",
        ),
      ).build()
      .use { context ->
        val business = mutableListOf<Any>()
        controllers
          .flatMap { it.declaredConstructors.filterNot { it.isSynthetic }.flatMap { it.parameters.toList() } }
          .distinctBy { it.type to it.getAnnotation(Named::class.java)?.value }
          .forEach { parameter ->
            @Suppress("UNCHECKED_CAST")
            val type = parameter.type as Class<Any>
            val instance =
              when (type) {
                ObjectMapper::class.java -> MoreMappers.initMapper()
                else ->
                  if (type.name == "io.airbyte.server.wrapped.apis.ApiHelper") {
                    type.getConstructor().newInstance()
                  } else {
                    mockkClass(type.kotlin, relaxed = true).also(business::add)
                  }
              }
            val name = parameter.getAnnotation(Named::class.java)?.value
            if (name == null) context.registerSingleton(type, instance) else context.registerSingleton(type, instance, Qualifiers.byName(name))
          }
        val data = mockk<AuthorizationDataService>()
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns owner
        every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns owner
        every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
        every { data.savedConnectionReferences(CONNECTION) } returns
          SavedConnectionReferences(SOURCE, DESTINATION, Optional.empty(), Optional.empty(), listOf(OPERATION))
        every { data.connectionWorkspaceAndOrganization(CONNECTION) } returns owner
        every { data.connectionEventConnectionId(EVENT) } returns CONNECTION
        every { data.operationWorkspaceAndOrganization(OPERATION) } returns owner
        every { data.destinationCatalogForValidation(CONNECTION, WORKSPACE, any()) } returns Optional.empty()
        every { data.actorConnectionIds(any(), WORKSPACE, any()) } returns emptyList()
        every { data.tagWorkspaceAndOrganization(TAG) } returns owner
        every { data.canUseDataplaneGroup(GROUP, ORGANIZATION) } returns true
        every { data.catalogWasFetchedByActor(CATALOG, any()) } returns true
        every { data.requireActorDefinitionType(DEFINITION, any()) } just Runs
        every { data.canUseActorDefinition(DEFINITION, owner) } returns true
        every { data.replicationJobConnectionId(0L) } returns CONNECTION
        every { data.isAssignedSyncWorkload(ORGANIZATION, WORKSPACE, CONNECTION, SOURCE, DESTINATION, "workload", GROUP, any()) } returns true
        val permissions = mockk<PermissionService>()
        every { permissions.getPermissionsByAuthUserId(any()) } returns emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
        context.start()
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          val routes =
            context
              .getBean(Router::class.java)
              .uriRoutes()
              .filter { route ->
                val path = route.uriMatchTemplate.toString()
                route.httpMethodName != "HEAD" && listOf("sources", "destinations", "connections").any { path.startsWith("/api/v1/$it/") } &&
                  path !in setOf("/api/v1/sources/search", "/api/v1/destinations/search", "/api/v1/connections/search")
              }.toList()
              .distinctBy { "${it.httpMethodName} ${it.uriMatchTemplate}" }
          assertEquals(if (cloud) 79 else 77, routes.size, "Cover all 75/77 newly migrated operations plus the two existing shared operations")
          HttpClient.newHttpClient().use { client ->
            every { permissions.getPermissionsByAuthUserId("caller") } returns
              listOf(Permission().withWorkspaceId(UUID(0, 998)).withPermissionType(Permission.PermissionType.WORKSPACE_READER))
            val protectedListings =
              listOf(
                "/api/v1/connections/events/list_minimal" to batchBody("ConnectionEventsListMinimalRequestBody"),
                "/api/v1/connections/list_by_actor_definition" to batchBody("ActorDefinitionRequestBody"),
              )
            assertAll(
              protectedListings.map { (path, body) ->
                Executable {
                  val response =
                    client.send(
                      java.net.http.HttpRequest
                        .newBuilder(URI("${server.url}$path"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer unscoped")
                        .POST(
                          java.net.http.HttpRequest.BodyPublishers
                            .ofString(body),
                        ).build(),
                      HttpResponse.BodyHandlers.ofString(),
                    )
                  assertEquals(
                    403,
                    response.statusCode(),
                    "An unscoped A-only reader must not list workspace B or global connections: $path ${response.body()}",
                  )
                }
              },
            )
            for (route in routes) {
              val path =
                route.uriMatchTemplate
                  .toString()
                  .replace(Regex("\\{[?&][^}]*}"), "")
                  .replace(
                    "{sourceId}",
                    SOURCE.toString(),
                  ).replace("{destinationId}", DESTINATION.toString())
                  .replace("{connectionId}", CONNECTION.toString())
              val bodyType =
                route.targetMethod.arguments
                  .singleOrNull { it.type != UUID::class.java && !(it.type == String::class.java && it.name in setOf("sourceId", "destinationId")) }
                  ?.type
              val body = if (bodyType == null) "" else batchBody(bodyType.simpleName)
              val method = route.targetMethod.methodName
              val action = batchAction(method)
              val grant =
                when (action) {
                  "read" -> Permission.PermissionType.WORKSPACE_READER
                  "admin" -> Permission.PermissionType.INSTANCE_ADMIN
                  "worker" -> Permission.PermissionType.DATAPLANE
                  "run" -> Permission.PermissionType.WORKSPACE_RUNNER
                  else -> Permission.PermissionType.WORKSPACE_EDITOR
                }
              val allowedGrant = Permission().withPermissionType(grant)
              if (action !in setOf("admin", "worker")) allowedGrant.workspaceId = WORKSPACE
              if (action == "worker") allowedGrant.organizationId = ORGANIZATION
              every { permissions.getPermissionsByAuthUserId("caller") } returns if (action == "authenticated") emptyList() else listOf(allowedGrant)
              val ruleType =
                route.annotationMetadata
                  .classValue(
                    AuthorizeOperation::class.java,
                    "rule",
                  ).orElseThrow()
                  .asSubclass(OperationRule::class.java)
              val rule = context.getBean(ruleType) as OperationRule
              val mapper = MoreMappers.initMapper()
              val parsed =
                route.targetMethod.arguments
                  .map { argument ->
                    if (argument.type == UUID::class.java ||
                      (argument.type == String::class.java && argument.name in setOf("sourceId", "destinationId"))
                    ) {
                      val id =
                        when {
                          path.contains("/sources/") -> SOURCE
                          path.contains("/destinations/") -> DESTINATION
                          else -> CONNECTION
                        }
                      if (argument.type == String::class.java) id.toString() else id
                    } else {
                      check(body.isNotEmpty()) { "Missing fixture body for $method $path (${argument.type.simpleName})" }
                      mapper.readValue(body, argument.type)
                    }
                  }.toTypedArray()
              val input = rule.readInput(parsed)
              val principal =
                if (action ==
                  "worker"
                ) {
                  Authentication.build(UUID(0, 212).toString(), emptyList(), mapOf(TokenType.SERVICE_ACCOUNT.toClaim()))
                } else {
                  Authentication.build("caller")
                }
              every { permissions.getPermissionsByServiceAccountId(any()) } returns listOf(allowedGrant)
              assertDoesNotThrow({ rule.authorize(principal, input) }, "$method must permit the existing $action action")
              if (action != "authenticated") {
                val insufficient =
                  if (action in setOf("write", "run", "worker", "admin")) {
                    Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_READER)
                  } else {
                    Permission().withOrganizationId(ORGANIZATION).withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)
                  }
                every { permissions.getPermissionsByAuthUserId("caller") } returns listOf(insufficient)
                every { permissions.getPermissionsByServiceAccountId(any()) } returns listOf(insufficient)
                val denied =
                  assertThrows(RuntimeException::class.java, { rule.authorize(principal, input) }, "$method must enforce the $action action")
                assertTrue(denied.javaClass.simpleName in setOf("ForbiddenProblem", "FusionSearchAuthorizationProblem"), "$method: $denied")
              }
              every { permissions.getPermissionsByAuthUserId("caller") } returns emptyList()
              val response =
                client.send(
                  java.net.http.HttpRequest
                    .newBuilder(URI("${server.url}$path"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer different-workspace")
                    .method(
                      route.httpMethodName,
                      java.net.http.HttpRequest.BodyPublishers
                        .ofString(body),
                    ).build(),
                  HttpResponse.BodyHandlers.ofString(),
                )
              val expected = if (path.endsWith("/search-status") || path.endsWith("/search")) 404 else 403
              assertEquals(expected, response.statusCode(), "${route.httpMethodName} $path (${bodyType?.simpleName}): ${response.body()}")
            }

            // A supplied sync catalog also uses the saved destination catalog when its ID is omitted.
            every { permissions.getPermissionsByAuthUserId("caller") } returns
              listOf(WORKSPACE, UUID(0, 998)).map {
                Permission().withWorkspaceId(it).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
              }
            val patch = """{"connectionId":"$CONNECTION","syncCatalog":{"streams":[]}}"""
            for ((failure, status) in listOf(
              null to 400,
              AuthorizationResourceNotFoundException() to 404,
              IOException("catalog unavailable") to 500,
            )) {
              every { data.destinationCatalogForValidation(CONNECTION, WORKSPACE, any()) } answers {
                if (failure != null) throw failure
                Optional.of(CATALOG)
              }
              every { data.catalogWasFetchedByActor(CATALOG, DESTINATION) } returns false
              for ((path, body) in listOf(
                "/api/v1/connections/update" to patch,
                "/api/v1/connections/update_with_reason" to """{"connectionUpdate":$patch,"updateReason":"test","autoUpdate":false}""",
              )) {
                val response =
                  client.send(
                    java.net.http.HttpRequest
                      .newBuilder(URI("${server.url}$path"))
                      .header("Content-Type", "application/json")
                      .header("Authorization", "Bearer unscoped")
                      .POST(
                        java.net.http.HttpRequest.BodyPublishers
                          .ofString(body),
                      ).build(),
                    HttpResponse.BodyHandlers.ofString(),
                  )
                assertEquals(status, response.statusCode(), "$path must reject an invalid saved fallback before business work: ${response.body()}")
              }
            }
          }
          for ((failure, status, title) in listOf(
            Triple(AuthorizationResourceNotFoundException(), 404, "not-found"),
            Triple(IOException("ownership unavailable"), 500, "internal-error"),
          )) {
            every { data.savedSourceWorkspaceAndOrganization(SOURCE) } throws failure
            every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } throws failure
            val executeRoutes =
              if (cloud) {
                listOf(
                  "POST" to "/api/v1/sources/$SOURCE/execute",
                  "POST" to "/api/v1/destinations/$DESTINATION/execute",
                )
              } else {
                emptyList()
              }
            val missingRoutes =
              listOf(
                "GET" to "/api/v1/sources/$SOURCE/search-status",
                "GET" to "/api/v1/destinations/$DESTINATION/search-status",
                "POST" to "/api/v1/sources/$SOURCE/search",
                "POST" to "/api/v1/destinations/$DESTINATION/search",
              ) + executeRoutes
            HttpClient.newHttpClient().use { client ->
              for ((method, path) in missingRoutes) {
                val bodyType = if (path.endsWith("execute")) "SourceExecuteRequest" else "FusionSearchRequest"
                val body = if (method == "GET") "" else batchBody(bodyType)
                val response =
                  client.send(
                    java.net.http.HttpRequest
                      .newBuilder(URI("${server.url}$path"))
                      .header("Content-Type", "application/json")
                      .header("Authorization", "Bearer different-workspace")
                      .method(
                        method,
                        java.net.http.HttpRequest.BodyPublishers
                          .ofString(body),
                      ).build(),
                    HttpResponse.BodyHandlers.ofString(),
                  )
                assertEquals(status, response.statusCode(), "$method $path: ${response.body()}")
                assertEquals(title, MoreMappers.initMapper().readTree(response.body())["title"].asText(), "Preserve the Fusion error envelope")
              }
            }
          }
          assertTrue(context.getBean(ConfigApiBatchEarlierAllow::class.java).calls > 0)
          for (dependency in business) verify { dependency wasNot io.mockk.Called }
        }
      }
  }

  companion object {
    val SOURCE: UUID = UUID(0, 201)
    val DESTINATION: UUID = UUID(0, 202)
    val CONNECTION: UUID = UUID(0, 203)
    val WORKSPACE: UUID = UUID(0, 204)
    val ORGANIZATION: UUID = UUID(0, 205)
    val DEFINITION: UUID = UUID(0, 206)
    val OPERATION: UUID = UUID(0, 207)
    val CATALOG: UUID = UUID(0, 208)
    val GROUP: UUID = UUID(0, 209)
    val TAG: UUID = UUID(0, 210)
    val EVENT: UUID = UUID(0, 211)
  }
}

private fun batchAction(method: String): String =
  when (method) {
    "listConnectionsByActorDefinition" -> "admin"
    "autoDisableConnection",
    "backfillConnectionEvents",
    "getConnectionContext",
    -> "admin"
    "writeDiscoverCatalogResult",
    "writeDestinationDiscoverCatalogResult",
    "resolveFusionEnablementForSync",
    -> "worker"
    "syncConnection",
    "refreshConnectionStream",
    "requestFusionConnectionBackfill",
    -> "run"
    "executeSource",
    "executeDestination",
    "getSource",
    "getSourceWithMetadata",
    "getMostRecentSourceActorCatalog",
    "listSourcesForWorkspace",
    "listSourcesForWorkspacePaginated",
    "getDestination",
    "listDestinationsForWorkspace",
    "listDestinationsForWorkspacesPaginated",
    "discoverCatalogForDestination",
    "getCatalogForConnection",
    "getConnection",
    "getConnectionForJob",
    "getConnectionDataHistory",
    "getConnectionUptimeHistory",
    "getConnectionEvent",
    "listConnectionEvents",
    "listConnectionEventsMinimal",
    "listConnectionEventsForJob",
    "getConnectionLastJobPerStream",
    "getConnectionStatuses",
    "getConnectionStreamHistory",
    "getConnectionSyncProgress",
    "listConnectionsForWorkspace",
    "listConnectionsForWorkspacesPaginated",
    "listAllConnectionsForWorkspace",
    "getFusionSourceEnablement",
    "getFusionDestinationEnablement",
    "getFusionConnectionEnablement",
    "searchFusionSource",
    "searchFusionDestination",
    "getFusionSourceSearchStatus",
    "getFusionDestinationSearchStatus",
    -> "read"
    else -> "write"
  }

private fun batchBody(type: String): String {
  val source = ConfigApiBatchAuthorizationHttpContract.SOURCE
  val destination = ConfigApiBatchAuthorizationHttpContract.DESTINATION
  val connection = ConfigApiBatchAuthorizationHttpContract.CONNECTION
  val workspace = ConfigApiBatchAuthorizationHttpContract.WORKSPACE
  val organization = ConfigApiBatchAuthorizationHttpContract.ORGANIZATION
  val definition = ConfigApiBatchAuthorizationHttpContract.DEFINITION
  val catalog = ConfigApiBatchAuthorizationHttpContract.CATALOG
  val group = ConfigApiBatchAuthorizationHttpContract.GROUP
  val operation = ConfigApiBatchAuthorizationHttpContract.OPERATION
  val tag = ConfigApiBatchAuthorizationHttpContract.TAG
  val event = ConfigApiBatchAuthorizationHttpContract.EVENT
  val dates = """"createdAtStart":"2026-10-19T00:00:00Z","createdAtEnd":"2026-10-20T00:00:00Z""""
  val catalogField = """"catalog":{"streams":[]},"catalogId":"$catalog""""
  val update =
    """
{"connectionId":"$connection",
"operationIds":["$operation"],
"sourceCatalogId":"$catalog",
"destinationCatalogId":"$catalog",
"dataplaneGroupId":"$group",
"tags":[{"tagId":"$tag",
"workspaceId":"$workspace",
"name":"tag",
"color":"#ffffff"}]}
    """.trimIndent()
  return when (type) {
    "SourceIdRequestBody",
    "SourceIdWithSecretCoordinatesRequestBody",
    ->
      """
{"sourceId":"$source"}
      """.trimIndent()
    "SourceUpdate",
    "PartialSourceUpdate",
    ->
      """
{"sourceId":"$source",
"name":"source",
"connectionConfiguration":{}}
      """.trimIndent()
    "SourceCreate" ->
      """
{"workspaceId":"$workspace",
"sourceDefinitionId":"$definition",
"name":"source",
"connectionConfiguration":{}}
      """.trimIndent()
    "SourceDiscoverSchemaRequestBody" ->
      """
{"sourceId":"$source",
"connectionId":"$connection",
"disableCache":false}
      """.trimIndent()
    "SourceAutoPropagateChange" ->
      """
{"sourceId":"$source",
"workspaceId":"$workspace",
$catalogField}
      """.trimIndent()
    "SourceDiscoverSchemaWriteRequestBody" ->
      """
{"sourceId":"$source",
"catalog":{"streams":[]},
"connectorVersion":"1",
"configurationHash":"hash"}
      """.trimIndent()
    "DestinationIdRequestBody" ->
      """
{"destinationId":"$destination"}
      """.trimIndent()
    "DestinationUpdate",
    "PartialDestinationUpdate",
    ->
      """
{"destinationId":"$destination",
"name":"destination",
"connectionConfiguration":{}}
      """.trimIndent()
    "DestinationCreate" ->
      """
{"workspaceId":"$workspace",
"destinationDefinitionId":"$definition",
"name":"destination",
"connectionConfiguration":{}}
      """.trimIndent()
    "DestinationDiscoverSchemaRequestBody" ->
      """
{"destinationId":"$destination",
"disableCache":false}
      """.trimIndent()
    "DestinationDiscoverSchemaWriteRequestBody" ->
      """
{"destinationId":"$destination",
"catalog":{"operations":[]},
"connectorVersion":"1",
"configurationHash":"hash"}
      """.trimIndent()
    "ActorListCursorPaginatedRequestBody",
    "WorkspaceIdRequestBody",
    ->
      """
{"workspaceId":"$workspace"}
      """.trimIndent()
    "ListResourcesForWorkspacesRequestBody" ->
      """
{"workspaceIds":["$workspace"],"pagination":{}}
      """.trimIndent()
    "ListConnectionsForWorkspacesRequestBody" ->
      """
{"workspaceIds":["$workspace"],
"userId":"$source",
"tagIds":["$tag"]}
      """.trimIndent()
    "ConnectionIdRequestBody",
    "ConnectionLastJobPerStreamRequestBody",
    ->
      """
{"connectionId":"$connection"}
      """.trimIndent()
    "ConnectionCreate" ->
      """
{"sourceId":"$source",
"destinationId":"$destination",
"status":"active",
"operationIds":["$operation"],
"sourceCatalogId":"$catalog",
"destinationCatalogId":"$catalog",
"dataplaneGroupId":"$group",
"tags":[{"tagId":"$tag",
"workspaceId":"$workspace",
"name":"tag",
"color":"#ffffff"}]}
      """.trimIndent()
    "ConnectionUpdate" -> update
    "ConnectionUpdateWithReason" ->
      """
{"connectionUpdate":$update,
"updateReason":"test",
"autoUpdate":false}
      """.trimIndent()
    "ConnectionStatusesRequestBody" ->
      """
{"connectionIds":["$connection"]}
      """.trimIndent()
    "ConnectionAndJobIdRequestBody" ->
      """
{"connectionId":"$connection",
"jobId":0}
      """.trimIndent()
    "ConnectionDataHistoryRequestBody",
    "ConnectionUptimeHistoryRequestBody",
    ->
      """
{"connectionId":"$connection",
"numberOfJobs":0}
      """.trimIndent()
    "ConnectionStreamRequestBody" ->
      """
{"connectionId":"$connection",
"streams":[]}
      """.trimIndent()
    "ConnectionStreamHistoryRequestBody" ->
      """
{"connectionId":"$connection",
"timezone":"UTC"}
      """.trimIndent()
    "ConnectionStreamRefreshRequestBody" ->
      """
{"connectionId":"$connection",
"refreshMode":"Truncate",
"streams":[]}
      """.trimIndent()
    "ConnectionAutoPropagateSchemaChange" ->
      """
{"connectionId":"$connection",
"workspaceId":"$workspace",
$catalogField}
      """.trimIndent()
    "PostprocessDiscoveredCatalogRequestBody" ->
      """
{"connectionId":"$connection",
"catalogId":"$catalog"}
      """.trimIndent()
    "ConnectionEventsRequestBody" ->
      """
{"connectionId":"$connection",
"eventTypes":[]}
      """.trimIndent()
    "ConnectionEventsBackfillRequestBody" ->
      """
{"connectionId":"$connection",
$dates}
      """.trimIndent()
    "ConnectionEventsListMinimalRequestBody" ->
      """
{"workspaceId":"$workspace",
"eventTypes":[],
$dates}
      """.trimIndent()
    "ConnectionEventIdRequestBody" ->
      """
{"connectionId":"$connection",
"connectionEventId":"$event"}
      """.trimIndent()
    "JobIdRequestBody" ->
      """
{"id":0}
      """.trimIndent()
    "ActorDefinitionRequestBody" ->
      """
{"actorDefinitionId":"$definition",
"actorType":"source"}
      """.trimIndent()
    "FusionSourceEnablementCreate" -> """{"enable_agent_access":true,"enable_indexing":true}"""
    "FusionDestinationEnablementCreate" -> """{"enable_agent_access":true,"enable_indexing":true,"enable_backfill":false}"""
    "FusionConnectionEnablementCreate" -> """{"enable_indexing":true}"""
    "FusionSourceEnablementUpdate" ->
      """{"enable_agent_access":true,"enable_indexing":true,"expectedState":{"enable_agent_access":false,"enable_indexing":false}}"""
    "FusionDestinationEnablementUpdate" ->
      """{"enable_agent_access":true,"enable_indexing":true,"enable_backfill":false,"expectedState":{"enable_agent_access":false,"enable_indexing":false,"enable_backfill":false}}"""
    "FusionConnectionEnablementUpdate" -> """{"enable_indexing":true,"expectedState":{"enable_indexing":false}}"""
    "FusionConnectionBackfillRequest" -> "{}"
    "FusionEnablementResolveRequest" ->
      """
{"organizationId":"$organization",
"workspaceId":"$workspace",
"connectionId":"$connection",
"sourceId":"$source",
"destinationId":"$destination",
"dataplaneGroupId":"$group",
"workloadId":"workload"}
      """.trimIndent()
    "FusionSearchRequest" ->
      """
{"type":"keyword",
"prompt":"query",
"destination_id":"$destination",
"streams":[{"source_id":"$source",
"stream_name":"stream"}]}
      """.trimIndent()
    "SourceExecuteRequest",
    "DestinationExecuteRequest",
    ->
      """
{"entity":"entity",
"action":"read",
"params":{}}
      """.trimIndent()
    else -> error("No explicit typed request fixture for $type")
  }
}

@Singleton
@Requires(property = "spec.name", value = "ConfigApiBatchAuthorizationHttpTest")
class ConfigApiBatchAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> =
    Publishers.just(
      Authentication.build(
        "caller",
        emptyList(),
        if (request.headers["Authorization"] ==
          "Bearer unscoped"
        ) {
          emptyMap()
        } else {
          mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(UUID(0, 999).toString()))
        },
      ),
    )
}

@Singleton
@Requires(property = "spec.name", value = "ConfigApiBatchAuthorizationHttpTest")
class ConfigApiBatchEarlierAllow : SecurityRule<HttpRequest<*>> {
  var calls = 0

  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> {
    calls++
    return Publishers.just(SecurityRuleResult.ALLOWED)
  }
}
