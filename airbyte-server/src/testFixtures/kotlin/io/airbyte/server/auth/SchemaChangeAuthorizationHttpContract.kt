/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.problems.model.generated.ProblemDestinationCatalogNotFoundData
import io.airbyte.api.problems.throwable.generated.DestinationCatalogNotFoundProblem
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionAutoPropagateResult
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.PostprocessDiscoveredCatalogResult
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.config.DestinationCatalog
import io.airbyte.config.DestinationCatalogWithId
import io.airbyte.config.Permission
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.DestinationCatalogId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.EnableDestinationCatalogValidation
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.service.SoftServiceLoader
import io.micronaut.http.annotation.Controller
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.mockk.Runs
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Optional
import java.util.UUID

/** Real served controllers, interceptor, rules, permissions, and saved-reference/feature-flag lookups in both editions. */
abstract class SchemaChangeAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `connection schema changes check the effective saved destination catalog before business`() =
    checkRoute("/api/v1/connections/apply_schema_changes")

  @Test
  fun `catalog postprocessing checks the effective saved destination catalog before business`() =
    checkRoute("/api/v1/connections/postprocess_discovered_catalog")

  @Test
  fun `source schema propagation checks every selected connections effective saved destination catalog before business`() =
    checkRoute("/api/v1/sources/apply_schema_changes")

  @Test
  fun `destination catalog reads check the saved destination association even with validation disabled`() =
    checkRoute("/api/v1/destinations/get_catalog_for_connection", CatalogRead.DESTINATION)

  @Test
  fun `connection schema changes check the saved source comparison catalog before business`() =
    checkRoute("/api/v1/connections/apply_schema_changes", CatalogRead.SOURCE)

  @Test
  fun `catalog postprocessing checks the saved source comparison catalog before business`() =
    checkRoute("/api/v1/connections/postprocess_discovered_catalog", CatalogRead.SOURCE)

  @Test
  fun `source schema propagation checks every selected connections saved source comparison catalog before business`() =
    checkRoute("/api/v1/sources/apply_schema_changes", CatalogRead.SOURCE)

  @Test
  fun `source discovery checks its connections saved source comparison catalog before business`() =
    checkRoute("/api/v1/sources/discover_schema", CatalogRead.SOURCE)

  @Test
  fun `ordinary connection reads do not check unused saved source catalogs`() = checkRoute("/api/v1/connections/get", CatalogRead.UNUSED_SOURCE)

  protected open fun stubSourceDiscovery(
    context: ApplicationContext,
    sourceId: UUID,
    catalogId: UUID,
  ): () -> Unit {
    val scheduler = context.getBean(SchedulerHandler::class.java)
    every { scheduler.discoverSchemaForSourceFromSourceId(any()) } returns
      Jsons.convertValue(sourceDiscoveryResult(catalogId), SourceDiscoverSchemaRead::class.java)
    return { verify(exactly = 1) { scheduler.discoverSchemaForSourceFromSourceId(match { it.sourceId == sourceId }) } }
  }

  protected fun sourceDiscoveryResult(catalogId: UUID): SourceDiscoverSchemaRead =
    SourceDiscoverSchemaRead(
      jobInfo =
        SynchronousJobRead(
          id = UUID.randomUUID(),
          configType = JobConfigType.DISCOVER_SCHEMA,
          createdAt = 0L,
          endedAt = 0L,
          succeeded = true,
        ),
      catalogId = catalogId,
    )

  private fun checkRoute(
    path: String,
    read: CatalogRead = CatalogRead.SCHEMA_DESTINATION,
  ) {
    val sourceCascade = path == "/api/v1/sources/apply_schema_changes"
    val discovery = path.endsWith("/discover_schema")
    val references = SoftServiceLoader.load(BeanDefinitionReference::class.java).collectAll()
    val controllers =
      references
        .map { it.beanType }
        .filter {
          it.simpleName in
            setOf(
              "SourceApiController",
              "ConnectionApiController",
              "DestinationApiController",
              "SourceApiWrappedController",
              "ConnectionApiWrappedController",
              "DestinationApiWrappedController",
            ) &&
            it.isAnnotationPresent(Controller::class.java)
        }.toSet()
    val allowed =
      controllers.map { it.name } +
        references.filter { OperationRule::class.java.isAssignableFrom(it.beanType) }.map { it.beanType.name } +
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
              if (type == ObjectMapper::class.java) {
                MoreMappers.initMapper()
              } else if (type.name == "io.airbyte.server.wrapped.apis.ApiHelper") {
                type.getConstructor().newInstance()
              } else {
                mockkClass(type.kotlin, relaxed = true).also(business::add)
              }
            val name = parameter.getAnnotation(Named::class.java)?.value
            if (name == null) context.registerSingleton(type, instance) else context.registerSingleton(type, instance, Qualifiers.byName(name))
          }
        val actors = mockk<ActorRepository>()
        val connections = mockk<ConnectionService>()
        val workspaces = mockk<WorkspaceService>()
        val flags = mockk<FeatureFlagClient>()
        val permissions = mockk<PermissionService>()
        val first = sync(CONNECTION, DESTINATION, DESTINATION_CATALOG)
        val second = sync(SECOND_CONNECTION, SECOND_DESTINATION, SECOND_DESTINATION_CATALOG)
        // A different source in the same workspace must never become a schema-cascade target.
        val unrelated = sync(UUID(0, 920), UUID(0, 921), UUID(0, 922)).withSourceId(UUID(0, 923))
        every { connections.getStandardSync(CONNECTION) } returns first
        every { connections.getStandardSync(SECOND_CONNECTION) } returns second
        every { connections.listWorkspaceStandardSyncs(WORKSPACE, false) } returns listOf(first, unrelated, second)
        every { actors.findSourceWorkspaceId(SOURCE) } returns Optional.of(WORKSPACE)
        for (destination in listOf(DESTINATION, SECOND_DESTINATION)) {
          every { actors.findDestinationWorkspaceId(destination) } returns Optional.of(WORKSPACE)
        }
        every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
          StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(ORGANIZATION)
        every { actors.catalogWasFetchedByActor(SOURCE_CATALOG, SOURCE) } returns true
        every { permissions.getPermissionsByAuthUserId("caller") } returns
          listOf(WORKSPACE, UUID(0, 998)).map {
            Permission().withWorkspaceId(it).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)
          }
        val dependencies = listOf(actors, connections, workspaces, flags, permissions)
        val constructor = AuthorizationDataService::class.java.declaredConstructors.single { !it.isSynthetic }
        val data =
          constructor.newInstance(
            *constructor.parameterTypes
              .map { type ->
                dependencies.singleOrNull { type.isInstance(it) } ?: mockkClass(type.kotlin)
              }.toTypedArray(),
          ) as AuthorizationDataService
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
        context.start()
        val handler = context.getBean(ConnectionsHandler::class.java)
        val scheduler = context.getBean(SchedulerHandler::class.java)
        every { handler.applySchemaChange(any()) } returns ConnectionAutoPropagateResult()
        every { handler.postprocessDiscoveredCatalog(any(), any()) } returns PostprocessDiscoveredCatalogResult()
        every { handler.getConnection(CONNECTION) } returns
          ConnectionRead(
            connectionId = CONNECTION,
            name = "fixture",
            sourceId = SOURCE,
            destinationId = DESTINATION,
            syncCatalog = AirbyteCatalog(streams = emptyList()),
            status = ConnectionStatus.ACTIVE,
            breakingChange = false,
          )
        every { scheduler.applySchemaChangeForSource(any()) } just Runs
        val verifyDiscovery = if (discovery) stubSourceDiscovery(context, SOURCE, SOURCE_CATALOG) else ({})
        val destinationDiscover = context.getBean(DestinationDiscoverService::class.java)
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          HttpClient.newHttpClient().use { client ->
            val states =
              when (read) {
                CatalogRead.SCHEMA_DESTINATION ->
                  listOf(
                    CatalogState.INVALID,
                    CatalogState.MISSING,
                    CatalogState.ERROR,
                    CatalogState.VALID,
                    CatalogState.ABSENT,
                    CatalogState.DISABLED,
                  )
                CatalogRead.DESTINATION ->
                  listOf(
                    CatalogState.INVALID,
                    CatalogState.MISSING,
                    CatalogState.ERROR,
                    CatalogState.VALID,
                    CatalogState.SHARED,
                    CatalogState.ABSENT,
                    CatalogState.DISABLED,
                  )
                CatalogRead.SOURCE ->
                  listOf(
                    CatalogState.INVALID,
                    CatalogState.INVALID_DISABLED,
                    CatalogState.MISSING,
                    CatalogState.ERROR,
                    CatalogState.VALID,
                    CatalogState.SHARED,
                    CatalogState.ABSENT,
                    CatalogState.DISABLED,
                  )
                CatalogRead.UNUSED_SOURCE -> listOf(CatalogState.INVALID, CatalogState.MISSING, CatalogState.ERROR)
              } +
                when {
                  sourceCascade -> listOf(CatalogState.NO_CATALOG, CatalogState.NO_CONNECTIONS)
                  discovery -> listOf(CatalogState.NO_CONNECTION)
                  else -> emptyList()
                }
            assertAll(
              states.map { state ->
                Executable {
                  business.forEach { clearMocks(it, answers = false) }
                  every { connections.listWorkspaceStandardSyncs(WORKSPACE, false) } returns
                    if (state == CatalogState.NO_CONNECTIONS) emptyList() else listOf(first, unrelated, second)
                  first.destinationCatalogId = if (state == CatalogState.ABSENT) null else DESTINATION_CATALOG
                  second.destinationCatalogId = if (state == CatalogState.ABSENT) null else SECOND_DESTINATION_CATALOG
                  first.sourceCatalogId = if (state == CatalogState.ABSENT) null else SAVED_SOURCE_CATALOG
                  second.sourceCatalogId =
                    when (state) {
                      CatalogState.ABSENT -> null
                      CatalogState.SHARED -> SAVED_SOURCE_CATALOG
                      else -> SECOND_SAVED_SOURCE_CATALOG
                    }
                  for (id in listOf(CONNECTION, SECOND_CONNECTION)) {
                    every { flags.boolVariation(EnableDestinationCatalogValidation, Multi(listOf(Workspace(WORKSPACE), Connection(id)))) } returns
                      (state !in setOf(CatalogState.DISABLED, CatalogState.INVALID_DISABLED))
                  }
                  every { actors.catalogWasFetchedByActor(DESTINATION_CATALOG, DESTINATION) } returns true
                  every { actors.catalogWasFetchedByActor(SECOND_DESTINATION_CATALOG, SECOND_DESTINATION) } returns true
                  every { actors.catalogWasFetchedByActor(SAVED_SOURCE_CATALOG, SOURCE) } returns true
                  every { actors.catalogWasFetchedByActor(SECOND_SAVED_SOURCE_CATALOG, SOURCE) } returns true
                  val sourceRead = read in setOf(CatalogRead.SOURCE, CatalogRead.UNUSED_SOURCE)
                  val catalog =
                    if (sourceRead) {
                      if (sourceCascade) SECOND_SAVED_SOURCE_CATALOG else SAVED_SOURCE_CATALOG
                    } else if (sourceCascade) {
                      SECOND_DESTINATION_CATALOG
                    } else {
                      DESTINATION_CATALOG
                    }
                  val actor =
                    if (sourceRead) {
                      SOURCE
                    } else if (sourceCascade) {
                      SECOND_DESTINATION
                    } else {
                      DESTINATION
                    }
                  // Catalog deduplication permits another actor's fetch only when the expected actor also fetched it.
                  every { actors.catalogWasFetchedByActor(catalog, OTHER_ACTOR) } returns (state != CatalogState.VALID)
                  every { actors.catalogWasFetchedByActor(catalog, actor) } answers {
                    when (state) {
                      CatalogState.INVALID,
                      CatalogState.INVALID_DISABLED,
                      CatalogState.NO_CATALOG,
                      CatalogState.NO_CONNECTIONS,
                      CatalogState.NO_CONNECTION,
                      -> false
                      CatalogState.DISABLED -> sourceRead
                      CatalogState.MISSING -> throw AuthorizationResourceNotFoundException()
                      CatalogState.ERROR -> throw IOException("catalog lookup unavailable")
                      else -> true
                    }
                  }
                  every { destinationDiscover.getDestinationCatalog(ConnectionId(CONNECTION)) } answers {
                    if (state == CatalogState.ABSENT) {
                      throw DestinationCatalogNotFoundProblem(ProblemDestinationCatalogNotFoundData().connectionId(CONNECTION))
                    }
                    DestinationCatalogWithId(DestinationCatalogId(DESTINATION_CATALOG), DestinationCatalog(emptyList()))
                  }
                  val body =
                    MoreMappers.initMapper().writeValueAsString(
                      buildMap {
                        if (state != CatalogState.NO_CONNECTION) put("connectionId", CONNECTION)
                        put("sourceId", SOURCE)
                        put("workspaceId", WORKSPACE)
                        put("catalogId", SOURCE_CATALOG)
                        put("catalog", if (state == CatalogState.NO_CATALOG) null else mapOf("streams" to emptyList<Any>()))
                        put("disableCache", true)
                      },
                    )
                  val response =
                    client.send(
                      HttpRequest
                        .newBuilder(URI("${server.url}$path"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer unscoped")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                      HttpResponse.BodyHandlers.ofString(),
                    )
                  val expected =
                    if (read == CatalogRead.UNUSED_SOURCE) {
                      200
                    } else {
                      when (state) {
                        CatalogState.INVALID, CatalogState.INVALID_DISABLED, CatalogState.NO_CATALOG -> 400
                        CatalogState.DISABLED -> if (read == CatalogRead.DESTINATION) 400 else 200
                        CatalogState.ABSENT -> if (read == CatalogRead.DESTINATION) 404 else 200
                        CatalogState.MISSING -> 404
                        CatalogState.ERROR -> 500
                        else -> 200
                      }
                    }
                  assertEquals(expected, response.statusCode(), "$path $state: ${response.body()}")
                  if (read == CatalogRead.DESTINATION && state == CatalogState.ABSENT) {
                    val problem = MoreMappers.initMapper().readTree(response.body())
                    assertEquals("error:destination/catalog-not-found", problem["type"].asText())
                    assertEquals(CONNECTION.toString(), problem["data"]["connectionId"].asText())
                    verify(exactly = 1) { destinationDiscover.getDestinationCatalog(ConnectionId(CONNECTION)) }
                  } else if (expected != 200) {
                    for (dependency in business) verify { dependency wasNot io.mockk.Called }
                  } else {
                    when {
                      read == CatalogRead.DESTINATION -> {
                        assertEquals(DESTINATION_CATALOG.toString(), MoreMappers.initMapper().readTree(response.body())["catalogId"].asText())
                        verify(exactly = 1) { destinationDiscover.getDestinationCatalog(ConnectionId(CONNECTION)) }
                      }
                      read == CatalogRead.UNUSED_SOURCE -> verify(exactly = 1) { handler.getConnection(CONNECTION) }
                      discovery -> verifyDiscovery()
                      sourceCascade ->
                        verify(exactly = 1) {
                          scheduler.applySchemaChangeForSource(
                            match {
                              it.sourceId == SOURCE &&
                                it.catalogId == SOURCE_CATALOG
                            },
                          )
                        }
                      path.endsWith(
                        "postprocess_discovered_catalog",
                      ) -> verify(exactly = 1) { handler.postprocessDiscoveredCatalog(CONNECTION, SOURCE_CATALOG) }
                      else ->
                        verify(
                          exactly = 1,
                        ) { handler.applySchemaChange(match { it.connectionId == CONNECTION && it.catalogId == SOURCE_CATALOG }) }
                    }
                  }
                }
              },
            )
          }
        }
      }
  }

  private enum class CatalogRead { SCHEMA_DESTINATION, DESTINATION, SOURCE, UNUSED_SOURCE }

  private enum class CatalogState {
    INVALID,
    INVALID_DISABLED,
    MISSING,
    ERROR,
    VALID,
    SHARED,
    ABSENT,
    DISABLED,
    NO_CATALOG,
    NO_CONNECTIONS,
    NO_CONNECTION,
  }

  companion object {
    private val SOURCE = UUID(0, 901)
    private val DESTINATION = UUID(0, 902)
    private val SECOND_DESTINATION = UUID(0, 903)
    private val CONNECTION = UUID(0, 904)
    private val SECOND_CONNECTION = UUID(0, 905)
    private val SOURCE_CATALOG = UUID(0, 906)
    private val DESTINATION_CATALOG = UUID(0, 907)
    private val SECOND_DESTINATION_CATALOG = UUID(0, 908)
    private val WORKSPACE = UUID(0, 909)
    private val ORGANIZATION = UUID(0, 910)
    private val SAVED_SOURCE_CATALOG = UUID(0, 911)
    private val SECOND_SAVED_SOURCE_CATALOG = UUID(0, 912)
    private val OTHER_ACTOR = UUID(0, 913)

    private fun sync(
      id: UUID,
      destination: UUID,
      catalog: UUID,
    ): StandardSync =
      StandardSync()
        .withConnectionId(id)
        .withSourceId(SOURCE)
        .withDestinationId(destination)
        .withDestinationCatalogId(catalog)
  }
}
