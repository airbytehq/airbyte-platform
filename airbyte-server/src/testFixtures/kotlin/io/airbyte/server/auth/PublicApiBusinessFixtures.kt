/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.EmbeddedWorkspacesHandler
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Application
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.DeclarativeManifest
import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.config.Permission
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.ApplicationService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.publicApi.server.generated.models.ConnectionResponse
import io.airbyte.publicApi.server.generated.models.ConnectionsResponse
import io.airbyte.publicApi.server.generated.models.ConnectorDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.DataplaneCreateResponse
import io.airbyte.publicApi.server.generated.models.DataplaneResponse
import io.airbyte.publicApi.server.generated.models.DestinationResponse
import io.airbyte.publicApi.server.generated.models.DestinationsResponse
import io.airbyte.publicApi.server.generated.models.JobResponse
import io.airbyte.publicApi.server.generated.models.JobsResponse
import io.airbyte.publicApi.server.generated.models.OrganizationsResponse
import io.airbyte.publicApi.server.generated.models.RegionResponse
import io.airbyte.publicApi.server.generated.models.SourceResponse
import io.airbyte.publicApi.server.generated.models.SourcesResponse
import io.airbyte.publicApi.server.generated.models.TagResponse
import io.airbyte.publicApi.server.generated.models.TagsResponse
import io.airbyte.publicApi.server.generated.models.UsersResponse
import io.airbyte.publicApi.server.generated.models.WorkspaceResponse
import io.airbyte.publicApi.server.generated.models.WorkspacesResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.airbyte.server.apis.publicapi.services.ConnectorDefinitionsService
import io.airbyte.server.apis.publicapi.services.DataplaneService
import io.airbyte.server.apis.publicapi.services.DestinationService
import io.airbyte.server.apis.publicapi.services.JobService
import io.airbyte.server.apis.publicapi.services.OrganizationService
import io.airbyte.server.apis.publicapi.services.RegionService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.airbyte.server.apis.publicapi.services.TagService
import io.airbyte.server.apis.publicapi.services.UserService
import io.airbyte.server.apis.publicapi.services.WorkspaceService
import io.airbyte.server.helpers.GroupPermissionValidator
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.micronaut.security.token.jwt.generator.JwtTokenGenerator
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import jakarta.ws.rs.core.Response
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.net.http.HttpResponse
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.concurrent.Callable

private val fixtureId = UUID.fromString("00000000-0000-0000-0000-000000000101")
private val fixtureDestination = UUID.fromString("00000000-0000-0000-0000-000000000104")
private val fixtureWorkspace = UUID.fromString("00000000-0000-0000-0000-000000000102")
private val fixtureOrganization = UUID.fromString("00000000-0000-0000-0000-000000000103")
private val fixtureMapper = MoreMappers.initMapper()

private inline fun <reified T> fixture(json: String): T = fixtureMapper.readValue(json, T::class.java)

private fun response(
  entity: Any,
  status: Int = 200,
) = Response.status(status).entity(entity).build()

/** Business dependencies return concrete models; controllers, binding, interceptors and serialization stay real. */
fun configurePublicApiBusiness(
  business: List<Any>,
  cloud: Boolean,
) {
  val user = AuthenticatedUser().withUserId(fixtureId).withAuthUserId("caller")
  val source: SourceResponse =
    fixture(
      """{"sourceId":"$fixtureId","name":"fixture","sourceType":"postgres","definitionId":"$fixtureId","workspaceId":"$fixtureWorkspace","configuration":{},"createdAt":0}""",
    )
  val destination: DestinationResponse =
    fixture(
      """{"destinationId":"$fixtureId","name":"fixture","destinationType":"postgres","definitionId":"$fixtureId","workspaceId":"$fixtureWorkspace","configuration":{},"createdAt":0}""",
    )
  val connection: ConnectionResponse =
    fixture(
      """{"connectionId":"$fixtureId","name":"fixture","sourceId":"$fixtureId","destinationId":"$fixtureDestination","workspaceId":"$fixtureWorkspace","status":"active","schedule":{"scheduleType":"manual"},"configurations":{"streams":[]},"tags":[],"createdAt":0}""",
    )
  val job: JobResponse =
    fixture("""{"jobId":1,"connectionId":"$fixtureId","jobType":"sync","status":"succeeded","startTime":"2026-01-01T00:00:00Z"}""")
  val tag = TagResponse(tagId = fixtureId, workspaceId = fixtureWorkspace, name = "fixture", color = "#123456")
  val workspace = fixture<WorkspaceResponse>("""{"workspaceId":"$fixtureWorkspace","name":"fixture","dataResidency":"auto","notifications":{}}""")
  val region = RegionResponse(fixtureId, "fixture", fixtureOrganization, true, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
  val dataplane = DataplaneResponse(fixtureId, "fixture", fixtureId, true, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
  val group =
    Group(
      GroupId(fixtureId),
      "fixture",
      null,
      OrganizationId(fixtureOrganization),
      0,
      OffsetDateTime.parse("2026-01-01T00:00:00Z"),
      OffsetDateTime.parse("2026-01-01T00:00:00Z"),
    )
  val permission =
    Permission()
      .withPermissionId(
        fixtureId,
      ).withWorkspaceId(fixtureWorkspace)
      .withUserId(fixtureId)
      .withGroupId(fixtureId)
      .withPermissionType(Permission.PermissionType.WORKSPACE_READER)
  val permissionRead: io.airbyte.api.server.generated.models.PermissionRead =
    fixture("""{"permissionId":"$fixtureId","workspaceId":"$fixtureWorkspace","userId":"$fixtureId","permissionType":"workspace_reader"}""")
  business.forEach { dependency ->
    when (dependency) {
      is CurrentUserService -> every { dependency.getCurrentUser() } returns user
      is TrackingHelper -> {
        every { dependency.callWithTracker<Any?>(any(), any(), any(), any()) } answers { firstArg<Callable<Any?>>().call() }
        every { dependency.trackSuccess(any(), any(), any(), any()) } just Runs
        every { dependency.trackSuccess(any(), any(), any()) } just Runs
        every { dependency.trackFailuresIfAny(any(), any(), any(), any()) } just Runs
      }
      is SourceService -> {
        every { dependency.getSource(any(), any()) } returns source
        every { dependency.createSource(any(), any()) } returns source
        every { dependency.updateSource(any(), any()) } returns source
        every { dependency.partialUpdateSource(any(), any()) } returns source
        every { dependency.deleteSource(any()) } just Runs
        every { dependency.listSourcesForWorkspaces(any(), any(), any(), any()) } returns SourcesResponse(listOf(source))
        every { dependency.getSourceSchema(any(), any()) } returns fixture("""{"catalogId":"$fixtureId","catalog":{"streams":[]}}""")
        every { dependency.controllerInitiateOAuth(any()) } returns
          if (cloud) response(mapOf("redirectUrl" to "https://example.com/oauth")) else Response.status(501).build()
      }
      is DestinationService -> {
        every { dependency.getDestination(any(), any()) } returns destination
        every { dependency.createDestination(any(), any()) } returns destination
        every { dependency.updateDestination(any(), any()) } returns destination
        every { dependency.partialUpdateDestination(any(), any()) } returns destination
        every { dependency.deleteDestination(any()) } just Runs
        every { dependency.listDestinationsForWorkspaces(any(), any(), any(), any()) } returns DestinationsResponse(listOf(destination))
        every { dependency.getDestinationRead(any()) } returns
          fixture(
            """{"destinationId":"$fixtureDestination","workspaceId":"$fixtureWorkspace","destinationDefinitionId":"$fixtureId","name":"fixture","destinationName":"fixture","createdAt":0,"connectionConfiguration":{}}""",
          )
        every { dependency.getDestinationSyncModes(any<UUID>()) } returns emptyList()
        every { dependency.getDestinationSyncModes(any<io.airbyte.api.server.generated.models.DestinationRead>()) } returns emptyList()
      }
      is ConnectionService -> {
        every { dependency.getConnection(any()) } returns connection
        every { dependency.createConnection(any(), any(), any(), any(), any()) } returns connection
        every { dependency.updateConnection(any(), any(), any(), any(), any()) } returns connection
        every { dependency.deleteConnection(any()) } just Runs
        every { dependency.listConnectionsForWorkspaces(any(), any(), any(), any(), any()) } returns ConnectionsResponse(listOf(connection))
      }
      is io.airbyte.commons.server.services.DestinationDiscoverService -> {
        every { dependency.getDestinationCatalogIfSupported(any(), any()) } returns null
      }
      is JobService -> {
        every { dependency.cancelJob(any()) } returns job
        every { dependency.getJobInfoWithoutLogs(any()) } returns job
        every { dependency.sync(any(), any(), any()) } returns job
        every { dependency.reset(any()) } returns job
        every { dependency.getJobList(any<UUID>(), any(), any(), any()) } returns JobsResponse(listOf(job))
      }
      is WorkspaceHelper -> every { dependency.getOrganizationForWorkspace(any()) } returns fixtureOrganization
      is WorkspaceService -> {
        every { dependency.controllerGetWorkspace(any()) } returns response(workspace)
        every { dependency.controllerCreateWorkspace(any()) } returns response(workspace)
        every { dependency.controllerUpdateWorkspace(any(), any()) } returns response(workspace)
        every { dependency.controllerDeleteWorkspace(any()) } returns Response.noContent().build()
        every { dependency.controllerListWorkspaces(any(), any(), any(), any()) } returns response(WorkspacesResponse(listOf(workspace)))
        every { dependency.controllerSetWorkspaceOverrideOAuthParams(any(), any()) } returns Response.status(if (cloud) 200 else 501).build()
        every { dependency.controllerDeleteWorkspaceOverrideOAuthParams(any(), any(), any()) } returns
          Response.status(if (cloud) 204 else 501).build()
      }
      is TagService -> {
        every { dependency.getTag(any()) } returns tag
        every { dependency.createTag(any()) } returns tag
        every { dependency.updateTag(any(), any(), any()) } returns tag
        every { dependency.deleteTag(any(), any()) } just Runs
        every { dependency.listTags(any()) } returns TagsResponse(listOf(tag))
      }
      is UserService -> {
        every { dependency.getAllWorkspaceIdsForUser(any()) } returns listOf(fixtureWorkspace)
        every { dependency.getUsersInAnOrganization(any(), any(), any()) } returns UsersResponse(emptyList())
      }
      is OrganizationService -> {
        every { dependency.getOrganizationsByUser(any()) } returns OrganizationsResponse(emptyList())
        every { dependency.setOrganizationOverrideOauthParams(any(), any(), any(), any()) } just Runs
        every { dependency.deleteOrganizationOverrideOauthParams(any(), any(), any()) } just Runs
      }
      is ConnectorDefinitionsService -> every { dependency.listConnectorDefinitions(any(), any()) } returns ConnectorDefinitionsResponse(emptyList())
      is RegionService -> {
        every { dependency.getOrganizationIdFromRegion(any()) } returns fixtureOrganization
        every { dependency.controllerListRegions(any()) } returns response(listOf(region))
        every { dependency.controllerCreateRegion(any()) } returns response(region)
        every { dependency.controllerGetRegion(any()) } returns response(region)
        every { dependency.controllerUpdateRegion(any(), any()) } returns response(region)
        every { dependency.controllerDeleteRegion(any()) } returns response(region)
      }
      is DataplaneService -> {
        every { dependency.getRegionIdFromDataplane(any()) } returns fixtureId
        every { dependency.controllerListDataplanes(any()) } returns response(listOf(dataplane))
        every { dependency.controllerCreateDataplane(any()) } returns response(DataplaneCreateResponse(fixtureId, fixtureId, "client", "secret"))
        every { dependency.controllerGetDataplane(any()) } returns response(dataplane)
        every { dependency.controllerUpdateDataplane(any(), any()) } returns response(dataplane)
        every { dependency.controllerDeleteDataplane(any()) } returns response(dataplane)
      }
      is GroupService -> {
        every { dependency.getGroup(any()) } returns group
        every { dependency.getGroupsForOrganization(any(), any()) } returns listOf(group)
        every { dependency.createGroup(any()) } returns group
        every { dependency.updateGroup(any()) } returns group
        every { dependency.deleteGroup(any(), any()) } just Runs
        every { dependency.getGroupMembers(any(), any()) } returns emptyList()
        every { dependency.addGroupMember(any(), any(), any()) } returns
          GroupMember(fixtureId, fixtureId, fixtureId, "fixture@example.com", "fixture", OffsetDateTime.parse("2026-01-01T00:00:00Z"))
        every { dependency.removeGroupMember(any(), any(), any()) } just Runs
      }
      is UserPersistence ->
        every { dependency.getUser(any()) } returns
          Optional.of(
            io.airbyte.config
              .User()
              .withUserId(fixtureId),
          )
      is GroupsEntitlementHelper -> every { dependency.ensureEntitled(any()) } just Runs
      is GroupPermissionValidator -> {
        every { dependency.validateScope(any(), any(), any(), any()) } just Runs
        every { dependency.isDuplicate(any(), any(), any(), any()) } returns false
      }
      is PermissionService -> {
        every { dependency.getPermissionsByGroupId(any()) } returns listOf(permission)
        every { dependency.getPermission(any()) } returns permission
        every { dependency.createGroupPermission(any()) } returns permission
        every { dependency.deleteGroupPermission(any()) } just Runs
      }
      is PermissionHandler -> {
        every { dependency.getPermissionById(any()) } returns permission
        every { dependency.getPermissionRead(any()) } returns permissionRead
        every { dependency.createPermission(any()) } returns permission
        every { dependency.updatePermission(any()) } just Runs
        every { dependency.deletePermission(any()) } just Runs
        every { dependency.permissionReadListForUser(any()) } returns
          io.airbyte.api.server.generated.models
            .PermissionReadList(emptyList())
        every { dependency.listEffectivePermissionsForUser(any()) } returns emptyList()
        every { dependency.isUserInstanceAdmin(any()) } returns false
      }
      is ApplicationService -> {
        val app =
          Application()
            .withId(
              fixtureId.toString(),
            ).withName("fixture")
            .withClientId("client")
            .withClientSecret("secret")
            .withCreatedOn("2026-01-01T00:00:00Z")
        every { dependency.createApplication(any(), any()) } returns app
        every { dependency.deleteApplication(any(), any()) } returns app
        every { dependency.listApplicationsByUser(any()) } returns listOf(app)
      }
      is LicenseEntitlementChecker -> every { dependency.ensureEntitled(any(), any()) } just Runs
      is EntitlementService -> every { dependency.ensureEntitled(any(), any()) } just Runs
      is JwtTokenGenerator -> every { dependency.generateToken(any<Map<String, Any>>()) } returns Optional.of("fixture-token")
      is AirbyteConfig -> every { dependency.airbyteUrl } returns "https://example.com"
      is AirbyteAuthConfig -> {
        every { dependency.tokenIssuer } returns "airbyte"
        every { dependency.tokenExpiration.embeddedTokenExpirationInMinutes } returns 10
      }
      is EmbeddedWorkspacesHandler -> every { dependency.getOrCreate(any(), any()) } returns WorkspaceId(fixtureWorkspace)
      is OrganizationsHandler ->
        every { dependency.listAllOrganizationsByUser(any()) } returns
          io.airbyte.api.server.generated.models
            .OrganizationReadList(emptyList())
    }
  }
  configureDefinitionBusiness(business)
}

private fun configureDefinitionBusiness(business: List<Any>) {
  // Definition controllers convert Config API models into the public response themselves.
  val source: io.airbyte.api.server.generated.models.SourceDefinitionRead =
    fixture(
      """{"sourceDefinitionId":"$fixtureId","name":"fixture","dockerRepository":"airbyte/source-fixture","dockerImageTag":"1.0.0","documentationUrl":"https://example.com","custom":true}""",
    )
  val destination: io.airbyte.api.server.generated.models.DestinationDefinitionRead =
    fixture(
      """{"destinationDefinitionId":"$fixtureId","name":"fixture","dockerRepository":"airbyte/destination-fixture","dockerImageTag":"1.0.0","documentationUrl":"https://example.com","custom":true}""",
    )
  business.forEach { dependency ->
    when (dependency) {
      is SourceDefinitionsHandler -> {
        every { dependency.listSourceDefinitionsForWorkspace(any()) } returns
          io.airbyte.api.server.generated.models
            .SourceDefinitionReadList(listOf(source))
        every { dependency.getSourceDefinition(any(), any()) } returns source
        every { dependency.createCustomSourceDefinition(any()) } returns source
        every { dependency.updateSourceDefinition(any()) } returns source
        every { dependency.deleteSourceDefinition(any()) } just Runs
      }
      is DestinationDefinitionsHandler -> {
        every { dependency.listDestinationDefinitionsForWorkspace(any()) } returns
          io.airbyte.api.server.generated.models
            .DestinationDefinitionReadList(listOf(destination))
        every { dependency.getDestinationDefinition(any(), any()) } returns destination
        every { dependency.createCustomDestinationDefinition(any()) } returns destination
        every { dependency.updateDestinationDefinition(any()) } returns destination
        every { dependency.deleteDestinationDefinition(any()) } just Runs
      }
      is ConnectorBuilderService -> {
        val manifest =
          DeclarativeManifest()
            .withActorDefinitionId(
              fixtureId,
            ).withVersion(1L)
            .withManifest(fixtureMapper.readTree("""{"spec":{"connection_specification":{}}}"""))
        every { dependency.getConnectorBuilderProjectIdForActorDefinitionId(any()) } returns Optional.of(fixtureId)
        every { dependency.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(any()) } returns manifest
        every { dependency.getDeclarativeManifestsByActorDefinitionId(any()) } answers {
          java.util.stream.Stream
            .of(manifest)
        }
        every { dependency.deleteBuilderProject(any()) } returns true
      }
      is ConnectorBuilderProjectsHandler -> {
        every { dependency.createConnectorBuilderProject(any()) } returns
          fixture("""{"builderProjectId":"$fixtureId","workspaceId":"$fixtureWorkspace","name":"fixture"}""")
        every { dependency.publishConnectorBuilderProject(any()) } returns fixture("""{"sourceDefinitionId":"$fixtureId"}""")
        every { dependency.getConnectorBuilderProjectWithManifest(any()) } returns
          fixture(
            """{"builderProject":{"builderProjectId":"$fixtureId","workspaceId":"$fixtureWorkspace","name":"fixture","hasDraft":true,"sourceDefinitionId":"$fixtureId","activeDeclarativeManifestVersion":1},"declarativeManifest":{"manifest":{"spec":{"connection_specification":{}}},"version":1}}""",
          )
      }
      is DeclarativeSourceDefinitionsHandler -> every { dependency.createDeclarativeSourceDefinitionManifest(any()) } just Runs
    }
  }
}

fun assertPublicApiResponse(
  controller: String,
  request: PublicApiHttpScenario,
  cloud: Boolean,
  response: HttpResponse<String>,
) {
  val path = request.path.substringBefore('?')
  val cloudUnsupported =
    cloud && controller == "DefinitionsController" && request.method in setOf("POST", "PUT") && !path.contains("declarative_sources")
  val unsupportedOAuth = !cloud && (path.endsWith("/initiateOAuth") || controller == "WorkspacesController" && path.contains("oauthCredentials"))
  val noContent =
    request.method == "DELETE" &&
      controller !in setOf("DefinitionsController", "ApplicationsController", "JobsController", "RegionController", "DataplaneController")
  val created = request.method == "POST" && controller in setOf("GroupsController", "GroupMembersController", "GroupPermissionsController")
  val status =
    when {
      unsupportedOAuth -> 501
      cloudUnsupported -> 400
      noContent -> 204
      created -> 201
      else -> 200
    }
  assertEquals(status, response.statusCode(), "${request.method} ${request.path}: ${response.body()}")
  if (unsupportedOAuth || noContent || request.method == "PUT" && path.contains("oauthCredentials")) {
    assertEquals("", response.body())
    return
  }
  val json = fixtureMapper.readTree(response.body())
  if (cloudUnsupported) {
    assertEquals("Non-declarative definitions cannot be created or updated in Airbyte Cloud.", json.path("data").path("message").asText())
    return
  }
  val field =
    when (controller) {
      "SourcesController" -> if (path.endsWith("initiateOAuth")) "redirectUrl" else "sourceId"
      "DestinationsController" -> "destinationId"
      "ConnectionsController" -> "connectionId"
      "WorkspacesController" -> if (path.contains("oauthCredentials")) "" else "workspaceId"
      "JobsController" -> "jobId"
      "TagsController" -> "tagId"
      "DefinitionsController" -> "id"
      "PermissionsController", "GroupPermissionsController" -> "permissionId"
      "GroupsController" -> "groupId"
      "GroupMembersController" -> "memberId"
      "RegionController" -> "regionId"
      "DataplaneController" -> if (request.method == "POST") "dataplane_id" else "dataplaneId"
      "ApplicationsController" -> "id"
      "EmbeddedController" -> if (request.method == "GET") "organizations" else "token"
      else -> ""
    }
  if (controller == "EmbeddedController") {
    if (request.method == "GET") {
      assertTrue(json.path("organizations").isArray, response.body())
      assertEquals(0, json.path("organizations").size(), response.body())
    } else if (path.endsWith("widget_token")) {
      val widget =
        fixtureMapper.readTree(
          java.util.Base64
            .getDecoder()
            .decode(json.path("token").asText()),
        )
      assertEquals("fixture-token", widget.path("token").asText(), response.body())
      assertTrue(widget.path("widgetUrl").asText().contains("workspaceId=$fixtureWorkspace"), response.body())
    } else {
      assertEquals("fixture-token", json.path("token").asText(), response.body())
    }
    return
  }
  val expectedId =
    when (field) {
      "jobId" -> "1"
      "workspaceId" -> fixtureWorkspace.toString()
      else -> fixtureId.toString()
    }
  if (controller == "ApplicationsController" && request.method == "GET" && path.endsWith("/applications")) {
    assertEquals(
      fixtureId.toString(),
      json
        .path("applications")
        .first()
        .path("id")
        .asText(),
      response.body(),
    )
    return
  }
  if (controller == "StreamsController") {
    assertTrue(json.isArray, response.body())
    assertEquals(0, json.size(), response.body())
    return
  }
  val listResponse =
    request.method == "GET" &&
      path.substringAfterLast('/') in
      setOf(
        "sources",
        "destinations",
        "connections",
        "jobs",
        "workspaces",
        "tags",
        "permissions",
        "groups",
        "members",
        "declarative_sources",
        "users",
        "organizations",
        "connector_definitions",
      )
  if (controller in setOf("RegionController", "DataplaneController") && request.method == "GET" &&
    path.substringAfterLast('/') in setOf("regions", "dataplanes")
  ) {
    assertTrue(json.isArray, response.body())
    assertEquals(1, json.size(), response.body())
    assertEquals(expectedId, json.first().path(field).asText(), response.body())
  } else if (listResponse) {
    assertTrue(json.path("data").isArray, response.body())
    val empty =
      controller in
        setOf("UsersController", "OrganizationsController", "PermissionsController", "GroupMembersController", "ConnectorDefinitionsController") ||
        controller == "DefinitionsController" && path.endsWith("declarative_sources")
    assertEquals(if (empty) 0 else 1, json.path("data").size(), response.body())
    if (!json.path("data").isEmpty && field.isNotEmpty()) {
      assertEquals(
        expectedId,
        json
          .path("data")
          .first()
          .path(field)
          .asText(),
        response.body(),
      )
    }
  } else if (field.isNotEmpty()) {
    val expected =
      when (field) {
        "jobId" -> "1"
        "workspaceId" -> fixtureWorkspace.toString()
        "redirectUrl" -> "https://example.com/oauth"
        "token" -> if (path.endsWith("widget_token")) json.path(field).asText() else "fixture-token"
        "organizations" -> json.path(field).asText()
        else -> fixtureId.toString()
      }
    assertTrue(json.has(field), "${request.method} ${request.path}: ${response.body()}")
    assertEquals(expected, json.path(field).asText(), response.body())
  } else {
    assertTrue(json.isArray || json.isObject, response.body())
  }
}
