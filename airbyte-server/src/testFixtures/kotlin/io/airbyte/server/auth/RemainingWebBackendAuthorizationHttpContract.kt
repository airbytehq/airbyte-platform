/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.api.server.generated.models.WebBackendConnectionRead
import io.airbyte.api.server.generated.models.WebBackendConnectionReadList
import io.airbyte.api.server.generated.models.WebBackendConnectionStatusCounts
import io.airbyte.api.server.generated.models.WebBackendValidateMappersResponse
import io.airbyte.api.server.generated.models.WebBackendWorkspaceStateResult
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.errors.AuthException
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.airbyte.server.apis.controllers.WebBackendApiController
import io.airbyte.server.handlers.WebBackendMappersHandler
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.Optional
import java.util.UUID

abstract class RemainingWebBackendAuthorizationHttpContract {
  protected open val cloud = false

  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `web backend operations reject unsupported callers before nested writes credentials or connection work`() {
    assertAll(scenarios().map { scenario -> Executable { exercise(scenario, 403, token = "workload", declaration = false) } })
  }

  @Test
  fun `web backend nested operation creation checks every consumed workspace before any creation`() {
    assertAll(
      scenarios().filter { it.suffix in setOf("connections/create", "connections/update") }.map { scenario ->
        Executable {
          exercise(
            scenario.copy(body = scenario.body.replace(NEW_WORKSPACE_PLACEHOLDER, UUID(0, 99).toString())),
            400,
            grant = Permission.PermissionType.INSTANCE_ADMIN,
            declaration = false,
          )
        }
      },
    )
  }

  @Test
  fun `web backend saved operations and typed list filters cannot cross the authorized workspace`() {
    val update = scenarios().single { it.suffix == "connections/update" }
    val listing = scenarios().single { it.suffix == "connections/list" }
    assertAll(
      { exercise(update.copy(body = """{"connectionId":"$CONNECTION","operations":[]}"""), 400, foreignSavedOperation = true, declaration = false) },
      {
        exercise(
          listing.copy(body = """{"workspaceId":"$WORKSPACE","sourceId":"$DESTINATION"}"""),
          404,
          wrongFilterKind = true,
          declaration = false,
        )
      },
    )
  }

  @Test
  fun `web backend catalog refresh requires write authority before discovery starts`() {
    val get = scenarios().single { it.suffix == "connections/get" }
    exercise(get.copy(body = """{"connectionId":"$CONNECTION","withRefreshedCatalog":true}"""), 403, declaration = false)
  }

  @Test
  fun `web backend update distinguishes saved operation updates new operation creation and catalog fallback`() {
    val update = scenarios().single { it.suffix == "connections/update" }
    val existing = """{"connectionId":"$CONNECTION","operations":[{"operationId":"$OPERATION","workspaceId":"${UUID(
      0,
      99,
    )}","name":"existing","operatorConfiguration":{"operatorType":"normalization"}}]}"""
    exercise(update.copy(body = existing), 200)
    exercise(update, 200, poisonSavedCatalog = true)
    exercise(update.copy(body = """{"connectionId":"$CONNECTION"}"""), 400, poisonSavedCatalog = true)
    exercise(update.copy(body = """{"connectionId":"$CONNECTION","operations":[]}"""), 200)
    exercise(update.copy(body = """{"connectionId":"$CONNECTION","syncCatalog":{"streams":[]}}"""), 400, invalidReference = "destination-catalog")
  }

  @Test
  fun `web backend actual actor catalog tag group and every typed list selector are checked`() {
    val create = scenarios().single { it.suffix == "connections/create" }
    for (reference in listOf("source-catalog", "destination-catalog", "dataplane-group", "tag", "destination-workspace")) {
      exercise(create, 400, invalidReference = reference)
    }
    exercise(create, 404, invalidReference = "definition-kind")
    val listing = scenarios().single { it.suffix == "connections/list" }
    exercise(listing.copy(body = """{"workspaceId":"$WORKSPACE","sourceId":["$SOURCE","$DESTINATION"]}"""), 404, wrongFilterKind = true)
    exercise(listing.copy(body = """{"workspaceId":"$WORKSPACE","destinationId":["$DESTINATION","$SOURCE"]}"""), 404, wrongFilterKind = true)
    exercise(
      listing.copy(body = """{"workspaceId":"$WORKSPACE","sourceId":["$SOURCE","${UUID(0, 98)}"]}"""),
      400,
      invalidReference = "foreign-list-source",
    )
    exercise(
      listing.copy(
        body = """{"workspaceId":"$WORKSPACE","sourceId":[],"destinationId":[],"filters":{"sourceDefinitionIds":["${UUID(
          0,
          98,
        )}"],"tagIds":["${UUID(0, 97)}"]}}""",
      ),
      200,
    )
  }

  @Test
  fun `web backend refreshed reads use editor alternatives and ordinary reads validate consumed saved metadata`() {
    val get = scenarios().single { it.suffix == "connections/get" }
    exercise(
      get.copy(body = """{"connectionId":"$CONNECTION","withRefreshedCatalog":true}"""),
      200,
      grant = Permission.PermissionType.WORKSPACE_DESTINATION_EDITOR,
    )
    exercise(get.copy(body = """{"connectionId":"$CONNECTION","withRefreshedCatalog":false}"""), 200)
    exercise(get, 400, foreignSavedOperation = true)
    exercise(get, 400, poisonSavedCatalog = true)
  }

  @Test
  fun `eight web backend methods declare exact connection workspace and mutation policies with normal results`() {
    for (scenario in scenarios()) {
      exercise(scenario, 200)
      exercise(scenario, 403, granted = false)
      exercise(scenario, 403, token = "embedded")
      exercise(scenario, 403, token = "scoped:${UUID(0, 99)}")
      exercise(scenario, 200, token = "scoped:$WORKSPACE")
      exercise(scenario, 200, token = "internal")
      exercise(scenario, 200, token = "service")
      exercise(scenario, 200, token = "dataplane")
      exercise(scenario, 404, missing = true)
      exercise(scenario, 500, lookupError = true)
      exercise(scenario, 200, granted = false, security = false)
      if (scenario.write) exercise(scenario, 403, grant = Permission.PermissionType.WORKSPACE_READER)
    }
  }

  @Test
  fun `refreshed connections require saved current user permissions while ordinary reads retain worker access`() {
    val get = scenarios().single { it.suffix == "connections/get" }
    val refreshed = get.copy(body = """{"connectionId":"$CONNECTION","withRefreshedCatalog":true}""")
    assertAll(
      listOf("internal", "dataplane", "service").map { token ->
        Executable {
          exercise(refreshed, 401, token = token, missingCurrentUser = true, grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
          exercise(refreshed, 403, token = token, currentUserGranted = false, grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
          exercise(
            refreshed,
            if (token == "internal") 200 else 403,
            token = token,
            granted = false,
            currentUserGranted = true,
            grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
          )
          exercise(refreshed, 200, token = token, grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
          exercise(get, 200, token = token, missingCurrentUser = true)
        }
      },
    )
    exercise(refreshed, 200, token = "application-owner", grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
  }

  private fun exercise(
    scenario: Scenario,
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    grant: Permission.PermissionType =
      if (scenario.write) {
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR
      } else {
        Permission.PermissionType.WORKSPACE_READER
      },
    granted: Boolean = true,
    security: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    foreignSavedOperation: Boolean = false,
    wrongFilterKind: Boolean = false,
    poisonSavedCatalog: Boolean = false,
    invalidReference: String = "",
    missingCurrentUser: Boolean = false,
    currentUserGranted: Boolean = granted,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val controller =
      if (cloud) {
        Class.forName("io.airbyte.server.wrapped.apis.WebBackendApiWrappedController")
      } else {
        WebBackendApiController::class.java
      }
    RemainingOperationHttpSupport(
      cloud,
      listOf(controller),
      scenarios().map { "io.airbyte.server.auth.${it.rule}" }.toSet() + "io.airbyte.server.auth.ConnectionMutationRule",
      security,
    ).run(
      configure = { context, business ->
        business.forEach(::configureDependency)
        val handler = business.filterIsInstance<WebBackendConnectionsHandler>().single()
        every { handler.getStateType(match { it.connectionId == CONNECTION }) } returns ConnectionStateType.STREAM
        every { handler.webBackendCreateConnection(match { it.sourceId == SOURCE && it.destinationId == DESTINATION }) } returns read()
        every { handler.webBackendGetConnection(match { it.connectionId == CONNECTION }) } returns read()
        every { handler.webBackendUpdateConnection(match { it.connectionId == CONNECTION }) } returns read()
        every { handler.webBackendListConnectionsForWorkspace(match { it.workspaceId == WORKSPACE }) } returns
          WebBackendConnectionReadList(connections = emptyList(), pageSize = 20, numConnections = 0)
        every { handler.getWorkspaceState(match { it.workspaceId == WORKSPACE }) } returns
          WebBackendWorkspaceStateResult(hasConnections = false, hasSources = false, hasDestinations = false)
        every { handler.webBackendGetConnectionStatusCounts(match { it.workspaceId == WORKSPACE }) } returns
          Jsons.deserialize(
            """{"running":0,"queued":0,"healthy":0,"failed":0,"paused":0,"notSynced":0}""",
            WebBackendConnectionStatusCounts::class.java,
          )
        val mappers = business.filterIsInstance<WebBackendMappersHandler>().single()
        every { mappers.validateMappers(match { it.connectionId == CONNECTION }) } returns
          WebBackendValidateMappersResponse(initialFields = emptyList(), outputFields = emptyList(), mappers = emptyList())
        business.filterIsInstance<RoleResolver>().forEach { resolver ->
          val request = mockk<RoleResolver.Request>()
          every { resolver.newRequest() } returns request
          every { request.withCurrentUser() } returns request
          every { request.withRef(AuthenticationId.CONNECTION_ID, CONNECTION.toString()) } returns request
          every {
            request.requireOneOfRoles(
              setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR),
            )
          } just Runs
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.workspaceAndOrganization(WORKSPACE) } returns owner
        every { data.workspaceAndOrganization(UUID(0, 99)) } returns WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
        every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
        every { data.savedConnectionReferences(CONNECTION) } returns
          SavedConnectionReferences(
            SOURCE,
            DESTINATION,
            Optional.of(if (poisonSavedCatalog) UUID(0, 97) else SOURCE_CATALOG),
            Optional.of(DESTINATION_CATALOG),
            listOf(OPERATION, SECOND_OPERATION),
          )
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns owner
        every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns owner
        every { data.savedActorDefinitionId(SOURCE, WORKSPACE) } returns SOURCE_DEFINITION
        every { data.savedActorDefinitionId(DESTINATION, WORKSPACE) } returns DESTINATION_DEFINITION
        every { data.requireActorDefinitionType(SOURCE_DEFINITION, true) } just Runs
        every { data.requireActorDefinitionType(DESTINATION_DEFINITION, false) } just Runs
        every { data.savedConnectionOperationIds(CONNECTION) } returns listOf(OPERATION, SECOND_OPERATION)
        every { data.operationWorkspaceAndOrganization(OPERATION) } returns owner
        every { data.operationWorkspaceAndOrganization(SECOND_OPERATION) } returns
          if (foreignSavedOperation) WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION) else owner
        every { data.operationWorkspaceAndOrganization(IGNORED_OPERATION) } throws AuthorizationResourceNotFoundException()
        every { data.catalogWasFetchedByActor(SOURCE_CATALOG, SOURCE) } returns true
        every { data.catalogWasFetchedByAnotherActor(SOURCE_CATALOG, SOURCE) } returns false
        every { data.catalogWasFetchedByActor(DESTINATION_CATALOG, DESTINATION) } returns true
        every { data.savedSourceCatalogId(CONNECTION) } returns Optional.of(if (poisonSavedCatalog) UUID(0, 97) else SOURCE_CATALOG)
        every { data.catalogWasFetchedByActor(UUID(0, 97), SOURCE) } returns false
        every { data.catalogWasFetchedByAnotherActor(UUID(0, 97), SOURCE) } returns true
        every { data.destinationCatalogForValidation(CONNECTION, WORKSPACE, any()) } returns Optional.of(DESTINATION_CATALOG)
        every { data.canUseDataplaneGroup(GROUP, ORGANIZATION) } returns true
        every { data.tagWorkspaceAndOrganization(TAG) } returns owner
        if (wrongFilterKind) {
          every { data.savedSourceWorkspaceAndOrganization(DESTINATION) } throws AuthorizationResourceNotFoundException()
          every { data.savedDestinationWorkspaceAndOrganization(SOURCE) } throws AuthorizationResourceNotFoundException()
        }
        when (invalidReference) {
          "source-catalog" -> {
            every { data.catalogWasFetchedByActor(SOURCE_CATALOG, SOURCE) } returns false
            every { data.catalogWasFetchedByAnotherActor(SOURCE_CATALOG, SOURCE) } returns true
          }
          "destination-catalog" -> every { data.catalogWasFetchedByActor(DESTINATION_CATALOG, DESTINATION) } returns false
          "dataplane-group" -> every { data.canUseDataplaneGroup(GROUP, ORGANIZATION) } returns false
          "tag" -> every { data.tagWorkspaceAndOrganization(TAG) } returns WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
          "destination-workspace" ->
            every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns
              WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
          "definition-kind" ->
            every { data.requireActorDefinitionType(DESTINATION_DEFINITION, false) } throws
              AuthorizationResourceNotFoundException()
          "foreign-list-source" ->
            every { data.savedSourceWorkspaceAndOrganization(UUID(0, 98)) } returns
              WorkspaceAndOrganization(UUID(0, 99), ORGANIZATION)
        }
        if (missing) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws AuthorizationResourceNotFoundException()
          every { data.savedSourceWorkspaceAndOrganization(SOURCE) } throws AuthorizationResourceNotFoundException()
        }
        if (lookupError) {
          every { data.workspaceAndOrganization(WORKSPACE) } throws java.io.IOException("workspace ownership unavailable")
          every { data.savedSourceWorkspaceAndOrganization(SOURCE) } throws java.io.IOException("connection ownership unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.workspaceId = WORKSPACE
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        val currentUser = mockk<CurrentUserService>()
        every { currentUser.getCurrentUser() } returns AuthenticatedUser().withAuthUserId("current-user")
        if (missingCurrentUser) {
          every { currentUser.getCurrentUser() } throws
            AuthException("Could not get the current Airbyte user due to an internal error.")
        }
        every { grants.getPermissionsByAuthUserId("current-user") } returns if (currentUserGranted) listOf(permission) else emptyList()
        context.registerSingleton(CurrentUserService::class.java, currentUser)
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", scenario.path, "io.airbyte.server.auth.${scenario.rule}")
        val response =
          RemainingOperationHttpSupport.request(
            server,
            "POST",
            scenario.path,
            scenario.body.replace(NEW_WORKSPACE_PLACEHOLDER, WORKSPACE.toString()),
            token,
          )
        assertEquals(expected, response.statusCode(), "${scenario.path}: ${response.body()}")
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
      },
    )
  }

  private data class Scenario(
    val suffix: String,
    val body: String,
    val rule: String,
    val write: Boolean = false,
  ) {
    val path = "/api/v1/web_backend/$suffix"
  }

  private fun scenarios(): List<Scenario> {
    val operation = """{"workspaceId":"$NEW_WORKSPACE_PLACEHOLDER","name":"fixture","operatorConfiguration":{"operatorType":"normalization"}}"""
    return listOf(
      Scenario("state/get_type", """{"connectionId":"$CONNECTION"}""", "WebBackendStateTypeRule"),
      Scenario(
        "connections/create",
        """{"sourceId":"$SOURCE","destinationId":"$DESTINATION","status":"active","sourceCatalogId":"$SOURCE_CATALOG","destinationCatalogId":"$DESTINATION_CATALOG","dataplaneGroupId":"$GROUP","tags":[{"tagId":"$TAG","workspaceId":"$WORKSPACE","name":"fixture","color":"#fff"}],"operationIds":["$IGNORED_OPERATION"],"operations":[$operation,$operation]}""",
        "WebBackendConnectionCreateRule",
        true,
      ),
      Scenario("connections/get", """{"connectionId":"$CONNECTION"}""", "WebBackendConnectionGetRule"),
      Scenario("workspace/state", """{"workspaceId":"$WORKSPACE"}""", "WebBackendWorkspaceReadRule"),
      Scenario(
        "connections/list",
        """{"workspaceId":"$WORKSPACE","sourceId":"$SOURCE","destinationId":"$DESTINATION"}""",
        "WebBackendConnectionListRule",
      ),
      Scenario("connections/status_counts", """{"workspaceId":"$WORKSPACE"}""", "WebBackendWorkspaceReadRule"),
      Scenario(
        "connections/update",
        """{"connectionId":"$CONNECTION","sourceCatalogId":"$SOURCE_CATALOG","destinationCatalogId":"$DESTINATION_CATALOG","dataplaneGroupId":"$GROUP","operations":[{"operationId":"$OPERATION","workspaceId":"$WORKSPACE","name":"existing","operatorConfiguration":{"operatorType":"normalization"}},{"operationId":"$IGNORED_OPERATION","workspaceId":"$NEW_WORKSPACE_PLACEHOLDER","name":"new","operatorConfiguration":{"operatorType":"normalization"}}]}""",
        "WebBackendConnectionUpdateRule",
        true,
      ),
      Scenario(
        "mappers/validate",
        """{"connectionId":"$CONNECTION","mappers":[],"streamDescriptor":{"name":"fixture"}}""",
        "WebBackendMappersRule",
        true,
      ),
    )
  }

  private fun read(): WebBackendConnectionRead =
    Jsons.deserialize(
      """{"connectionId":"$CONNECTION","name":"fixture","sourceId":"$SOURCE","destinationId":"$DESTINATION","syncCatalog":{"streams":[]},"status":"active","source":{"sourceDefinitionId":"$SOURCE_DEFINITION","sourceId":"$SOURCE","workspaceId":"$WORKSPACE","connectionConfiguration":{},"name":"fixture","sourceName":"fixture","createdAt":0},"destination":{"destinationDefinitionId":"$DESTINATION_DEFINITION","destinationId":"$DESTINATION","workspaceId":"$WORKSPACE","connectionConfiguration":{},"name":"fixture","destinationName":"fixture","createdAt":0},"isSyncing":false,"schemaChange":"no_change","notifySchemaChanges":false,"notifySchemaChangesByEmail":false,"nonBreakingChangesPreference":"ignore","sourceActorDefinitionVersion":{"dockerRepository":"fixture/source","dockerImageTag":"1","supportsRefreshes":false,"supportState":"supported","isVersionOverrideApplied":false,"supportsFileTransfer":false,"supportsDataActivation":false},"destinationActorDefinitionVersion":{"dockerRepository":"fixture/destination","dockerImageTag":"1","supportsRefreshes":false,"supportState":"supported","isVersionOverrideApplied":false,"supportsFileTransfer":false,"supportsDataActivation":false},"tags":[],"onDemandEnabled":false}""",
      WebBackendConnectionRead::class.java,
    )

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val CONNECTION: UUID = UUID(0, 3)
    val SOURCE: UUID = UUID(0, 4)
    val DESTINATION: UUID = UUID(0, 5)
    val SOURCE_DEFINITION: UUID = UUID(0, 6)
    val DESTINATION_DEFINITION: UUID = UUID(0, 7)
    val OPERATION: UUID = UUID(0, 8)
    val SECOND_OPERATION: UUID = UUID(0, 9)
    val IGNORED_OPERATION: UUID = UUID(0, 10)
    val SOURCE_CATALOG: UUID = UUID(0, 11)
    val DESTINATION_CATALOG: UUID = UUID(0, 12)
    val GROUP: UUID = UUID(0, 13)
    val TAG: UUID = UUID(0, 14)
    const val NEW_WORKSPACE_PLACEHOLDER = "NEW_OPERATION_WORKSPACE"
  }
}
