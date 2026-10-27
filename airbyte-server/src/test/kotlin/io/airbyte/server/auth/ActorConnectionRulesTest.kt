/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.AbstractThrowableProblem
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionAndJobIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionStatusesRequestBody
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.ConnectionUpdateWithReason
import io.airbyte.api.server.generated.models.DestinationCatalog
import io.airbyte.api.server.generated.models.DestinationDiscoverSchemaWriteRequestBody
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.FusionEnablementResolveRequest
import io.airbyte.api.server.generated.models.FusionSearchRequest
import io.airbyte.api.server.generated.models.FusionSearchStreamSelector
import io.airbyte.api.server.generated.models.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.Pagination
import io.airbyte.api.server.generated.models.SourceAutoPropagateChange
import io.airbyte.api.server.generated.models.SourceCreate
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.Tag
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.auth.TokenType
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedConnectionReferences
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class ActorConnectionRulesTest {
  @Test
  fun `destination catalog results allow supported workers and reject human admins and workload API tokens`() {
    val f = Fixture()
    val saved = mockk<PermissionService>()
    every { saved.getPermissionsByAuthUserId(any()) } returns listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))
    every { saved.getPermissionsByServiceAccountId(SOURCE) } returns
      listOf(Permission().withOrganizationId(ORGANIZATION).withPermissionType(Permission.PermissionType.DATAPLANE))
    val permissions = OperationPermissions(saved, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
    val rule = DestinationCatalogWriteRule(f.data, permissions)
    val input =
      rule.readInput(
        arrayOf(DestinationDiscoverSchemaWriteRequestBody(catalog = DestinationCatalog(emptyList()), destinationId = DESTINATION)),
      )
    for (type in listOf(TokenType.USER, TokenType.WORKLOAD_API, TokenType.EMBEDDED_V1)) {
      val caller =
        io.micronaut.security.authentication.Authentication
          .build(SOURCE.toString(), emptyList(), mapOf(type.toClaim()))
      assertThrows(ForbiddenProblem::class.java) { rule.authorize(caller, input) }
    }
    for (type in listOf(TokenType.INTERNAL_CLIENT, TokenType.DATAPLANE_V1, TokenType.SERVICE_ACCOUNT)) {
      val caller =
        io.micronaut.security.authentication.Authentication
          .build(SOURCE.toString(), emptyList(), mapOf(type.toClaim()))
      assertDoesNotThrow { rule.authorize(caller, input) }
    }
  }

  @Test
  fun `unauthorized callers cannot probe supplied source or connection relationships`() {
    val f = Fixture(granted = false)
    val source = SourceWriteRule(f.data, f.permissions)
    assertThrows(ForbiddenProblem::class.java) {
      source.authorize(
        f.caller,
        source.readInput(
          arrayOf(
            SourceAutoPropagateChange(catalog = AirbyteCatalog(emptyList()), catalogId = CATALOG, sourceId = SOURCE, workspaceId = OTHER_WORKSPACE),
          ),
        ),
      )
    }
    val mutation = ConnectionMutationRule(f.data, f.permissions)
    every { f.data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns OTHER_OWNER
    assertThrows(ForbiddenProblem::class.java) { mutation.authorize(f.caller, mutation.readInput(arrayOf(create()))) }
  }

  @Test
  fun `connection create update and nested update check every operation even with both workspace grants`() {
    for (body in listOf(
      create().copy(operationIds = listOf(OPERATION, OTHER_OPERATION)),
      ConnectionUpdate(connectionId = CONNECTION, operationIds = listOf(OPERATION, OTHER_OPERATION)),
      ConnectionUpdateWithReason(ConnectionUpdate(connectionId = CONNECTION, operationIds = listOf(OPERATION, OTHER_OPERATION))),
    )) {
      val f = Fixture()
      val rule = ConnectionMutationRule(f.data, f.permissions)
      every { f.data.operationWorkspaceAndOrganization(OTHER_OPERATION) } returns OTHER_OWNER
      assertThrows(BadRequestProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(body))) }
    }
  }

  @Test
  fun `saved source and destination must share a workspace despite both grants and in no auth`() {
    for (security in listOf(true, false)) {
      val f = Fixture(security = security)
      every { f.data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns OTHER_OWNER
      val rule = ConnectionMutationRule(f.data, f.permissions)
      assertThrows(BadRequestProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(create()))) }
    }
  }

  @Test
  fun `catalogs must have a saved fetch for the actual actor while shared catalog IDs remain valid`() {
    val f = Fixture()
    val rule = ConnectionMutationRule(f.data, f.permissions)
    val request = create().copy(sourceCatalogId = CATALOG, destinationCatalogId = CATALOG)
    assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
    every { f.data.catalogWasFetchedByActor(CATALOG, DESTINATION) } returns false
    assertThrows(BadRequestProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
  }

  @Test
  fun `tags and dataplane group references cannot cross the saved workspace or organization`() {
    for (foreign in listOf("tag", "dataplane")) {
      val f = Fixture()
      val rule = ConnectionMutationRule(f.data, f.permissions)
      if (foreign == "tag") every { f.data.tagWorkspaceAndOrganization(TAG) } returns OTHER_OWNER
      if (foreign == "dataplane") every { f.data.canUseDataplaneGroup(GROUP, ORGANIZATION) } returns false
      assertThrows(BadRequestProblem::class.java) {
        rule.authorize(
          f.caller,
          rule.readInput(
            arrayOf(
              create().copy(tags = listOf(Tag(tagId = TAG, workspaceId = WORKSPACE, name = "fixture", color = "#000000")), dataplaneGroupId = GROUP),
            ),
          ),
        )
      }
    }
  }

  @Test
  fun `absent and empty optional operations tags and catalogs retain create and update behavior`() {
    val f = Fixture()
    val rule = ConnectionMutationRule(f.data, f.permissions)
    for (body in listOf(create(), create().copy(operationIds = emptyList(), tags = emptyList()), ConnectionUpdate(connectionId = CONNECTION))) {
      assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(body))) }
    }
    verify(exactly = 0) { f.data.operationWorkspaceAndOrganization(any()) }
    verify(exactly = 0) { f.data.catalogWasFetchedByActor(any(), any()) }
  }

  @Test
  fun `typed inputs retain original IDs and list references after a list is mutated`() {
    val f = Fixture()
    val rule = ConnectionMutationRule(f.data, f.permissions)
    val ids = mutableListOf(OPERATION)
    val body = create().copy(operationIds = ids)
    val input = rule.readInput(arrayOf(body))
    ids.add(OTHER_OPERATION)
    rule.authorize(f.caller, input)
    verify(exactly = 1) { f.data.savedSourceWorkspaceAndOrganization(SOURCE) }
    verify(exactly = 0) { f.data.savedSourceWorkspaceAndOrganization(OTHER_SOURCE) }
    verify(exactly = 1) { f.data.operationWorkspaceAndOrganization(OPERATION) }
    verify(exactly = 0) { f.data.operationWorkspaceAndOrganization(OTHER_OPERATION) }
  }

  @Test
  fun `missing null and malformed nested references reject rather than becoming an empty resource scope`() {
    val f = Fixture()
    val rule = ConnectionMutationRule(f.data, f.permissions)
    for (args in listOf(
      emptyArray<Any?>(),
      arrayOf<Any?>(null),
    )) {
      assertThrows(BadRequestProblem::class.java) { rule.readInput(args) }
    }
    val nullOperation: List<UUID?> = listOf(null)
    @Suppress("UNCHECKED_CAST")
    assertThrows(BadRequestProblem::class.java) { rule.readInput(arrayOf(create().copy(operationIds = nullOperation as List<UUID>))) }
  }

  @Test
  fun `missing and failed required resource lookups propagate and do not authorize the mutation`() {
    for (failure in listOf(AuthorizationResourceNotFoundException(), IOException("lookup unavailable"))) {
      val f = Fixture()
      val rule = ConnectionMutationRule(f.data, f.permissions)
      every { f.data.operationWorkspaceAndOrganization(OPERATION) } throws failure
      assertThrows(failure.javaClass) { rule.authorize(f.caller, rule.readInput(arrayOf(create().copy(operationIds = listOf(OPERATION))))) }
    }
  }

  @Test
  fun `source discover binds optional connection to its actual source rather than merely its workspace`() {
    val f = Fixture()
    val rule = SourceWriteRule(f.data, f.permissions)
    val request = SourceDiscoverSchemaRequestBody(sourceId = SOURCE, connectionId = CONNECTION)
    assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
    every { f.data.savedConnectionReferences(CONNECTION) } returns
      SavedConnectionReferences(OTHER_SOURCE, DESTINATION, Optional.empty(), Optional.empty(), emptyList())
    assertThrows(BadRequestProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
    assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(request.copy(connectionId = null)))) }
  }

  @Test
  fun `source creation checks saved definition kind and definition use after workspace permission`() {
    val f = Fixture()
    val rule = SourceWriteRule(f.data, f.permissions)
    val request =
      SourceCreate(sourceDefinitionId = DEFINITION, connectionConfiguration = Jsons.emptyObject(), workspaceId = WORKSPACE, name = "source")
    every { f.data.canUseActorDefinition(DEFINITION, OWNER) } returns false
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
    every { f.data.canUseActorDefinition(DEFINITION, OWNER) } returns true
    assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
    verify { f.data.requireActorDefinitionType(DEFINITION, true) }
  }

  @Test
  fun `get for job checks exact saved connection even when both connections have the same owner`() {
    val f = Fixture()
    val rule = ConnectionReadOrWorkerRule(f.data, f.permissions)
    val request = ConnectionAndJobIdRequestBody(connectionId = CONNECTION, jobId = 0L)
    every { f.data.replicationJobConnectionId(0L) } returns OTHER_CONNECTION
    assertThrows(BadRequestProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
    every { f.data.replicationJobConnectionId(0L) } returns CONNECTION
    assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(request))) }
  }

  @Test
  fun `Fusion search checks every nested source and optional destination in the saved workspace`() {
    val f = Fixture()
    val rule = FusionDestinationSearchRule(f.data, f.permissions)
    val request =
      FusionSearchRequest(
        type = FusionSearchRequest.Type.KEYWORD,
        prompt = "query",
        streams =
          listOf(
            FusionSearchStreamSelector(streamName = "stream", sourceId = SOURCE),
            FusionSearchStreamSelector(streamName = "stream", sourceId = OTHER_SOURCE),
          ),
      )
    every { f.data.savedSourceWorkspaceAndOrganization(OTHER_SOURCE) } returns OTHER_OWNER
    assertThrows(AbstractThrowableProblem::class.java) { rule.authorize(f.caller, rule.readInput(arrayOf(DESTINATION, request))) }
    every { f.data.savedSourceWorkspaceAndOrganization(OTHER_SOURCE) } returns OWNER
    assertDoesNotThrow { rule.authorize(f.caller, rule.readInput(arrayOf(DESTINATION, request))) }
  }

  @Test
  fun `Fusion resolution rejects a workload without a saved assignment before resolver business work`() {
    val data = mockk<AuthorizationDataService>(relaxed = true)
    every { data.savedConnectionReferences(CONNECTION) } returns
      SavedConnectionReferences(SOURCE, DESTINATION, Optional.empty(), Optional.empty(), emptyList())
    every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns OWNER
    every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns OWNER
    every { data.canUseDataplaneGroup(GROUP, ORGANIZATION) } returns true
    val rule = FusionResolveRule(data, mockk(relaxed = true))
    val request =
      FusionEnablementResolveRequest(
        organizationId = ORGANIZATION,
        workspaceId = WORKSPACE,
        connectionId = CONNECTION,
        sourceId = SOURCE,
        destinationId = DESTINATION,
        dataplaneGroupId = GROUP,
        workloadId = "unassigned-workload",
      )
    val input = rule.readInput(arrayOf(request))
    val caller =
      io.micronaut.security.authentication.Authentication
        .build(UUID(0, 99).toString())
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(caller, input) }
    every { data.isAssignedSyncWorkload(ORGANIZATION, WORKSPACE, CONNECTION, SOURCE, DESTINATION, "unassigned-workload", GROUP, UUID(0, 99)) } returns
      true
    assertDoesNotThrow { rule.authorize(caller, input) }
  }

  @Test
  fun `Fusion execute rules read string path IDs from wrapped controllers`() {
    val f = Fixture()
    assertEquals(
      ActorOperationInput.Saved(SOURCE),
      FusionSourceExecuteRule(f.data, f.permissions).readInput(arrayOf(SOURCE.toString(), "body")),
    )
    assertEquals(
      ActorOperationInput.Saved(DESTINATION),
      FusionDestinationExecuteRule(f.data, f.permissions).readInput(arrayOf(DESTINATION.toString(), "body")),
    )
    assertThrows(BadRequestProblem::class.java) { FusionSourceExecuteRule(f.data, f.permissions).readInput(arrayOf("not-a-uuid")) }
  }

  @Test
  fun `connection deletion rejects saved operations in another workspace despite both grants`() {
    val sync =
      StandardSync()
        .withConnectionId(CONNECTION)
        .withSourceId(SOURCE)
        .withDestinationId(DESTINATION)
        .withOperationIds(listOf(OPERATION))
    val data = savedData(sync)
    val rule =
      declaredRule(
        io.airbyte.server.apis.controllers.ConnectionApiController::class.java,
        "deleteConnection",
        ConnectionIdRequestBody::class.java,
        data,
      )
    assertThrows(BadRequestProblem::class.java) {
      rule.authorize(
        io.micronaut.security.authentication.Authentication
          .build("caller"),
        rule.readInput(arrayOf(ConnectionIdRequestBody(connectionId = CONNECTION))),
      )
    }
  }

  @Test
  fun `postprocess and delete load each saved connection once`() {
    val references =
      SavedConnectionReferences(SOURCE, DESTINATION, Optional.of(CATALOG), Optional.of(CATALOG), listOf(OPERATION))
    val postprocess = Fixture()
    every { postprocess.data.savedConnectionReferences(CONNECTION) } returns references
    every { postprocess.data.destinationCatalogForValidation(CONNECTION, WORKSPACE, references.destinationCatalogId) } returns
      references.destinationCatalogId
    ConnectionWriteRule(postprocess.data, postprocess.permissions).authorize(
      postprocess.caller,
      ConnectionOperationInput.Saved(listOf(CONNECTION), catalogId = Optional.of(CATALOG), schemaChange = true),
    )
    verify(exactly = 1) { postprocess.data.savedConnectionReferences(CONNECTION) }

    val deletion = Fixture()
    every { deletion.data.savedConnectionReferences(CONNECTION) } returns references
    ConnectionDeleteRule(deletion.data, deletion.permissions).authorize(deletion.caller, ConnectionOperationInput.Saved(listOf(CONNECTION)))
    verify(exactly = 1) { deletion.data.savedConnectionReferences(CONNECTION) }
  }

  @Test
  fun `source deletion checks saved destination ownership for every affected connection before business`() {
    val sync = StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE).withDestinationId(OTHER_DESTINATION)
    val data = savedData(sync)
    val rule = declaredRule(io.airbyte.server.apis.controllers.SourceApiController::class.java, "deleteSource", SourceIdRequestBody::class.java, data)
    assertThrows(BadRequestProblem::class.java) {
      rule.authorize(
        io.micronaut.security.authentication.Authentication
          .build("caller"),
        rule.readInput(arrayOf(SourceIdRequestBody(SOURCE))),
      )
    }
  }

  @Test
  fun `destination deletion cannot proceed when the saved cascade lookup fails`() {
    val data = savedData(StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE).withDestinationId(DESTINATION), failCascade = true)
    val rule =
      declaredRule(
        io.airbyte.server.apis.controllers.DestinationApiController::class.java,
        "deleteDestination",
        DestinationIdRequestBody::class.java,
        data,
      )
    assertThrows(IOException::class.java) {
      rule.authorize(
        io.micronaut.security.authentication.Authentication
          .build("caller"),
        rule.readInput(arrayOf(DestinationIdRequestBody(DESTINATION))),
      )
    }
  }

  @Test
  fun `source schema propagation checks every target connections saved owners before business`() {
    val data = savedData(StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE).withDestinationId(OTHER_DESTINATION))
    val rule =
      declaredRule(
        io.airbyte.server.apis.controllers.SourceApiController::class.java,
        "applySchemaChangeForSource",
        SourceAutoPropagateChange::class.java,
        data,
      )
    val request = SourceAutoPropagateChange(catalog = AirbyteCatalog(emptyList()), catalogId = CATALOG, sourceId = SOURCE, workspaceId = WORKSPACE)
    assertThrows(BadRequestProblem::class.java) {
      rule.authorize(
        io.micronaut.security.authentication.Authentication
          .build("caller"),
        rule.readInput(arrayOf(request)),
      )
    }
  }

  @Test
  fun `explicit empty target arrays retain admin and no auth behavior and reject scoped callers`() {
    val saved = mockk<PermissionService>()
    every { saved.getPermissionsByAuthUserId("caller") } returns listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))
    for (security in listOf(true, false)) {
      val permissions = OperationPermissions(saved, RoleResolver(mockk(), mockk(), mockk(), mockk()), security)
      val data = Fixture().data
      val cases =
        listOf(
          SourceReadRule(data, permissions) to ListResourcesForWorkspacesRequestBody(emptyList(), Pagination()),
          DestinationReadRule(data, permissions) to ListResourcesForWorkspacesRequestBody(emptyList(), Pagination()),
          ConnectionReadRule(data, permissions) to ListConnectionsForWorkspacesRequestBody(workspaceIds = emptyList(), userId = UUID(0, 17)),
          ConnectionReadRule(data, permissions) to
            ListConnectionsForWorkspacesRequestBody(workspaceIds = emptyList(), userId = UUID(0, 17), tagIds = listOf(TAG)),
          ConnectionReadRule(data, permissions) to ConnectionStatusesRequestBody(connectionIds = emptyList()),
        )
      for ((rule, body) in cases) {
        val input = rule.readInput(arrayOf(body))
        assertDoesNotThrow {
          rule.authorize(
            io.micronaut.security.authentication.Authentication
              .build("caller"),
            input,
          )
        }
        if (security) {
          assertThrows(ForbiddenProblem::class.java) {
            rule.authorize(
              io.micronaut.security.authentication.Authentication.build(
                "caller",
                emptyList(),
                mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(WORKSPACE.toString())),
              ),
              input,
            )
          }
        }
      }
    }
  }

  @Test
  fun `missing actor bodies are rejected before an empty request policy can apply`() {
    val f = Fixture()
    for ((rule, body) in listOf(
      SourceReadRule(f.data, f.permissions) to null,
      DestinationReadRule(f.data, f.permissions) to null,
    )) {
      assertThrows(BadRequestProblem::class.java) { rule.readInput(arrayOf(body)) }
    }
  }

  private fun declaredRule(
    controller: Class<*>,
    method: String,
    body: Class<*>,
    data: AuthorizationDataService,
  ): OperationRule =
    controller
      .getMethod(method, body)
      .getAnnotation(AuthorizeOperation::class.java)
      .rule.java
      .getConstructor(AuthorizationDataService::class.java, OperationPermissions::class.java)
      .newInstance(data, Fixture().permissions)

  private fun savedData(
    sync: StandardSync,
    failCascade: Boolean = false,
  ): AuthorizationDataService {
    val connections = mockk<ConnectionService>()
    every { connections.getStandardSync(CONNECTION) } returns sync
    every { connections.listWorkspaceStandardSyncs(WORKSPACE, false) } returns listOf(sync)
    if (failCascade) every { connections.listWorkspaceStandardSyncs(WORKSPACE, false) } throws IOException("cascade unavailable")
    val actors = mockk<ActorRepository>()
    every { actors.findSourceWorkspaceId(SOURCE) } returns Optional.of(WORKSPACE)
    every { actors.findDestinationWorkspaceId(DESTINATION) } returns Optional.of(WORKSPACE)
    every { actors.findDestinationWorkspaceId(OTHER_DESTINATION) } returns Optional.of(OTHER_WORKSPACE)
    every { actors.catalogWasFetchedByActor(CATALOG, SOURCE) } returns true
    val workspaces = mockk<WorkspaceService>()
    every { workspaces.getStandardWorkspaceNoSecrets(any(), false) } answers {
      StandardWorkspace().withWorkspaceId(firstArg()).withOrganizationId(if (firstArg<UUID>() == WORKSPACE) ORGANIZATION else OTHER_ORGANIZATION)
    }
    val operations = mockk<OperationService>()
    every { operations.getStandardSyncOperation(OPERATION) } returns
      StandardSyncOperation().withOperationId(OPERATION).withWorkspaceId(OTHER_WORKSPACE)
    val constructor = AuthorizationDataService::class.java.declaredConstructors.single { !it.isSynthetic }
    return constructor.newInstance(
      *constructor.parameterTypes
        .map { type ->
          when (type) {
            ConnectionService::class.java -> connections
            ActorRepository::class.java -> actors
            WorkspaceService::class.java -> workspaces
            OperationService::class.java -> operations
            else -> mockkClass(type.kotlin, relaxed = true)
          }
        }.toTypedArray(),
    ) as AuthorizationDataService
  }

  private class Fixture(
    granted: Boolean = true,
    security: Boolean = true,
  ) {
    val data = mockk<AuthorizationDataService>()
    val caller =
      io.micronaut.security.authentication.Authentication
        .build("caller")
    private val savedPermissions = mockk<PermissionService>()
    val permissions = OperationPermissions(savedPermissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), security)

    init {
      every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns OWNER
      every { data.savedSourceWorkspaceAndOrganization(OTHER_SOURCE) } returns OWNER
      every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns OWNER
      every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
      every { data.savedConnectionReferences(CONNECTION) } returns
        SavedConnectionReferences(SOURCE, DESTINATION, Optional.empty(), Optional.empty(), emptyList())
      every { data.destinationCatalogForValidation(CONNECTION, WORKSPACE, any()) } returns Optional.empty()
      every { data.workspaceAndOrganization(WORKSPACE) } returns OWNER
      every { data.operationWorkspaceAndOrganization(any()) } returns OWNER
      every { data.tagWorkspaceAndOrganization(any()) } returns OWNER
      every { data.canUseDataplaneGroup(any(), ORGANIZATION) } returns true
      every { data.catalogWasFetchedByActor(any(), any()) } returns true
      every { data.requireActorDefinitionType(any(), any()) } just Runs
      every { data.canUseActorDefinition(any(), any()) } returns true
      every { savedPermissions.getPermissionsByAuthUserId("caller") } returns
        if (granted) {
          listOf(
            Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR),
            Permission().withWorkspaceId(OTHER_WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR),
          )
        } else {
          emptyList()
        }
    }
  }

  companion object {
    val SOURCE: UUID = UUID(0, 1)
    val DESTINATION: UUID = UUID(0, 2)
    val CONNECTION: UUID = UUID(0, 3)
    val WORKSPACE: UUID = UUID(0, 4)
    val ORGANIZATION: UUID = UUID(0, 5)
    val OTHER_WORKSPACE: UUID = UUID(0, 6)
    val OTHER_ORGANIZATION: UUID = UUID(0, 7)
    val OTHER_SOURCE: UUID = UUID(0, 8)
    val OPERATION: UUID = UUID(0, 9)
    val OTHER_OPERATION: UUID = UUID(0, 10)
    val CATALOG: UUID = UUID(0, 11)
    val TAG: UUID = UUID(0, 12)
    val GROUP: UUID = UUID(0, 13)
    val DEFINITION: UUID = UUID(0, 14)
    val OTHER_CONNECTION: UUID = UUID(0, 15)
    val OTHER_DESTINATION: UUID = UUID(0, 16)
    val OWNER = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
    val OTHER_OWNER = WorkspaceAndOrganization(OTHER_WORKSPACE, OTHER_ORGANIZATION)

    fun create(): ConnectionCreate = ConnectionCreate(sourceId = SOURCE, destinationId = DESTINATION, status = ConnectionStatus.ACTIVE)
  }
}
