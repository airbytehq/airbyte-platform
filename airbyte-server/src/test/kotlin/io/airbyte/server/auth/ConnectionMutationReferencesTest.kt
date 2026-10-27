/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.ConnectionUpdateWithReason
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.featureflag.EnableDestinationCatalogValidation
import io.airbyte.featureflag.FeatureFlagClient
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.Optional
import java.util.UUID

class ConnectionMutationReferencesTest {
  @Test
  fun `create update and nested update accept shared default regions for a customer organization`() {
    val f = Fixture(DEFAULT_ORGANIZATION_ID)
    for (body in mutations()) assertDoesNotThrow { f.authorize(body) }
  }

  @Test
  fun `create update and nested update accept entitled PrivateLink and reject unentitled use`() {
    val f = Fixture(PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)
    for (body in mutations()) {
      f.entitled = true
      assertDoesNotThrow { f.authorize(body) }
      f.entitled = false
      assertThrows(BadRequestProblem::class.java) { f.authorize(body) }
    }
  }

  @Test
  fun `create update and nested update retain own custom regions and reject foreign customer regions`() {
    for (body in mutations()) {
      assertDoesNotThrow { Fixture(ORGANIZATION).authorize(body) }
      assertThrows(BadRequestProblem::class.java) { Fixture(UUID(0, 999)).authorize(body) }
    }
  }

  @Test
  fun `ordinary and nested sync catalog updates reject a saved destination catalog fetched by another actor despite both grants`() {
    val f = Fixture(ORGANIZATION)
    every { f.actors.catalogWasFetchedByActor(CATALOG, DESTINATION) } returns false
    for (body in updates()) assertThrows(BadRequestProblem::class.java) { f.authorize(body) }
  }

  @Test
  fun `ordinary and nested sync catalog updates validate a valid saved fallback before succeeding`() {
    val f = Fixture(ORGANIZATION)
    for (body in updates()) assertDoesNotThrow { f.authorize(body) }
    verify(exactly = 2) { f.actors.catalogWasFetchedByActor(CATALOG, DESTINATION) }
  }

  @Test
  fun `ordinary and nested saved fallback lookup failures propagate`() {
    for (body in updates()) {
      val f = Fixture(ORGANIZATION)
      every { f.actors.catalogWasFetchedByActor(CATALOG, DESTINATION) } throws IOException("catalog unavailable")
      assertThrows(IOException::class.java) { f.authorize(body) }
    }
    for (body in updates()) {
      val f = Fixture(ORGANIZATION)
      every { f.connections.getStandardSync(CONNECTION) } returns StandardSync().withConnectionId(UUID(0, 999))
      assertThrows(AuthorizationResourceNotFoundException::class.java) { f.authorize(body) }
    }
  }

  @Test
  fun `saved fallback is unused without a sync catalog or when destination validation is off`() {
    val f = Fixture(ORGANIZATION)
    assertDoesNotThrow { f.authorize(ConnectionUpdate(connectionId = CONNECTION)) }
    f.validationEnabled = false
    for (body in updates()) assertDoesNotThrow { f.authorize(body) }
    verify(exactly = 0) { f.actors.catalogWasFetchedByActor(any(), any()) }
  }

  @Test
  fun `explicit destination catalog replaces the saved fallback and absent saved catalogs remain optional`() {
    val f = Fixture(ORGANIZATION)
    val replacement = UUID(0, 210)
    every { f.actors.catalogWasFetchedByActor(replacement, DESTINATION) } returns true
    every { f.actors.catalogWasFetchedByActor(CATALOG, DESTINATION) } returns false
    assertDoesNotThrow {
      f.authorize(
        ConnectionUpdate(connectionId = CONNECTION, syncCatalog = AirbyteCatalog(streams = emptyList()), destinationCatalogId = replacement),
      )
    }
    f.sync.destinationCatalogId = null
    for (body in updates()) assertDoesNotThrow { f.authorize(body) }
    verify(exactly = 0) { f.actors.catalogWasFetchedByActor(CATALOG, DESTINATION) }
  }

  private class Fixture(
    groupOwner: UUID,
  ) {
    var entitled = false
    var validationEnabled = true
    val actors = mockk<ActorRepository>()
    val connections = mockk<ConnectionService>()
    val sync =
      StandardSync()
        .withConnectionId(CONNECTION)
        .withSourceId(SOURCE)
        .withDestinationId(DESTINATION)
        .withDestinationCatalogId(CATALOG)
    private val workspaces = mockk<WorkspaceService>()
    private val groups = mockk<DataplaneGroupService>()
    private val organizations = mockk<OrganizationService>()
    private val entitlements = mockk<EntitlementService>()
    private val flags = mockk<FeatureFlagClient>()
    private val permissions = mockk<PermissionService>()
    private val rule: ConnectionMutationRule

    init {
      every { actors.findSourceWorkspaceId(SOURCE) } returns Optional.of(WORKSPACE)
      every { actors.findDestinationWorkspaceId(DESTINATION) } returns Optional.of(WORKSPACE)
      every { actors.catalogWasFetchedByActor(CATALOG, DESTINATION) } returns true
      every { connections.getStandardSync(CONNECTION) } returns sync
      every { workspaces.getStandardWorkspaceNoSecrets(WORKSPACE, false) } returns
        StandardWorkspace().withWorkspaceId(WORKSPACE).withOrganizationId(ORGANIZATION)
      val group =
        DataplaneGroup()
          .withId(GROUP)
          .withOrganizationId(groupOwner)
          .withEnabled(true)
          .withTombstone(false)
      every { groups.getDataplaneGroup(GROUP) } returns group
      every { groups.listDefaultDataplaneGroups() } returns if (groupOwner == DEFAULT_ORGANIZATION_ID) listOf(group) else emptyList()
      every { organizations.getOrganization(groupOwner) } returns Optional.of(Organization().withOrganizationId(groupOwner))
      every { entitlements.checkEntitlement(OrganizationId(ORGANIZATION), PrivateLinkEntitlement) } answers
        { EntitlementResult(PrivateLinkEntitlement.featureId, entitled) }
      every { flags.boolVariation(EnableDestinationCatalogValidation, any()) } answers { validationEnabled }
      every { permissions.getPermissionsByAuthUserId("caller") } returns
        listOf(
          Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR),
          Permission().withWorkspaceId(UUID(0, 998)).withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR),
        )
      val dependencies = listOf(actors, connections, workspaces, groups, organizations, entitlements, flags)
      val constructor = AuthorizationDataService::class.java.declaredConstructors.single { !it.isSynthetic }
      val data =
        constructor.newInstance(
          *constructor.parameterTypes
            .map { type ->
              dependencies.singleOrNull { type.isInstance(it) } ?: mockkClass(type.kotlin)
            }.toTypedArray(),
        ) as AuthorizationDataService
      rule = ConnectionMutationRule(data, OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true))
    }

    fun authorize(body: Any) = rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(body)))
  }

  companion object {
    private val SOURCE = UUID(0, 201)
    private val DESTINATION = UUID(0, 202)
    private val CONNECTION = UUID(0, 203)
    private val WORKSPACE = UUID(0, 204)
    private val ORGANIZATION = UUID(0, 205)
    private val CATALOG = UUID(0, 206)
    private val GROUP = UUID(0, 207)

    private fun updates(): List<Any> =
      listOf(
        ConnectionUpdate(connectionId = CONNECTION, syncCatalog = AirbyteCatalog(streams = emptyList())),
        ConnectionUpdateWithReason(ConnectionUpdate(connectionId = CONNECTION, syncCatalog = AirbyteCatalog(streams = emptyList()))),
      )

    private fun mutations(): List<Any> =
      listOf(
        ConnectionCreate(sourceId = SOURCE, destinationId = DESTINATION, status = ConnectionStatus.ACTIVE, dataplaneGroupId = GROUP),
        ConnectionUpdate(connectionId = CONNECTION, dataplaneGroupId = GROUP),
        ConnectionUpdateWithReason(ConnectionUpdate(connectionId = CONNECTION, dataplaneGroupId = GROUP)),
      )
  }
}
