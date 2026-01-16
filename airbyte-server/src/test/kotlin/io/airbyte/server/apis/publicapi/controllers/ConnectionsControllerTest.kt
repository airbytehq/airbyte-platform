/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.problems.throwable.generated.ConnectionLockedProblem
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.UnimitedConnectionsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.StatusReason
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.domain.models.OrganizationId
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ConnectionStatusEnum
import io.airbyte.publicApi.server.generated.models.StreamConfigurations
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.airbyte.server.apis.publicapi.services.DestinationService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkObject
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.Callable

class ConnectionsControllerTest {
  private lateinit var controller: ConnectionsController
  private val connectionService: ConnectionService = mockk()
  private val sourceService: SourceService = mockk()
  private val destinationService: DestinationService = mockk()
  private val trackingHelper: TrackingHelper = mockk(relaxed = true)
  private val roleResolver: RoleResolver = mockk(relaxed = true)
  private val currentUserService: CurrentUserService = mockk()
  private val destinationDiscoverService: DestinationDiscoverService = mockk(relaxed = true)
  private val workspaceHelper: WorkspaceHelper = mockk()
  private val entitlementService: EntitlementService = mockk()
  private val connectionDataService: io.airbyte.data.services.ConnectionService = mockk()

  @BeforeEach
  fun setUp() {
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser()
    every { currentUserService.getCurrentUser().userId } returns UUID.randomUUID()

    // Mock trackingHelper to just execute the passed function
    every { trackingHelper.callWithTracker<Any>(any(), any(), any(), any()) } answers {
      (firstArg() as Callable<Any>).call()
    }

    controller =
      ConnectionsController(
        connectionService = connectionService,
        sourceService = sourceService,
        destinationService = destinationService,
        trackingHelper = trackingHelper,
        roleResolver = roleResolver,
        currentUserService = currentUserService,
        destinationDiscoverService = destinationDiscoverService,
        workspaceHelper = workspaceHelper,
        entitlementService = entitlementService,
        connectionDataService = connectionDataService,
      )
  }

  @Test
  fun `patchConnection throws ConnectionLockedProblem when connection is locked`() {
    val connectionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    every { connectionService.getConnection(connectionId) } returns
      mockk {
        every { this@mockk.destinationId } returns destinationId.toString()
        every { this@mockk.status } returns ConnectionStatusEnum.LOCKED
        every { this@mockk.statusReason } returns StatusReason.SUBSCRIPTION_DOWNGRADED_ACCESS_REVOKED.value
      }

    every { destinationService.getDestinationRead(destinationId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId
      }

    org.junit.jupiter.api.Assertions.assertThrows(ConnectionLockedProblem::class.java) {
      controller.patchConnection(
        connectionId.toString(),
        ConnectionPatchRequest(status = ConnectionStatusEnum.ACTIVE),
      )
    }
  }

  @Test
  fun `patchConnection does not set configurations if not being patched`() {
    val connectionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    every {
      connectionService.getConnection(connectionId)
    } returns
      mockk {
        every { this@mockk.destinationId } returns destinationId.toString()
        every { this@mockk.status } returns ConnectionStatusEnum.ACTIVE
      }

    every { destinationService.getDestinationRead(destinationId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId
      }

    val expectedConnectionPatchRequest =
      ConnectionPatchRequest(
        status = ConnectionStatusEnum.INACTIVE,
      )

    every {
      connectionService.updateConnection(
        connectionId,
        expectedConnectionPatchRequest,
        null,
        null,
        workspaceId,
      )
    } returns mockk()

    controller.patchConnection(
      connectionId.toString(),
      ConnectionPatchRequest(
        status = ConnectionStatusEnum.INACTIVE,
      ),
    )

    verify {
      connectionService.updateConnection(
        connectionId,
        expectedConnectionPatchRequest,
        null,
        null,
        workspaceId,
      )
    }
  }

  @Test
  fun `patchConnection sets configurations when being patched`() {
    val connectionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    every {
      connectionService.getConnection(connectionId)
    } returns
      mockk {
        every { this@mockk.destinationId } returns destinationId.toString()
        every { this@mockk.sourceId } returns sourceId.toString()
        every { this@mockk.status } returns ConnectionStatusEnum.ACTIVE
      }

    every { destinationService.getDestinationRead(destinationId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId
      }

    val mockSchemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns mockk()
      }

    every { sourceService.getSourceSchema(sourceId, false) } returns mockSchemaResponse
    every { destinationService.getDestinationSyncModes(any<DestinationRead>()) } returns mockk()

    val configurations =
      mockk<StreamConfigurations> {
        every { streams } returns listOf(mockk())
      }
    val expectedConnectionPatchRequest =
      ConnectionPatchRequest(
        status = ConnectionStatusEnum.INACTIVE,
        configurations = configurations,
      )

    // Mock the AirbyteCatalogHelper.getValidConfiguredStreams call
    mockkObject(AirbyteCatalogHelper)
    val mockedConfiguredStreams = listOf(mockk<AirbyteStreamAndConfiguration>())
    every {
      AirbyteCatalogHelper.getValidConfiguredStreams(
        any(),
        any(),
        any(),
      )
    } returns mockedConfiguredStreams

    val expectedConfiguredCatalog = AirbyteCatalog().streams(mockedConfiguredStreams)

    every {
      connectionService.updateConnection(
        connectionId,
        expectedConnectionPatchRequest,
        catalogId,
        expectedConfiguredCatalog,
        workspaceId,
      )
    } returns mockk()

    controller.patchConnection(
      connectionId.toString(),
      ConnectionPatchRequest(
        status = ConnectionStatusEnum.INACTIVE,
        configurations = configurations,
      ),
    )

    verify {
      connectionService.updateConnection(
        connectionId,
        expectedConnectionPatchRequest,
        catalogId,
        expectedConfiguredCatalog,
        workspaceId,
      )
    }

    // Clean up the mock
    unmockkObject(AirbyteCatalogHelper)
  }

  @Test
  fun `publicCreateConnection succeeds with unlimited entitlement`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    val connectionCreateRequest = mockk<io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest>(relaxed = true)
    every { connectionCreateRequest.destinationId } returns destinationId
    every { connectionCreateRequest.sourceId } returns sourceId
    every { connectionCreateRequest.schedule } returns null
    every { connectionCreateRequest.configurations } returns null

    val destinationRead =
      mockk<DestinationRead> {
        every { this@mockk.workspaceId } returns workspaceId
      }

    val airbyteCatalog =
      mockk<AirbyteCatalog> {
        every { streams } returns emptyList()
      }

    val schemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns airbyteCatalog
      }

    every { destinationService.getDestinationRead(destinationId) } returns destinationRead
    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse
    every { destinationService.getDestinationSyncModes(destinationRead) } returns emptyList()
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every { entitlementService.checkEntitlement(OrganizationId(organizationId), UnimitedConnectionsEntitlement) } returns
      EntitlementResult("feature-unlimited-n-connections", true)
    every { connectionService.createConnection(any(), any(), any(), any(), any()) } returns mockk()

    controller.publicCreateConnection(connectionCreateRequest)

    verify(exactly = 0) { connectionDataService.countConnectionsForOrganization(organizationId) }
    verify(exactly = 1) { connectionService.createConnection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `publicCreateConnection succeeds when under limit`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    val connectionCreateRequest = mockk<io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest>(relaxed = true)
    every { connectionCreateRequest.destinationId } returns destinationId
    every { connectionCreateRequest.sourceId } returns sourceId
    every { connectionCreateRequest.schedule } returns null
    every { connectionCreateRequest.configurations } returns null

    val destinationRead =
      mockk<DestinationRead> {
        every { this@mockk.workspaceId } returns workspaceId
      }

    val airbyteCatalog2 =
      mockk<AirbyteCatalog> {
        every { streams } returns emptyList()
      }

    val schemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns airbyteCatalog2
      }

    every { destinationService.getDestinationRead(destinationId) } returns destinationRead
    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse
    every { destinationService.getDestinationSyncModes(destinationRead) } returns emptyList()
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every { entitlementService.checkEntitlement(OrganizationId(organizationId), UnimitedConnectionsEntitlement) } returns
      EntitlementResult("feature-unlimited-n-connections", false)
    every { connectionDataService.countConnectionsForOrganization(organizationId) } returns 50
    every { connectionService.createConnection(any(), any(), any(), any(), any()) } returns mockk()

    controller.publicCreateConnection(connectionCreateRequest)

    verify(exactly = 1) { connectionDataService.countConnectionsForOrganization(organizationId) }
    verify(exactly = 1) { connectionService.createConnection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `publicCreateConnection throws exception at limit`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    val connectionCreateRequest = mockk<io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest>(relaxed = true)
    every { connectionCreateRequest.destinationId } returns destinationId
    every { connectionCreateRequest.sourceId } returns sourceId
    every { connectionCreateRequest.schedule } returns null
    every { connectionCreateRequest.configurations } returns null

    val destinationRead =
      mockk<DestinationRead> {
        every { this@mockk.workspaceId } returns workspaceId
      }

    val schemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns mockk<AirbyteCatalog>()
      }

    every { destinationService.getDestinationRead(destinationId) } returns destinationRead
    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every { entitlementService.checkEntitlement(OrganizationId(organizationId), UnimitedConnectionsEntitlement) } returns
      EntitlementResult(UnimitedConnectionsEntitlement.featureId, false)
    every { connectionDataService.countConnectionsForOrganization(organizationId) } returns ConnectionsController.CONNECTION_LIMIT

    val exception =
      org.junit.jupiter.api.Assertions.assertThrows(ApplicationErrorKnownException::class.java) {
        controller.publicCreateConnection(connectionCreateRequest)
      }

    org.junit.jupiter.api.Assertions
      .assertTrue(exception.message!!.contains("reached the maximum limit of ${ConnectionsController.CONNECTION_LIMIT} connections"))
    verify(exactly = 1) { connectionDataService.countConnectionsForOrganization(organizationId) }
    verify(exactly = 0) { connectionService.createConnection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `publicCreateConnection throws exception over limit`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()

    val connectionCreateRequest = mockk<io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest>(relaxed = true)
    every { connectionCreateRequest.destinationId } returns destinationId
    every { connectionCreateRequest.sourceId } returns sourceId
    every { connectionCreateRequest.schedule } returns null
    every { connectionCreateRequest.configurations } returns null

    val destinationRead =
      mockk<DestinationRead> {
        every { this@mockk.workspaceId } returns workspaceId
      }

    val schemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns mockk<AirbyteCatalog>()
      }

    every { destinationService.getDestinationRead(destinationId) } returns destinationRead
    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every { entitlementService.checkEntitlement(OrganizationId(organizationId), UnimitedConnectionsEntitlement) } returns
      EntitlementResult(UnimitedConnectionsEntitlement.featureId, false)
    every { connectionDataService.countConnectionsForOrganization(organizationId) } returns 150

    val exception =
      org.junit.jupiter.api.Assertions.assertThrows(ApplicationErrorKnownException::class.java) {
        controller.publicCreateConnection(connectionCreateRequest)
      }

    org.junit.jupiter.api.Assertions
      .assertTrue(exception.message!!.contains("reached the maximum limit of ${ConnectionsController.CONNECTION_LIMIT} connections"))
    verify(exactly = 1) { connectionDataService.countConnectionsForOrganization(organizationId) }
    verify(exactly = 0) { connectionService.createConnection(any(), any(), any(), any(), any()) }
  }
}
