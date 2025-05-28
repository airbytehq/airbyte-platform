/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
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
import io.mockk.mockkConstructor
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
  private val roleResolver: RoleResolver = mockk()
  private val currentUserService: CurrentUserService = mockk()

  @BeforeEach
  fun setUp() {
    every { currentUserService.currentUser } returns AuthenticatedUser()
    every { currentUserService.currentUser.userId } returns UUID.randomUUID()
    mockkConstructor(RoleResolver.Request::class)
    every { anyConstructed<RoleResolver.Request>().withCurrentUser() } returns
      mockk {
        every { withRef(any(), any<String>()) } returns this
        every { requireRole(any()) } returns Unit
      }

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
      )
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
}
