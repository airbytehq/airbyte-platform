/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataCron
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.problems.throwable.generated.ConnectionLockedProblem
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.services.DestinationDiscoverService
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.StatusReason
import io.airbyte.publicApi.server.generated.models.AirbyteApiConnectionSchedule
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.ConnectionResponse
import io.airbyte.publicApi.server.generated.models.ConnectionStatusEnum
import io.airbyte.publicApi.server.generated.models.ScheduleTypeEnum
import io.airbyte.publicApi.server.generated.models.StreamConfigurations
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.helpers.AirbyteCatalogHelper
import io.airbyte.server.apis.publicapi.mappers.ConnectionCreateMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionReadMapper
import io.airbyte.server.apis.publicapi.mappers.ConnectionUpdateMapper
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.airbyte.server.apis.publicapi.services.DestinationService
import io.airbyte.server.apis.publicapi.services.SourceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.slot
import io.mockk.unmockkObject
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
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
  fun `patchConnection normalizes a suffixed cron timezone before updating`() {
    val connectionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val capturedRequest = slot<ConnectionPatchRequest>()

    every {
      connectionService.getConnection(connectionId)
    } returns
      mockk<ConnectionResponse> {
        every { this@mockk.destinationId } returns destinationId.toString()
        every { this@mockk.status } returns ConnectionStatusEnum.ACTIVE
      }
    every { destinationService.getDestinationRead(destinationId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId
      }
    every {
      connectionService.updateConnection(
        connectionId,
        capture(capturedRequest),
        null,
        null,
        workspaceId,
      )
    } returns mockk()

    controller.patchConnection(
      connectionId.toString(),
      ConnectionPatchRequest(
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = "0 0 */3 * * ? US/Pacific",
          ),
      ),
    )

    val connectionUpdate =
      ConnectionUpdateMapper.from(
        connectionId,
        capturedRequest.captured,
        null,
        null,
      )
    assertEquals("0 0 */3 * * ?", connectionUpdate.scheduleData!!.cron!!.cronExpression)
    assertEquals("US/Pacific", connectionUpdate.scheduleData!!.cron!!.cronTimeZone)
  }

  @Test
  fun `patchConnection round trips the cron timezone emitted by the read mapper`() {
    val connectionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val connectionRead =
      ConnectionRead().apply {
        this.connectionId = UUID.randomUUID()
        name = "testconnection"
        status = ConnectionStatus.ACTIVE
        scheduleType = ConnectionScheduleType.CRON
        scheduleData =
          ConnectionScheduleData().apply {
            cron =
              ConnectionScheduleDataCron().apply {
                cronExpression = "0 0 */3 * * ?"
                cronTimeZone = "US/Pacific"
              }
          }
        sourceId = UUID.randomUUID()
        this.destinationId = destinationId
        createdAt = 1L
      }
    val publicSchedule = ConnectionReadMapper.from(connectionRead, workspaceId).schedule
    val capturedRequest = slot<ConnectionPatchRequest>()

    every {
      connectionService.getConnection(connectionId)
    } returns
      mockk<ConnectionResponse> {
        every { this@mockk.destinationId } returns destinationId.toString()
        every { this@mockk.status } returns ConnectionStatusEnum.ACTIVE
      }
    every { destinationService.getDestinationRead(destinationId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId
      }
    every {
      connectionService.updateConnection(
        connectionId,
        capture(capturedRequest),
        null,
        null,
        workspaceId,
      )
    } returns mockk()

    controller.patchConnection(
      connectionId.toString(),
      ConnectionPatchRequest(
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = publicSchedule.cronExpression,
          ),
      ),
    )

    val connectionUpdate =
      ConnectionUpdateMapper.from(
        connectionId,
        capturedRequest.captured,
        null,
        null,
      )
    assertEquals("0 0 */3 * * ?", connectionUpdate.scheduleData!!.cron!!.cronExpression)
    assertEquals("US/Pacific", connectionUpdate.scheduleData!!.cron!!.cronTimeZone)
  }

  @Test
  fun `publicCreateConnection normalizes a suffixed cron timezone before creating`() {
    val workspaceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()
    val capturedRequest = slot<ConnectionCreateRequest>()
    val destinationRead =
      mockk<DestinationRead> {
        every { this@mockk.workspaceId } returns workspaceId
      }
    val airbyteCatalog = AirbyteCatalog().apply { streams = emptyList() }
    val schemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns airbyteCatalog
      }

    every { destinationService.getDestinationRead(destinationId) } returns destinationRead
    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse
    every { destinationService.getDestinationSyncModes(destinationRead) } returns emptyList()
    every {
      connectionService.createConnection(
        capture(capturedRequest),
        catalogId,
        any(),
        any(),
        workspaceId,
      )
    } returns mockk()

    controller.publicCreateConnection(
      ConnectionCreateRequest(
        sourceId = sourceId,
        destinationId = destinationId,
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = "0 0 */3 * * ? US/Pacific",
          ),
      ),
    )

    val createdConnection =
      ConnectionCreateMapper.from(
        capturedRequest.captured,
        catalogId,
        null,
        airbyteCatalog,
      )
    assertEquals("0 0 */3 * * ?", createdConnection.scheduleData!!.cron!!.cronExpression)
    assertEquals("US/Pacific", createdConnection.scheduleData!!.cron!!.cronTimeZone)
  }

  @Test
  fun `publicCreateConnection preserves an explicit timezone over a suffix`() {
    val workspaceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()
    val capturedRequest = slot<ConnectionCreateRequest>()
    val destinationRead =
      mockk<DestinationRead> {
        every { this@mockk.workspaceId } returns workspaceId
      }
    val airbyteCatalog = AirbyteCatalog().apply { streams = emptyList() }
    val schemaResponse =
      mockk<SourceDiscoverSchemaRead> {
        every { this@mockk.catalogId } returns catalogId
        every { this@mockk.catalog } returns airbyteCatalog
      }

    every { destinationService.getDestinationRead(destinationId) } returns destinationRead
    every { sourceService.getSourceSchema(sourceId, false) } returns schemaResponse
    every { destinationService.getDestinationSyncModes(destinationRead) } returns emptyList()
    every {
      connectionService.createConnection(
        capture(capturedRequest),
        catalogId,
        any(),
        any(),
        workspaceId,
      )
    } returns mockk()

    controller.publicCreateConnection(
      ConnectionCreateRequest(
        sourceId = sourceId,
        destinationId = destinationId,
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = "0 0 */3 * * ? US/Pacific",
            cronTimeZone = "Europe/Paris",
          ),
      ),
    )

    val createdConnection =
      ConnectionCreateMapper.from(
        capturedRequest.captured,
        catalogId,
        null,
        airbyteCatalog,
      )
    assertEquals("0 0 */3 * * ?", createdConnection.scheduleData!!.cron!!.cronExpression)
    assertEquals("Europe/Paris", createdConnection.scheduleData!!.cron!!.cronTimeZone)
  }

  @Test
  fun `patchConnection round trips a fixed offset from an explicit create timezone`() {
    val connectionId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()
    val catalog = AirbyteCatalog().apply { streams = emptyList() }
    val createdConnection =
      ConnectionCreateMapper.from(
        ConnectionCreateRequest(
          sourceId = sourceId,
          destinationId = destinationId,
          schedule =
            AirbyteApiConnectionSchedule(
              scheduleType = ScheduleTypeEnum.CRON,
              cronExpression = "0 0 */3 * * ?",
              cronTimeZone = "+05:30",
            ),
        ),
        catalogId,
        null,
        catalog,
      )
    val connectionRead =
      ConnectionRead().apply {
        this.connectionId = connectionId
        name = "testconnection"
        status = ConnectionStatus.ACTIVE
        scheduleType = ConnectionScheduleType.CRON
        scheduleData = createdConnection.scheduleData
        this.sourceId = sourceId
        this.destinationId = destinationId
        createdAt = 1L
      }
    val publicSchedule = ConnectionReadMapper.from(connectionRead, workspaceId).schedule
    val capturedRequest = slot<ConnectionPatchRequest>()

    every {
      connectionService.getConnection(connectionId)
    } returns
      mockk<ConnectionResponse> {
        every { this@mockk.destinationId } returns destinationId.toString()
        every { this@mockk.status } returns ConnectionStatusEnum.ACTIVE
      }
    every { destinationService.getDestinationRead(destinationId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId
      }
    every {
      connectionService.updateConnection(
        connectionId,
        capture(capturedRequest),
        null,
        null,
        workspaceId,
      )
    } returns mockk()

    controller.patchConnection(
      connectionId.toString(),
      ConnectionPatchRequest(
        schedule =
          AirbyteApiConnectionSchedule(
            scheduleType = ScheduleTypeEnum.CRON,
            cronExpression = publicSchedule.cronExpression,
          ),
      ),
    )

    val connectionUpdate =
      ConnectionUpdateMapper.from(
        connectionId,
        capturedRequest.captured,
        null,
        null,
      )
    assertEquals("0 0 */3 * * ?", connectionUpdate.scheduleData!!.cron!!.cronExpression)
    assertEquals("+05:30", connectionUpdate.scheduleData!!.cron!!.cronTimeZone)
  }

  @Test
  fun `publicCreateConnection succeeds`() {
    val workspaceId = UUID.randomUUID()
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
    every { connectionService.createConnection(any(), any(), any(), any(), any()) } returns mockk()

    controller.publicCreateConnection(connectionCreateRequest)

    verify(exactly = 1) { connectionService.createConnection(any(), any(), any(), any(), any()) }
  }
}
