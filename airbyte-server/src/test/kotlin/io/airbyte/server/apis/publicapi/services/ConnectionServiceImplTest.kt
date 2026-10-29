/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionScheduleType
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.helpers.widenColumnTypesContext
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.WidenColumnTypes
import io.airbyte.publicApi.server.generated.models.ConnectionPatchRequest
import io.airbyte.publicApi.server.generated.models.SourceResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ConnectionServiceImplTest {
  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val sourceDefinitionId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()

  private val savedSchema = schema("""{"type": "string", "airbyte_source_type": "integer"}""")
  private val discoveredSchema = schema("""{"type": "number", "airbyte_type": "integer"}""")

  private lateinit var sourceService: SourceService
  private lateinit var connectionHandler: ConnectionsHandler
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var connectionService: ConnectionServiceImpl
  private val update = slot<ConnectionUpdate>()

  @BeforeEach
  fun setup() {
    sourceService = mockk()
    every { sourceService.getSource(sourceId, false) } returns sourceResponse()
    connectionHandler = mockk()
    every { connectionHandler.getConnection(connectionId) } returns connectionRead(savedSchema)
    every { connectionHandler.updateConnection(capture(update), null, false) } returns connectionRead(savedSchema)
    featureFlagClient = mockk()
    every { featureFlagClient.boolVariation(any(), any()) } returns false
    val workspaceHelper: WorkspaceHelper = mockk()
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    connectionService = ConnectionServiceImpl(mockk(), sourceService, connectionHandler, mockk(), mockk(), featureFlagClient, workspaceHelper)
  }

  @Test
  fun `flag off saves the discovered schema`() {
    patch(catalog(discoveredSchema))

    assertEquals(discoveredSchema, savedOrderSchema())
  }

  @Test
  fun `flag on keeps a widened column`() {
    every { featureFlagClient.boolVariation(WidenColumnTypes, widenColumnTypesContext(organizationId, sourceDefinitionId)) } returns true

    patch(catalog(discoveredSchema))

    assertEquals(savedSchema, savedOrderSchema())
  }

  @Test
  fun `flag on takes a stream that wasn't saved as is`() {
    every { featureFlagClient.boolVariation(WidenColumnTypes, widenColumnTypesContext(organizationId, sourceDefinitionId)) } returns true

    patch(catalog(discoveredSchema, name = "new_stream"))

    assertEquals(discoveredSchema, savedOrderSchema())
  }

  @Test
  fun `flag on doesn't widen a changed column the request makes a primary key`() {
    every { featureFlagClient.boolVariation(WidenColumnTypes, widenColumnTypesContext(organizationId, sourceDefinitionId)) } returns true
    every { connectionHandler.getConnection(connectionId) } returns connectionRead(schema("""{"type": "string"}"""))

    patch(
      catalog(discoveredSchema, config = config().copy(destinationSyncMode = DestinationSyncMode.APPEND_DEDUP, primaryKey = listOf(listOf(ORDER)))),
    )

    assertEquals(discoveredSchema, savedOrderSchema())
  }

  @Test
  fun `flag on widens a changed column`() {
    every { featureFlagClient.boolVariation(WidenColumnTypes, widenColumnTypesContext(organizationId, sourceDefinitionId)) } returns true
    every { connectionHandler.getConnection(connectionId) } returns connectionRead(schema("""{"type": "string"}"""))

    patch(catalog(discoveredSchema))

    assertEquals(savedSchema, savedOrderSchema())
  }

  @Test
  fun `a patch without stream configurations doesn't look up the saved catalog`() {
    connectionService.updateConnection(connectionId, ConnectionPatchRequest(), null, null, workspaceId)

    verify(exactly = 0) { connectionHandler.getConnection(any()) }
    assertEquals(null, update.captured.syncCatalog)
  }

  private fun patch(catalog: AirbyteCatalog) {
    connectionService.updateConnection(connectionId, ConnectionPatchRequest(), UUID.randomUUID(), catalog, workspaceId)
  }

  private fun savedOrderSchema(): JsonNode =
    update.captured.syncCatalog!!
      .streams[0]
      .stream!!
      .jsonSchema!!

  private fun schema(orderColumn: String): JsonNode = Jsons.deserialize("""{"type": "object", "properties": {"$ORDER": $orderColumn}}""")

  private fun config(): AirbyteStreamConfiguration =
    AirbyteStreamConfiguration(syncMode = SyncMode.FULL_REFRESH, destinationSyncMode = DestinationSyncMode.OVERWRITE, selected = true)

  private fun catalog(
    jsonSchema: JsonNode,
    name: String = STREAM,
    config: AirbyteStreamConfiguration = config(),
  ): AirbyteCatalog =
    AirbyteCatalog(
      streams =
        listOf(
          AirbyteStreamAndConfiguration(
            stream = AirbyteStream(name = name, jsonSchema = jsonSchema, supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
            config = config,
          ),
        ),
    )

  private fun connectionRead(jsonSchema: JsonNode): ConnectionRead =
    ConnectionRead(
      connectionId = connectionId,
      name = "connection",
      sourceId = sourceId,
      destinationId = UUID.randomUUID(),
      syncCatalog = catalog(jsonSchema),
      status = ConnectionStatus.ACTIVE,
      scheduleType = ConnectionScheduleType.MANUAL,
      createdAt = 1L,
      breakingChange = false,
    )

  private fun sourceResponse(): SourceResponse =
    SourceResponse(
      sourceId = sourceId.toString(),
      name = "postgres",
      sourceType = "postgres",
      definitionId = sourceDefinitionId.toString(),
      workspaceId = workspaceId.toString(),
      configuration = Jsons.emptyObject(),
      createdAt = 1L,
    )

  companion object {
    private const val STREAM = "orders"
    private const val ORDER = "order_id"
  }
}
