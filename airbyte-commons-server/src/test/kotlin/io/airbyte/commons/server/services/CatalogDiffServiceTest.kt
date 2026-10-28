/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import io.airbyte.api.server.generated.models.DiffCatalogsRequest
import io.airbyte.api.server.generated.models.SchemaChange
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.SourceService
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.WidenColumnTypes
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.SyncMode
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog as ProtocolConfiguredAirbyteCatalog

internal class CatalogDiffServiceTest {
  private lateinit var catalogService: CatalogService
  private lateinit var connectionService: ConnectionService
  private lateinit var applySchemaChangeHelper: ApplySchemaChangeHelper
  private lateinit var metricClient: MetricClient
  private lateinit var featureFlagClient: TestClient
  private lateinit var sourceService: SourceService
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var catalogDiffService: CatalogDiffService

  private lateinit var workspaceId: UUID
  private lateinit var sourceId: UUID
  private lateinit var destinationId: UUID
  private lateinit var connectionId: UUID
  private lateinit var currentCatalogId: UUID
  private lateinit var newCatalogId: UUID

  private val fieldGenerator =
    io.airbyte.config.helpers
      .FieldGenerator()
  private val catalogConverter =
    io.airbyte.commons.server.handlers.helpers
      .CatalogConverter(fieldGenerator, listOf())
  private val catalogMergeHelper =
    io.airbyte.commons.server.handlers.helpers
      .CatalogMergeHelper(fieldGenerator)

  @BeforeEach
  fun setUp() {
    catalogService = mockk()
    connectionService = mockk()
    applySchemaChangeHelper = mockk()
    every { connectionService.writeStandardSync(any()) } just Runs
    metricClient = mockk(relaxed = true)
    featureFlagClient = mockk()
    every { featureFlagClient.boolVariation(any(), any()) } returns false
    sourceService = mockk()
    every { sourceService.getSourceConnection(any()) } returns
      SourceConnection().withSourceDefinitionId(UUID.randomUUID()).withWorkspaceId(UUID.randomUUID())
    workspaceHelper = mockk()
    every { workspaceHelper.getOrganizationForWorkspace(any()) } returns UUID.randomUUID()

    catalogDiffService =
      CatalogDiffService(
        catalogService,
        connectionService,
        applySchemaChangeHelper,
        catalogConverter,
        catalogMergeHelper,
        metricClient,
        featureFlagClient,
        sourceService,
        workspaceHelper,
      )

    workspaceId = UUID.randomUUID()
    sourceId = UUID.randomUUID()
    destinationId = UUID.randomUUID()
    connectionId = UUID.randomUUID()
    currentCatalogId = UUID.randomUUID()
    newCatalogId = UUID.randomUUID()
  }

  @Test
  fun `diffCatalogs should return NO_CHANGE when catalogs are identical`() {
    // Setup
    val catalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name", "email"))

    setupMocks(catalog, catalog, configuredCatalog)

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NO_CHANGE, result.schemaChange)
    assertTrue(result.catalogDiff.transforms.isEmpty())
  }

  @Test
  fun `diffCatalogs should return NON_BREAKING when field is added`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns false

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NON_BREAKING, result.schemaChange)
    assertTrue(result.catalogDiff.transforms.isNotEmpty())
  }

  @Test
  fun `diffCatalogs should return BREAKING when change is breaking`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name", "email"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns true

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.BREAKING, result.schemaChange)
    assertTrue(result.catalogDiff.transforms.isNotEmpty())
  }

  private fun setupMocks(
    currentCatalog: AirbyteCatalog,
    newCatalog: AirbyteCatalog,
    configuredCatalog: ProtocolConfiguredAirbyteCatalog,
  ) {
    // Mock connection
    val connection =
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withCatalog(convertToConfigCatalog(configuredCatalog))

    every { connectionService.getStandardSync(connectionId) } returns connection

    // Mock catalogs
    val currentActorCatalog =
      ActorCatalog()
        .withId(currentCatalogId)
        .withCatalog(Jsons.jsonNode(currentCatalog))

    val newActorCatalog =
      ActorCatalog()
        .withId(newCatalogId)
        .withCatalog(Jsons.jsonNode(newCatalog))

    every { catalogService.getActorCatalogById(currentCatalogId) } returns currentActorCatalog
    every { catalogService.getActorCatalogById(newCatalogId) } returns newActorCatalog
  }

  private fun createAirbyteCatalog(
    streamName: String,
    fields: List<String>,
  ): AirbyteCatalog {
    val properties = mutableMapOf<String, Any>()
    fields.forEach { field ->
      properties[field] = mapOf("type" to "string")
    }

    val stream =
      AirbyteStream()
        .withName(streamName)
        .withJsonSchema(Jsons.jsonNode(mapOf("type" to "object", "properties" to properties)))
        .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))

    return AirbyteCatalog().withStreams(listOf(stream))
  }

  private fun createConfiguredCatalog(
    streamName: String,
    fields: List<String>,
  ): ProtocolConfiguredAirbyteCatalog {
    val properties = mutableMapOf<String, Any>()
    fields.forEach { field ->
      properties[field] = mapOf("type" to "string")
    }

    val stream =
      AirbyteStream()
        .withName(streamName)
        .withJsonSchema(Jsons.jsonNode(mapOf("type" to "object", "properties" to properties)))
        .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))

    val configuredStream =
      ConfiguredAirbyteStream()
        .withStream(stream)
        .withSyncMode(SyncMode.FULL_REFRESH)
        .withDestinationSyncMode(DestinationSyncMode.OVERWRITE)

    return ProtocolConfiguredAirbyteCatalog().withStreams(listOf(configuredStream))
  }

  private fun convertToConfigCatalog(protocolCatalog: ProtocolConfiguredAirbyteCatalog): ConfiguredAirbyteCatalog {
    // Simple conversion for test purposes
    return Jsons.`object`(Jsons.jsonNode(protocolCatalog), ConfiguredAirbyteCatalog::class.java)
  }

  @Test
  fun `diffCatalogs should return merged catalog when connectionId is provided`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns false

    // catalogConverter is a real instance, so no need to mock it

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NON_BREAKING, result.schemaChange)
    assertTrue(result.catalogDiff.transforms.isNotEmpty())

    // Verify merged catalog is returned
    assertTrue(result.mergedCatalog != null, "Merged catalog should be present when connectionId is provided")
    assertEquals(1, result.mergedCatalog!!.streams.size)

    // The merged catalog should preserve the 'selected' state from the configured catalog
    // but have the fields from the new catalog
    val mergedStream = result.mergedCatalog!!.streams[0]
    assertEquals(true, mergedStream.config!!.selected, "Stream should remain selected from configured catalog")
  }

  @Test
  fun `diffCatalogs widens a changed column in the merged catalog when the flag is on`() {
    val currentCatalog = createAirbyteCatalog("users", listOf("id"))
    val newCatalog = createAirbyteCatalog("users", listOf("id"))
    newCatalog.streams[0].jsonSchema = Jsons.jsonNode(mapOf("type" to "object", "properties" to mapOf("id" to mapOf("type" to "integer"))))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns false
    every { featureFlagClient.boolVariation(WidenColumnTypes, any()) } returns true

    val result =
      catalogDiffService.diffCatalogs(
        DiffCatalogsRequest(currentCatalogId = currentCatalogId, newCatalogId = newCatalogId, connectionId = connectionId),
      )

    assertEquals(
      Jsons.jsonNode(mapOf("type" to "string", "airbyte_source_type" to "integer")),
      result.mergedCatalog!!
        .streams[0]
        .stream!!
        .jsonSchema!!
        .get("properties")
        .get("id"),
    )
  }

  @Test
  fun `diffCatalogs should not return merged catalog when connectionId is not provided`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))

    // Mock catalogs without connection
    val currentActorCatalog =
      ActorCatalog()
        .withId(currentCatalogId)
        .withCatalog(Jsons.jsonNode(currentCatalog))

    val newActorCatalog =
      ActorCatalog()
        .withId(newCatalogId)
        .withCatalog(Jsons.jsonNode(newCatalog))

    every { catalogService.getActorCatalogById(currentCatalogId) } returns currentActorCatalog
    every { catalogService.getActorCatalogById(newCatalogId) } returns newActorCatalog

    val request =
      DiffCatalogsRequest(currentCatalogId = currentCatalogId, newCatalogId = newCatalogId)
    // No connectionId

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertTrue(result.mergedCatalog == null, "Merged catalog should be null when connectionId is not provided")
  }

  @Test
  fun `diffCatalogs with persist_changes should update connection with non-breaking change`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns false

    // Capture the original connection state
    val originalConnection = connectionService.getStandardSync(connectionId)
    val originalCatalog = originalConnection.catalog
    val originalStatus = originalConnection.status

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
        persistChanges = true,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NON_BREAKING, result.schemaChange)

    // Verify connection was updated
    val connectionCaptor = slot<StandardSync>()
    verify { connectionService.writeStandardSync(capture(connectionCaptor)) }

    val updatedConnection = connectionCaptor.captured
    assertEquals(false, updatedConnection.breakingChange, "breakingChange flag should be false for non-breaking change")
    assertEquals(originalStatus, updatedConnection.status, "status should remain unchanged for non-breaking change")
    assertEquals(originalCatalog, updatedConnection.catalog, "catalog should NOT be updated")
  }

  @Test
  fun `diffCatalogs with persist_changes should update connection with breaking change`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name", "email"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns true

    // Capture the original connection state
    val originalConnection = connectionService.getStandardSync(connectionId)
    val originalCatalog = originalConnection.catalog

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
        persistChanges = true,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.BREAKING, result.schemaChange)

    // Verify connection was updated
    val connectionCaptor = slot<StandardSync>()
    verify { connectionService.writeStandardSync(capture(connectionCaptor)) }

    val updatedConnection = connectionCaptor.captured
    assertEquals(true, updatedConnection.breakingChange, "breakingChange flag should be true for breaking change")
    assertEquals(StandardSync.Status.INACTIVE, updatedConnection.status, "status should be set to INACTIVE for breaking change")
    assertEquals(originalCatalog, updatedConnection.catalog, "catalog should NOT be updated")
  }

  @Test
  fun `diffCatalogs without persist_changes should not update connection`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns false

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
        persistChanges = false,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NON_BREAKING, result.schemaChange)

    // Verify connection was NOT updated
    verify(exactly = 0) { connectionService.writeStandardSync(any()) }
  }

  @Test
  fun `diffCatalogs with persist_changes and NO_CHANGE should not modify connection`() {
    // Setup
    val catalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name", "email"))

    setupMocks(catalog, catalog, configuredCatalog)

    // Capture the original connection state
    val originalConnection = connectionService.getStandardSync(connectionId)
    val originalCatalog = originalConnection.catalog
    val originalStatus = originalConnection.status

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
        persistChanges = true,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NO_CHANGE, result.schemaChange)

    // Verify connection was updated
    val connectionCaptor = slot<StandardSync>()
    verify { connectionService.writeStandardSync(capture(connectionCaptor)) }

    val updatedConnection = connectionCaptor.captured
    assertFalse(updatedConnection.breakingChange, "breakingChange flag should be false for no change")
    assertEquals(originalStatus, updatedConnection.status, "status should remain unchanged for no change")
    assertEquals(originalCatalog, updatedConnection.catalog, "catalog should NOT be updated")
  }

  @Test
  fun `diffCatalogs with persist_changes should disable connection when nonBreakingChangesPreference is DISABLE`() {
    // Setup
    val currentCatalog = createAirbyteCatalog("users", listOf("id", "name"))
    val newCatalog = createAirbyteCatalog("users", listOf("id", "name", "email"))
    val configuredCatalog = createConfiguredCatalog("users", listOf("id", "name"))

    setupMocks(currentCatalog, newCatalog, configuredCatalog)
    every { applySchemaChangeHelper.containsBreakingChange(any()) } returns false
    every { applySchemaChangeHelper.containsChanges(any()) } returns true

    // Set the connection's nonBreakingChangesPreference to DISABLE
    val connection = connectionService.getStandardSync(connectionId)
    connection.nonBreakingChangesPreference = StandardSync.NonBreakingChangesPreference.DISABLE

    val originalCatalog = connection.catalog

    val request =
      DiffCatalogsRequest(
        currentCatalogId = currentCatalogId,
        newCatalogId = newCatalogId,
        connectionId = connectionId,
        persistChanges = true,
      )

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertEquals(SchemaChange.NON_BREAKING, result.schemaChange)

    // Verify connection was updated
    val connectionCaptor = slot<StandardSync>()
    verify { connectionService.writeStandardSync(capture(connectionCaptor)) }

    val updatedConnection = connectionCaptor.captured
    assertFalse(updatedConnection.breakingChange, "breakingChange flag should be false for non-breaking change")
    assertEquals(
      StandardSync.Status.INACTIVE,
      updatedConnection.status,
      "status should be INACTIVE when nonBreakingChangesPreference is DISABLE and there are changes",
    )
    assertEquals(originalCatalog, updatedConnection.catalog, "catalog should NOT be updated")
  }
}
