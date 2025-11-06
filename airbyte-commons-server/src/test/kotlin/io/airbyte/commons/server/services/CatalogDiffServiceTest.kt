/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.services

import io.airbyte.api.model.generated.DiffCatalogsRequest
import io.airbyte.api.model.generated.SchemaChange
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.StandardSync
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.protocol.models.v0.SyncMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.util.UUID
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog as ProtocolConfiguredAirbyteCatalog

internal class CatalogDiffServiceTest {
  private lateinit var catalogService: CatalogService
  private lateinit var connectionService: ConnectionService
  private lateinit var applySchemaChangeHelper: ApplySchemaChangeHelper
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
    catalogService = mock()
    connectionService = mock()
    applySchemaChangeHelper = mock()

    catalogDiffService =
      CatalogDiffService(
        catalogService,
        connectionService,
        applySchemaChangeHelper,
        catalogConverter,
        catalogMergeHelper,
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
      DiffCatalogsRequest()
        .currentCatalogId(currentCatalogId)
        .newCatalogId(newCatalogId)
        .connectionId(connectionId)

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
    whenever(applySchemaChangeHelper.containsBreakingChange(any())).thenReturn(false)

    val request =
      DiffCatalogsRequest()
        .currentCatalogId(currentCatalogId)
        .newCatalogId(newCatalogId)
        .connectionId(connectionId)

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
    whenever(applySchemaChangeHelper.containsBreakingChange(any())).thenReturn(true)

    val request =
      DiffCatalogsRequest()
        .currentCatalogId(currentCatalogId)
        .newCatalogId(newCatalogId)
        .connectionId(connectionId)

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

    whenever(connectionService.getStandardSync(connectionId)).thenReturn(connection)

    // Mock catalogs
    val currentActorCatalog =
      ActorCatalog()
        .withId(currentCatalogId)
        .withCatalog(Jsons.jsonNode(currentCatalog))

    val newActorCatalog =
      ActorCatalog()
        .withId(newCatalogId)
        .withCatalog(Jsons.jsonNode(newCatalog))

    whenever(catalogService.getActorCatalogById(currentCatalogId)).thenReturn(currentActorCatalog)
    whenever(catalogService.getActorCatalogById(newCatalogId)).thenReturn(newActorCatalog)
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
    whenever(applySchemaChangeHelper.containsBreakingChange(any())).thenReturn(false)

    // catalogConverter is a real instance, so no need to mock it

    val request =
      DiffCatalogsRequest()
        .currentCatalogId(currentCatalogId)
        .newCatalogId(newCatalogId)
        .connectionId(connectionId)

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
    assertTrue(mergedStream.config.selected, "Stream should remain selected from configured catalog")
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

    whenever(catalogService.getActorCatalogById(currentCatalogId)).thenReturn(currentActorCatalog)
    whenever(catalogService.getActorCatalogById(newCatalogId)).thenReturn(newActorCatalog)

    val request =
      DiffCatalogsRequest()
        .currentCatalogId(currentCatalogId)
        .newCatalogId(newCatalogId)
    // No connectionId

    // Execute
    val result = catalogDiffService.diffCatalogs(request)

    // Assert
    assertTrue(result.mergedCatalog == null, "Merged catalog should be null when connectionId is not provided")
  }
}
