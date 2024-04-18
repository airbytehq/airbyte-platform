/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.helpers

import io.airbyte.airbyte_api.model.generated.ConnectionSchedule
import io.airbyte.airbyte_api.model.generated.ConnectionSyncModeEnum
import io.airbyte.airbyte_api.model.generated.ScheduleTypeEnum
import io.airbyte.airbyte_api.model.generated.StreamConfiguration
import io.airbyte.airbyte_api.model.generated.StreamConfigurations
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.SelectedFieldInfo
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.api.server.problems.ConnectionConfigurationProblem
import io.airbyte.commons.json.Jsons
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

internal class AirbyteCatalogHelperTest {
  @Test
  internal fun `test that a stream configuration is not empty`() {
    val streamConfigurations: StreamConfigurations = mockk()

    every { streamConfigurations.streams } returns listOf(mockk<StreamConfiguration>())

    assertTrue(AirbyteCatalogHelper.hasStreamConfigurations(streamConfigurations))
  }

  @Test
  internal fun `test that a stream configuration is empty`() {
    val streamConfigurations: StreamConfigurations = mockk()

    every { streamConfigurations.streams } returns listOf()

    assertFalse(AirbyteCatalogHelper.hasStreamConfigurations(streamConfigurations))

    every { streamConfigurations.streams } returns null

    assertFalse(AirbyteCatalogHelper.hasStreamConfigurations(streamConfigurations))

    assertFalse(AirbyteCatalogHelper.hasStreamConfigurations(null))
  }

  @Test
  internal fun `test that a copy of the AirbyteStreamConfiguration is returned when it is updated to full refresh overwrite mode`() {
    val originalStreamConfiguration = createAirbyteStreamConfiguration()

    val updatedStreamConfiguration = AirbyteCatalogHelper.updateConfigDefaultFullRefreshOverwrite(config = originalStreamConfiguration)
    assertFalse(originalStreamConfiguration === updatedStreamConfiguration)
    assertEquals(SyncMode.FULL_REFRESH, updatedStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.OVERWRITE, updatedStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that a copy of the AirbyteCatalog is returned when all of its streams are updated to full refresh overwrite mode`() {
    val originalAirbyteCatalog = createAirbyteCatalog()
    val updatedAirbyteCatalog = AirbyteCatalogHelper.updateAllStreamsFullRefreshOverwrite(airbyteCatalog = originalAirbyteCatalog)
    assertFalse(originalAirbyteCatalog === updatedAirbyteCatalog)
    updatedAirbyteCatalog.streams.stream().forEach { stream ->
      assertEquals(SyncMode.FULL_REFRESH, stream.config?.syncMode)
      assertEquals(DestinationSyncMode.OVERWRITE, stream.config?.destinationSyncMode)
    }
  }

  @Test
  internal fun `test that streams can be validated`() {
    val referenceCatalog = createAirbyteCatalog()
    val streamConfiguration = StreamConfiguration()
    streamConfiguration.name = "name1"
    val streamConfigurations = StreamConfigurations()
    streamConfigurations.streams = listOf(streamConfiguration)

    assertTrue(AirbyteCatalogHelper.validateStreams(referenceCatalog = referenceCatalog, streamConfigurations = streamConfigurations))
  }

  @Test
  internal fun `test that a stream with an invalid name is considered to be invalid`() {
    val referenceCatalog = createAirbyteCatalog()
    val streamConfiguration = StreamConfiguration()
    streamConfiguration.name = "unknown"
    val streamConfigurations = StreamConfigurations()
    streamConfigurations.streams = listOf(streamConfiguration)

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateStreams(referenceCatalog = referenceCatalog, streamConfigurations = streamConfigurations)
      }
    assertEquals(true, throwable.message?.contains("Invalid stream found"))
  }

  @Test
  internal fun `test that streams with duplicate streams is considered to be invalid`() {
    val referenceCatalog = createAirbyteCatalog()
    val streamConfiguration1 = StreamConfiguration()
    streamConfiguration1.name = "name1"
    val streamConfiguration2 = StreamConfiguration()
    streamConfiguration2.name = "name1"
    val streamConfigurations = StreamConfigurations()
    streamConfigurations.streams = listOf(streamConfiguration1, streamConfiguration2)

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateStreams(referenceCatalog = referenceCatalog, streamConfigurations = streamConfigurations)
      }
    assertEquals(true, throwable.message?.contains("Duplicate stream found in configuration"))
  }

  @Test
  internal fun `test that valid streams can be retrieved from the AirbyteCatalog`() {
    val airbyteCatalog = createAirbyteCatalog()
    val validStreamNames = AirbyteCatalogHelper.getValidStreams(airbyteCatalog = airbyteCatalog)
    assertEquals(airbyteCatalog.streams.map { it.stream?.name }.toSet(), validStreamNames.keys)
  }

  @Test
  internal fun `test that the cron configuration can be validated`() {
    val connectionSchedule = ConnectionSchedule()
    connectionSchedule.scheduleType = ScheduleTypeEnum.CRON
    connectionSchedule.cronExpression = "0 15 10 * * ? * UTC"
    assertTrue(AirbyteCatalogHelper.validateCronConfiguration(connectionSchedule = connectionSchedule))
    assertFalse(connectionSchedule.cronExpression.contains("UTC"))

    connectionSchedule.scheduleType = ScheduleTypeEnum.MANUAL
    assertTrue(AirbyteCatalogHelper.validateCronConfiguration(connectionSchedule = connectionSchedule))

    assertTrue(AirbyteCatalogHelper.validateCronConfiguration(connectionSchedule = null))
  }

  @Test
  internal fun `test that the cron configuration with a missing cron expression is invalid`() {
    val connectionSchedule = ConnectionSchedule()
    connectionSchedule.scheduleType = ScheduleTypeEnum.CRON
    connectionSchedule.cronExpression = null

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateCronConfiguration(connectionSchedule = connectionSchedule)
      }
    assertEquals(true, throwable.message?.contains("Missing cron expression in the schedule."))
  }

  @Test
  internal fun `test that the cron configuration with an invalid cron expression length is invalid`() {
    val connectionSchedule = ConnectionSchedule()
    connectionSchedule.scheduleType = ScheduleTypeEnum.CRON
    connectionSchedule.cronExpression = "0 15 10 * * ? * * * *"

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateCronConfiguration(connectionSchedule = connectionSchedule)
      }
    assertEquals(true, throwable.message?.contains("Cron expression contains 10 parts but we expect one of [6, 7]"))
  }

  @Test
  internal fun `test that the cron configuration with an invalid cron expression is invalid`() {
    val connectionSchedule = ConnectionSchedule()
    connectionSchedule.scheduleType = ScheduleTypeEnum.CRON
    connectionSchedule.cronExpression = "not a valid cron expression string"

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateCronConfiguration(connectionSchedule = connectionSchedule)
      }
    assertEquals(true, throwable.message?.contains("Failed to parse cron expression. Invalid chars in expression!"))
  }

  @ParameterizedTest
  @EnumSource(ConnectionSyncModeEnum::class)
  internal fun `test that when a stream configuration is updated, the corret sync modes are set based on the stream configuration`(
    connectionSyncMode: ConnectionSyncModeEnum,
  ) {
    val cursorField = "cursor"
    val primayKeyColumn = "primary"
    val airbyteStream = AirbyteStream()
    val airbyteStreamConfiguration = createAirbyteStreamConfiguration()
    val streamConfiguration = StreamConfiguration()
    streamConfiguration.syncMode = connectionSyncMode
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.primaryKey = listOf(listOf(primayKeyColumn))

    val updatedAirbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = airbyteStreamConfiguration,
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )
    assertEquals(true, updatedAirbyteStreamConfiguration.selected)
    assertEquals(getSyncMode(connectionSyncMode), updatedAirbyteStreamConfiguration.syncMode)
    assertEquals(getDestinationSyncMode(connectionSyncMode), updatedAirbyteStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that when a stream configuration does not have a configured sync mode, the updated configuration uses full refresh overwrite`() {
    val cursorField = "cursor"
    val primayKeyColumn = "primary"
    val airbyteStream = AirbyteStream()
    val airbyteStreamConfiguration = createAirbyteStreamConfiguration()
    val streamConfiguration = StreamConfiguration()
    streamConfiguration.syncMode = null
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.primaryKey = listOf(listOf(primayKeyColumn))

    val updatedAirbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = airbyteStreamConfiguration,
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    assertEquals(true, updatedAirbyteStreamConfiguration.selected)
    assertEquals(SyncMode.FULL_REFRESH, updatedAirbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.OVERWRITE, updatedAirbyteStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that when validating a stream without a sync mode, the sync mode is set to full refresh and the stream is considered valid`() {
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    streamConfiguration.syncMode = null
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    assertTrue(AirbyteCatalogHelper.validateStreamConfig(streamConfiguration, listOf(), airbyteStream))
    assertEquals(SyncMode.FULL_REFRESH, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.OVERWRITE, airbyteStreamConfiguration.destinationSyncMode)
    assertEquals(true, airbyteStreamConfiguration.selected)
  }

  @Test
  internal fun `test that if the stream configuration contains an invalid sync mode, the stream is considered invalid`() {
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.syncMode = ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE
    streamConfiguration.name = "stream-name"

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateStreamConfig(
          streamConfiguration = streamConfiguration,
          validDestinationSyncModes = listOf(DestinationSyncMode.OVERWRITE),
          airbyteStream = airbyteStream,
        )
      }
    assertEquals(true, throwable.message?.contains("Cannot set sync mode to ${streamConfiguration.syncMode} for stream"))
  }

  @Test
  internal fun `test that a stream configuration with FULL_REFRESH_APPEND is always considered to be valid`() {
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.supportedSyncModes = listOf(SyncMode.FULL_REFRESH)
    streamConfiguration.syncMode = ConnectionSyncModeEnum.FULL_REFRESH_APPEND
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        createAirbyteStreamConfiguration(),
        airbyteStream,
        streamConfiguration,
      )

    assertTrue(
      AirbyteCatalogHelper.validateStreamConfig(
        streamConfiguration = streamConfiguration,
        validDestinationSyncModes = listOf(DestinationSyncMode.APPEND),
        airbyteStream = airbyteStream,
      ),
    )
    assertEquals(SyncMode.FULL_REFRESH, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND, airbyteStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that a stream configuration with FULL_REFRESH_OVERWRITE is always considered to be valid`() {
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.supportedSyncModes = listOf(SyncMode.FULL_REFRESH)
    streamConfiguration.syncMode = ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    assertTrue(
      AirbyteCatalogHelper.validateStreamConfig(
        streamConfiguration = streamConfiguration,
        validDestinationSyncModes = listOf(DestinationSyncMode.OVERWRITE),
        airbyteStream = airbyteStream,
      ),
    )
    assertEquals(SyncMode.FULL_REFRESH, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.OVERWRITE, airbyteStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that a stream configuration with INCREMENTAL_APPEND is only valid if the source defined cursor field is also valid`() {
    val cursorField = "cursor"
    val airbyteStream = AirbyteStream()
    val airbyteStreamConfiguration = createAirbyteStreamConfiguration()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf(cursorField)
    airbyteStream.sourceDefinedCursor = true
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_APPEND

    assertTrue(
      AirbyteCatalogHelper.validateStreamConfig(
        streamConfiguration = streamConfiguration,
        validDestinationSyncModes = listOf(DestinationSyncMode.APPEND),
        airbyteStream = airbyteStream,
      ),
    )
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND, airbyteStreamConfiguration.destinationSyncMode)
    assertEquals(listOf(cursorField), airbyteStreamConfiguration.cursorField)
  }

  @Test
  internal fun `test that a stream configuration with INCREMENTAL_APPEND is invalid if the source defined cursor field is invalid`() {
    val cursorField = "cursor"
    val streamName = "stream-name"
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf(cursorField)
    airbyteStream.name = streamName
    airbyteStream.sourceDefinedCursor = true
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf("other")
    streamConfiguration.name = airbyteStream.name
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_APPEND
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateStreamConfig(
          streamConfiguration = streamConfiguration,
          validDestinationSyncModes = listOf(DestinationSyncMode.APPEND),
          airbyteStream = airbyteStream,
        )
      }
    assertEquals(true, throwable.message?.contains("Do not include a cursor field configuration for this stream"))
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND, airbyteStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that a stream configuration with INCREMENTAL_APPEND is only valid if the source cursor field is also valid`() {
    val cursorField = "cursor"
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf(cursorField)
    airbyteStream.jsonSchema = Jsons.deserialize("{\"properties\": {\"$cursorField\": {}}}")
    airbyteStream.sourceDefinedCursor = false
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_APPEND
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    assertTrue(
      AirbyteCatalogHelper.validateStreamConfig(
        streamConfiguration = streamConfiguration,
        validDestinationSyncModes = listOf(DestinationSyncMode.APPEND),
        airbyteStream = airbyteStream,
      ),
    )
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND, airbyteStreamConfiguration.destinationSyncMode)
    assertEquals(listOf(cursorField), airbyteStreamConfiguration.cursorField)
  }

  @Test
  internal fun `test that a stream configuration with INCREMENTAL_APPEND is invalid if the source cursor field is invalid`() {
    val cursorField = "cursor"
    val otherCursorField = "other"
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf(otherCursorField)
    airbyteStream.jsonSchema = Jsons.deserialize("{\"properties\": {\"$otherCursorField\": {}}}")
    airbyteStream.name = "name"
    airbyteStream.sourceDefinedCursor = false
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_APPEND
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateStreamConfig(
          streamConfiguration = streamConfiguration,
          validDestinationSyncModes = listOf(DestinationSyncMode.APPEND),
          airbyteStream = airbyteStream,
        )
      }
    assertEquals(
      true,
      throwable.message?.contains(
        "Invalid cursor field for stream: ${airbyteStream.name}. The list of valid cursor fields include: [[$otherCursorField]]",
      ),
    )
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND, airbyteStreamConfiguration.destinationSyncMode)
    assertEquals(listOf(cursorField), airbyteStreamConfiguration.cursorField)
  }

  @Test
  internal fun `test that a stream configuration with INCREMENTAL_APPEND is invalid if there is no cursor field`() {
    val cursorField = "cursor"
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf()
    airbyteStream.jsonSchema = Jsons.deserialize("{\"properties\": {\"$cursorField\": {}}}")
    airbyteStream.name = "name"
    airbyteStream.sourceDefinedCursor = false
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf()
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_APPEND
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    val throwable =
      assertThrows(ConnectionConfigurationProblem::class.java) {
        AirbyteCatalogHelper.validateStreamConfig(
          streamConfiguration = streamConfiguration,
          validDestinationSyncModes = listOf(DestinationSyncMode.APPEND),
          airbyteStream = airbyteStream,
        )
      }
    assertEquals(
      true,
      throwable.message?.contains(
        "No default cursor field for stream: ${airbyteStream.name}. Please include a cursor field configuration for this stream.",
      ),
    )
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND, airbyteStreamConfiguration.destinationSyncMode)
  }

  @Test
  internal fun `test that an INCREMENTAL_DEDUPED_HISTORY stream is only valid if the source defined cursor and primary key field are also valid`() {
    val cursorField = "cursor"
    val primaryKey = "primary"
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf(cursorField)
    airbyteStream.jsonSchema = Jsons.deserialize("{\"properties\": {\"$cursorField\": {}, \"$primaryKey\": {}}}")
    airbyteStream.sourceDefinedCursor = true
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.primaryKey = listOf(listOf(primaryKey))
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    assertTrue(
      AirbyteCatalogHelper.validateStreamConfig(
        streamConfiguration = streamConfiguration,
        validDestinationSyncModes = listOf(DestinationSyncMode.APPEND_DEDUP),
        airbyteStream = airbyteStream,
      ),
    )
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND_DEDUP, airbyteStreamConfiguration.destinationSyncMode)
    assertEquals(listOf(cursorField), airbyteStreamConfiguration.cursorField)
    assertEquals(listOf(listOf(primaryKey)), airbyteStreamConfiguration.primaryKey)
  }

  @Test
  internal fun `test that an INCREMENTAL_DEDUPED_HISTORY stream is only valid if the source cursor field and primary key field are also valid`() {
    val cursorField = "cursor"
    val primaryKey = "primary"
    val airbyteStream = AirbyteStream()
    val streamConfiguration = StreamConfiguration()
    airbyteStream.defaultCursorField = listOf(cursorField)
    airbyteStream.jsonSchema = Jsons.deserialize("{\"properties\": {\"$cursorField\": {}, \"$primaryKey\": {}}}")
    airbyteStream.sourceDefinedCursor = false
    airbyteStream.supportedSyncModes = listOf(SyncMode.INCREMENTAL)
    streamConfiguration.cursorField = listOf(cursorField)
    streamConfiguration.primaryKey = listOf(listOf(primaryKey))
    streamConfiguration.syncMode = ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY
    val airbyteStreamConfiguration =
      AirbyteCatalogHelper.updateAirbyteStreamConfiguration(
        config = createAirbyteStreamConfiguration(),
        airbyteStream = airbyteStream,
        streamConfiguration = streamConfiguration,
      )

    assertTrue(
      AirbyteCatalogHelper.validateStreamConfig(
        streamConfiguration = streamConfiguration,
        validDestinationSyncModes = listOf(DestinationSyncMode.APPEND_DEDUP),
        airbyteStream = airbyteStream,
      ),
    )
    assertEquals(SyncMode.INCREMENTAL, airbyteStreamConfiguration.syncMode)
    assertEquals(DestinationSyncMode.APPEND_DEDUP, airbyteStreamConfiguration.destinationSyncMode)
    assertEquals(listOf(cursorField), airbyteStreamConfiguration.cursorField)
    assertEquals(listOf(listOf(primaryKey)), airbyteStreamConfiguration.primaryKey)
  }

  @Test
  internal fun `test that the combined sync modes are valid`() {
    val validSourceSyncModes = listOf(SyncMode.FULL_REFRESH)
    val validDestinationSyncModes = listOf(DestinationSyncMode.OVERWRITE)

    val combinedSyncModes =
      AirbyteCatalogHelper.validCombinedSyncModes(
        validSourceSyncModes = validSourceSyncModes,
        validDestinationSyncModes = validDestinationSyncModes,
      )
    assertEquals(1, combinedSyncModes.size)
    assertEquals(listOf(ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE).first(), combinedSyncModes.first())
  }

  private fun createAirbyteCatalog(): AirbyteCatalog {
    val airbyteCatalog = AirbyteCatalog()
    val streams = mutableListOf<AirbyteStreamAndConfiguration>()
    for (i in 1..5) {
      val streamAndConfiguration = AirbyteStreamAndConfiguration()
      val stream = AirbyteStream()
      stream.name = "name$i"
      stream.namespace = "namespace"
      streamAndConfiguration.stream = stream
      streamAndConfiguration.config = createAirbyteStreamConfiguration()
      streams += streamAndConfiguration
    }
    airbyteCatalog.streams(streams)
    return airbyteCatalog
  }

  private fun createAirbyteStreamConfiguration(): AirbyteStreamConfiguration {
    val airbyteStreamConfiguration = AirbyteStreamConfiguration()
    airbyteStreamConfiguration.aliasName = "alias"
    airbyteStreamConfiguration.cursorField = listOf("cursor")
    airbyteStreamConfiguration.destinationSyncMode = DestinationSyncMode.APPEND
    airbyteStreamConfiguration.fieldSelectionEnabled = true
    airbyteStreamConfiguration.primaryKey = listOf(listOf("primary"))
    airbyteStreamConfiguration.selected = false
    airbyteStreamConfiguration.selectedFields = listOf(SelectedFieldInfo())
    airbyteStreamConfiguration.suggested = false
    airbyteStreamConfiguration.syncMode = SyncMode.INCREMENTAL
    return airbyteStreamConfiguration
  }

  private fun getSyncMode(connectionSyncMode: ConnectionSyncModeEnum): SyncMode {
    return when (connectionSyncMode) {
      ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE -> SyncMode.FULL_REFRESH
      ConnectionSyncModeEnum.FULL_REFRESH_APPEND -> SyncMode.FULL_REFRESH
      ConnectionSyncModeEnum.INCREMENTAL_APPEND -> SyncMode.INCREMENTAL
      ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY -> SyncMode.INCREMENTAL
    }
  }

  private fun getDestinationSyncMode(connectionSyncMode: ConnectionSyncModeEnum): DestinationSyncMode {
    return when (connectionSyncMode) {
      ConnectionSyncModeEnum.FULL_REFRESH_OVERWRITE -> DestinationSyncMode.OVERWRITE
      ConnectionSyncModeEnum.FULL_REFRESH_APPEND -> DestinationSyncMode.APPEND
      ConnectionSyncModeEnum.INCREMENTAL_APPEND -> DestinationSyncMode.APPEND
      ConnectionSyncModeEnum.INCREMENTAL_DEDUPED_HISTORY -> DestinationSyncMode.APPEND_DEDUP
    }
  }
}
