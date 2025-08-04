/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionSchedule
import io.airbyte.api.model.generated.ConnectionScheduleData
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.model.generated.ConnectionScheduleType
import io.airbyte.api.model.generated.ConnectionStatus
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationSnippetRead
import io.airbyte.api.model.generated.JobStatus
import io.airbyte.api.model.generated.NamespaceDefinitionType
import io.airbyte.api.model.generated.SchemaChange
import io.airbyte.api.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceSnippetRead
import io.airbyte.api.model.generated.Tag
import io.airbyte.api.model.generated.WebBackendConnectionListItem
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.text.Names.toAlphanumericAndUnderscore
import io.airbyte.config.AirbyteStream
import io.airbyte.config.BasicSchedule
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.FieldType
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.helpers.CatalogHelpers.Companion.createAirbyteStream
import io.airbyte.config.helpers.CatalogHelpers.Companion.fieldsToJsonSchema
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.Field
import java.util.Optional
import java.util.UUID

object ConnectionHelpers {
  private const val STREAM_NAME_BASE = "users-data"
  private val STREAM_NAME = STREAM_NAME_BASE + "0"
  private const val STREAM_NAMESPACE = "namespace"
  const val FIELD_NAME: String = "id"

  const val SECOND_FIELD_NAME: String = "id2"
  private const val BASIC_SCHEDULE_TIME_UNIT = "days"
  private const val BASIC_SCHEDULE_UNITS = 1L
  private const val BASIC_SCHEDULE_DATA_TIME_UNITS = "days"
  private const val BASIC_SCHEDULE_DATA_UNITS = 1L
  private const val ONE_HUNDRED_G = "100g"
  private const val STANDARD_SYNC_NAME = "presto to hudi"
  private const val STANDARD_SYNC_PREFIX = "presto_to_hudi"
  private val fieldGenerator = FieldGenerator()

  private val catalogConverters = CatalogConverter(fieldGenerator, mutableListOf())

  private val apiPojoConverters = ApiPojoConverters(catalogConverters)

  val STREAM_DESCRIPTOR: StreamDescriptor? = StreamDescriptor().withName(STREAM_NAME)

  // only intended for unit tests, so intentionally set very high to ensure they aren't being used
  // elsewhere
  @JvmField
  val TESTING_RESOURCE_REQUIREMENTS: ResourceRequirements =
    ResourceRequirements()
      .withCpuLimit(ONE_HUNDRED_G)
      .withCpuRequest(ONE_HUNDRED_G)
      .withMemoryLimit(ONE_HUNDRED_G)
      .withMemoryRequest(ONE_HUNDRED_G)

  fun generateSyncWithSourceId(sourceId: UUID?): StandardSync {
    val connectionId = UUID.randomUUID()

    return StandardSync()
      .withConnectionId(connectionId)
      .withName(STANDARD_SYNC_NAME)
      .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
      .withNamespaceFormat(null)
      .withPrefix(STANDARD_SYNC_PREFIX)
      .withStatus(StandardSync.Status.ACTIVE)
      .withCatalog(generateBasicConfiguredAirbyteCatalog())
      .withSourceId(sourceId)
      .withDestinationId(UUID.randomUUID())
      .withOperationIds(listOf(UUID.randomUUID()))
      .withManual(false)
      .withSchedule(generateBasicSchedule())
      .withResourceRequirements(TESTING_RESOURCE_REQUIREMENTS)
      .withBreakingChange(false)
  }

  fun generateSyncWithDestinationId(destinationId: UUID?): StandardSync {
    val connectionId = UUID.randomUUID()

    return StandardSync()
      .withConnectionId(connectionId)
      .withName(STANDARD_SYNC_NAME)
      .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
      .withNamespaceFormat(null)
      .withPrefix(STANDARD_SYNC_PREFIX)
      .withStatus(StandardSync.Status.ACTIVE)
      .withCatalog(generateBasicConfiguredAirbyteCatalog())
      .withSourceId(UUID.randomUUID())
      .withDestinationId(destinationId)
      .withOperationIds(listOf(UUID.randomUUID()))
      .withManual(true)
  }

  fun generateSyncWithSourceAndDestinationId(
    sourceId: UUID?,
    destinationId: UUID?,
    isBroken: Boolean,
    status: StandardSync.Status?,
  ): StandardSync {
    val connectionId = UUID.randomUUID()

    return StandardSync()
      .withConnectionId(connectionId)
      .withName(STANDARD_SYNC_NAME)
      .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
      .withNamespaceFormat(null)
      .withPrefix(STANDARD_SYNC_PREFIX)
      .withStatus(status)
      .withCatalog(generateBasicConfiguredAirbyteCatalog())
      .withSourceCatalogId(UUID.randomUUID())
      .withSourceId(sourceId)
      .withDestinationId(destinationId)
      .withOperationIds(listOf(UUID.randomUUID()))
      .withManual(true)
      .withBreakingChange(isBroken)
      .withNotifySchemaChanges(false)
      .withNotifySchemaChangesByEmail(true)
  }

  @JvmStatic
  fun generateBasicConnectionSchedule(): ConnectionSchedule? =
    ConnectionSchedule()
      .timeUnit(ConnectionSchedule.TimeUnitEnum.fromValue(BASIC_SCHEDULE_TIME_UNIT))
      .units(BASIC_SCHEDULE_UNITS)

  @JvmStatic
  fun generateBasicSchedule(): Schedule? =
    Schedule()
      .withTimeUnit(Schedule.TimeUnit.fromValue(BASIC_SCHEDULE_TIME_UNIT))
      .withUnits(BASIC_SCHEDULE_UNITS)

  @JvmStatic
  fun generateBasicConnectionScheduleData(): ConnectionScheduleData? =
    ConnectionScheduleData().basicSchedule(
      ConnectionScheduleDataBasicSchedule().timeUnit(ConnectionScheduleDataBasicSchedule.TimeUnitEnum.DAYS).units(BASIC_SCHEDULE_UNITS),
    )

  @JvmStatic
  fun generateBasicScheduleData(): ScheduleData? =
    ScheduleData().withBasicSchedule(
      BasicSchedule()
        .withTimeUnit(BasicSchedule.TimeUnit.fromValue((BASIC_SCHEDULE_DATA_TIME_UNITS)))
        .withUnits(BASIC_SCHEDULE_DATA_UNITS),
    )

  fun generateExpectedConnectionRead(
    connectionId: UUID?,
    sourceId: UUID?,
    destinationId: UUID?,
    operationIds: MutableList<UUID?>?,
    sourceCatalogId: UUID?,
    breaking: Boolean,
    notifySchemaChange: Boolean?,
    notifySchemaChangeByEmail: Boolean?,
    backfillPreference: SchemaChangeBackfillPreference?,
    tags: MutableList<Tag?>?,
  ): ConnectionRead =
    ConnectionRead()
      .connectionId(connectionId)
      .sourceId(sourceId)
      .destinationId(destinationId)
      .operationIds(operationIds)
      .name("presto to hudi")
      .namespaceDefinition(NamespaceDefinitionType.SOURCE)
      .namespaceFormat(null)
      .prefix("presto_to_hudi")
      .schedule(generateBasicConnectionSchedule())
      .scheduleType(ConnectionScheduleType.BASIC)
      .scheduleData(generateBasicConnectionScheduleData())
      .syncCatalog(generateBasicApiCatalog())
      .resourceRequirements(
        io.airbyte.api.model.generated
          .ResourceRequirements()
          .cpuRequest(TESTING_RESOURCE_REQUIREMENTS.getCpuRequest())
          .cpuLimit(TESTING_RESOURCE_REQUIREMENTS.getCpuLimit())
          .memoryRequest(TESTING_RESOURCE_REQUIREMENTS.getMemoryRequest())
          .memoryLimit(TESTING_RESOURCE_REQUIREMENTS.getMemoryLimit()),
      ).sourceCatalogId(sourceCatalogId)
      .breakingChange(breaking)
      .notifySchemaChanges(notifySchemaChange)
      .notifySchemaChangesByEmail(notifySchemaChangeByEmail)
      .backfillPreference(backfillPreference)
      .tags(tags)

  @JvmStatic
  fun generateExpectedConnectionRead(standardSync: StandardSync): ConnectionRead {
    val connectionRead =
      generateExpectedConnectionRead(
        standardSync.getConnectionId(),
        standardSync.getSourceId(),
        standardSync.getDestinationId(),
        standardSync.getOperationIds(),
        standardSync.getSourceCatalogId(),
        standardSync.getBreakingChange(),
        standardSync.getNotifySchemaChanges(),
        standardSync.getNotifySchemaChangesByEmail(),
        standardSync.getBackfillPreference()?.convertTo<SchemaChangeBackfillPreference>(),
        standardSync
          .getTags()
          .stream()
          .map<Tag?> { tag: io.airbyte.config.Tag? -> apiPojoConverters.toApiTag(tag!!) }
          .toList(),
      )

    if (standardSync.getSchedule() == null) {
      connectionRead.schedule(null)
    } else {
      connectionRead.schedule(
        ConnectionSchedule()
          .timeUnit(ConnectionSchedule.TimeUnitEnum.fromValue(standardSync.getSchedule().getTimeUnit().value()))
          .units(standardSync.getSchedule().getUnits()),
      )
    }

    if (standardSync.getStatus() == StandardSync.Status.INACTIVE) {
      connectionRead.setStatus(ConnectionStatus.INACTIVE)
    } else if (standardSync.getStatus() == StandardSync.Status.ACTIVE) {
      connectionRead.setStatus(ConnectionStatus.ACTIVE)
    } else if (standardSync.getStatus() == StandardSync.Status.DEPRECATED) {
      connectionRead.setStatus(ConnectionStatus.DEPRECATED)
    }

    return connectionRead
  }

  @JvmStatic
  fun connectionReadFromStandardSync(standardSync: StandardSync): ConnectionRead {
    val connectionRead = ConnectionRead()
    connectionRead
      .connectionId(standardSync.getConnectionId())
      .sourceId(standardSync.getSourceId())
      .destinationId(standardSync.getDestinationId())
      .operationIds(standardSync.getOperationIds())
      .name(standardSync.getName())
      .namespaceFormat(standardSync.getNamespaceFormat())
      .prefix(standardSync.getPrefix())
      .sourceCatalogId(standardSync.getSourceCatalogId())
      .breakingChange(standardSync.getBreakingChange())
      .notifySchemaChanges(standardSync.getNotifySchemaChanges())
      .notifySchemaChangesByEmail(standardSync.getNotifySchemaChangesByEmail())

    if (standardSync.getNamespaceDefinition() != null) {
      connectionRead
        .namespaceDefinition(NamespaceDefinitionType.fromValue(standardSync.getNamespaceDefinition().value()))
    }
    if (standardSync.getStatus() != null) {
      connectionRead.status(ConnectionStatus.fromValue(standardSync.getStatus().value()))
    }
    apiPojoConverters.populateConnectionReadSchedule(standardSync, connectionRead)

    if (standardSync.getCatalog() != null) {
      connectionRead.syncCatalog(catalogConverters.toApi(standardSync.getCatalog(), standardSync.getFieldSelectionData()))
    }
    if (standardSync.getResourceRequirements() != null) {
      connectionRead.resourceRequirements(
        io.airbyte.api.model.generated
          .ResourceRequirements()
          .cpuLimit(standardSync.getResourceRequirements().getCpuLimit())
          .cpuRequest(standardSync.getResourceRequirements().getCpuRequest())
          .memoryLimit(standardSync.getResourceRequirements().getMemoryLimit())
          .memoryRequest(standardSync.getResourceRequirements().getMemoryRequest()),
      )
    }
    return connectionRead
  }

  fun generateExpectedWebBackendConnectionListItem(
    standardSync: StandardSync,
    source: SourceRead,
    destination: DestinationRead,
    isSyncing: Boolean,
    latestSyncJobCreatedAt: Long?,
    latestSynJobStatus: JobStatus?,
    schemaChange: SchemaChange?,
  ): WebBackendConnectionListItem {
    val connectionListItem =
      WebBackendConnectionListItem()
        .connectionId(standardSync.getConnectionId())
        .name(standardSync.getName())
        .source(
          SourceSnippetRead()
            .icon(source.getIcon())
            .name(source.getName())
            .sourceName(source.getSourceName())
            .sourceDefinitionId(source.getSourceDefinitionId())
            .sourceId(source.getSourceId()),
        ).destination(
          DestinationSnippetRead()
            .icon(destination.getIcon())
            .name(destination.getName())
            .destinationName(destination.getDestinationName())
            .destinationDefinitionId(destination.getDestinationDefinitionId())
            .destinationId(destination.getDestinationId()),
        ).status(apiPojoConverters.toApiStatus(standardSync.getStatus()))
        .isSyncing(isSyncing)
        .latestSyncJobCreatedAt(latestSyncJobCreatedAt)
        .latestSyncJobStatus(latestSynJobStatus)
        .scheduleType(apiPojoConverters.toApiConnectionScheduleType(standardSync))
        .scheduleData(apiPojoConverters.toApiConnectionScheduleData(standardSync))
        .schemaChange(schemaChange)
        .tags(mutableListOf())
        .sourceActorDefinitionVersion(ActorDefinitionVersionRead())
        .destinationActorDefinitionVersion(ActorDefinitionVersionRead())

    return connectionListItem
  }

  @JvmStatic
  fun generateBasicJsonSchema(): JsonNode = fieldsToJsonSchema(Field.of(FIELD_NAME, JsonSchemaType.STRING))

  fun generateJsonSchemaWithTwoFields(): JsonNode =
    fieldsToJsonSchema(
      Field.of(FIELD_NAME, JsonSchemaType.STRING),
      Field.of(SECOND_FIELD_NAME, JsonSchemaType.STRING),
    )

  @JvmStatic
  fun generateBasicConfiguredAirbyteCatalog(): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog().withStreams(listOf(generateBasicConfiguredStream(null)))

  @JvmStatic
  fun generateAirbyteCatalogWithTwoFields(): ConfiguredAirbyteCatalog =
    ConfiguredAirbyteCatalog(
      listOf(
        ConfiguredAirbyteStream
          .Builder()
          .stream(
            AirbyteStream(
              STREAM_NAME,
              generateJsonSchemaWithTwoFields(),
              listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
            ).withDefaultCursorField(listOf(FIELD_NAME))
              .withSourceDefinedCursor(false)
              .withSourceDefinedPrimaryKey(mutableListOf()),
          ).cursorField(listOf(FIELD_NAME))
          .syncMode(SyncMode.INCREMENTAL)
          .destinationSyncMode(DestinationSyncMode.APPEND)
          .fields(fieldGenerator.getFieldsFromSchema(generateJsonSchemaWithTwoFields()))
          .build(),
      ),
    )

  @JvmStatic
  fun generateMultipleStreamsConfiguredAirbyteCatalog(streamsCount: Int): ConfiguredAirbyteCatalog {
    val configuredStreams: MutableList<ConfiguredAirbyteStream> = mutableListOf()
    for (i in 0..<streamsCount) {
      configuredStreams.add(generateBasicConfiguredStream(i.toString()))
    }
    return ConfiguredAirbyteCatalog().withStreams(configuredStreams)
  }

  fun generateBasicConfiguredStream(nameSuffix: String?): ConfiguredAirbyteStream =
    ConfiguredAirbyteStream
      .Builder()
      .stream(generateBasicAirbyteStream(nameSuffix))
      .syncMode(SyncMode.INCREMENTAL)
      .destinationSyncMode(DestinationSyncMode.APPEND)
      .cursorField(listOf(FIELD_NAME))
      .fields(listOf(io.airbyte.config.Field(FIELD_NAME, FieldType.STRING, false)))
      .build()

  private fun generateBasicAirbyteStream(nameSuffix: String?): AirbyteStream =
    createAirbyteStream(
      if (nameSuffix == null) STREAM_NAME else STREAM_NAME_BASE + nameSuffix,
      Field.of(FIELD_NAME, JsonSchemaType.STRING),
    ).withDefaultCursorField(listOf(FIELD_NAME))
      .withSourceDefinedCursor(false)
      .withSupportedSyncModes(listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))

  @JvmStatic
  fun generateBasicApiCatalog(): AirbyteCatalog {
    val streams: MutableList<AirbyteStreamAndConfiguration?> = mutableListOf()
    streams.add(
      AirbyteStreamAndConfiguration()
        .stream(generateBasicApiStream(null))
        .config(generateBasicApiStreamConfig(null)),
    )
    return AirbyteCatalog().streams(streams)
  }

  /**
   * Generates an API catalog that has two fields, both selected.
   *
   * @return AirbyteCatalog
   */
  @JvmStatic
  fun generateApiCatalogWithTwoFields(): AirbyteCatalog =
    AirbyteCatalog().streams(
      listOf(
        AirbyteStreamAndConfiguration()
          .stream(generateApiStreamWithTwoFields())
          .config(generateBasicApiStreamConfig(null)),
      ),
    )

  @JvmStatic
  fun generateMultipleStreamsApiCatalog(
    uniqueNamespace: Boolean,
    uniqueStreamName: Boolean,
    streamsCount: Int,
  ): AirbyteCatalog {
    val streamAndConfigurations: MutableList<AirbyteStreamAndConfiguration?> = mutableListOf()
    for (i in 0..<streamsCount) {
      streamAndConfigurations.add(
        AirbyteStreamAndConfiguration()
          .stream(
            ConnectionHelpers.generateApiStreamWithNamespace(
              if (uniqueNamespace) Optional.of("namespace-" + i) else Optional.empty(),
              if (uniqueStreamName) Optional.of("stream-" + i) else Optional.empty(),
            ),
          ).config(generateBasicApiStreamConfig(i.toString())),
      )
    }
    return AirbyteCatalog().streams(streamAndConfigurations)
  }

  @JvmStatic
  fun generateMultipleStreamsApiCatalog(streamsCount: Int): AirbyteCatalog {
    val streamAndConfigurations: MutableList<AirbyteStreamAndConfiguration?> = mutableListOf()
    for (i in 0..<streamsCount) {
      streamAndConfigurations.add(
        AirbyteStreamAndConfiguration()
          .stream(generateBasicApiStream(i.toString()))
          .config(generateBasicApiStreamConfig(i.toString())),
      )
    }
    return AirbyteCatalog().streams(streamAndConfigurations)
  }

  private fun generateBasicApiStreamConfig(nameSuffix: String?): AirbyteStreamConfiguration? =
    AirbyteStreamConfiguration()
      .syncMode(io.airbyte.api.model.generated.SyncMode.INCREMENTAL)
      .cursorField(listOf(FIELD_NAME))
      .destinationSyncMode(io.airbyte.api.model.generated.DestinationSyncMode.APPEND)
      .primaryKey(mutableListOf())
      .aliasName(toAlphanumericAndUnderscore(if (nameSuffix == null) STREAM_NAME else STREAM_NAME_BASE + nameSuffix))
      .selected(true)
      .suggested(false)
      .includeFiles(false)
      .fieldSelectionEnabled(false)
      .selectedFields(mutableListOf())

  private fun generateBasicApiStream(nameSuffix: String?): io.airbyte.api.model.generated.AirbyteStream? =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .name(if (nameSuffix == null) STREAM_NAME else STREAM_NAME_BASE + nameSuffix)
      .jsonSchema(generateBasicJsonSchema())
      .defaultCursorField(listOf(FIELD_NAME))
      .sourceDefinedCursor(false)
      .sourceDefinedPrimaryKey(mutableListOf())
      .supportedSyncModes(
        listOf(
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
        ),
      )

  private fun generateApiStreamWithTwoFields(): io.airbyte.api.model.generated.AirbyteStream? =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .name(STREAM_NAME)
      .jsonSchema(generateJsonSchemaWithTwoFields())
      .defaultCursorField(listOf(FIELD_NAME))
      .sourceDefinedCursor(false)
      .sourceDefinedPrimaryKey(mutableListOf())
      .supportedSyncModes(
        listOf(
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
        ),
      )

  private fun generateApiStreamWithNamespace(
    namespace: Optional<String>,
    name: Optional<String>,
  ): io.airbyte.api.model.generated.AirbyteStream? =
    io.airbyte.api.model.generated
      .AirbyteStream()
      .namespace(namespace.orElse(STREAM_NAMESPACE))
      .name(name.orElse(STREAM_NAME))
      .jsonSchema(generateJsonSchemaWithTwoFields())
      .defaultCursorField(listOf(FIELD_NAME))
      .sourceDefinedCursor(false)
      .sourceDefinedPrimaryKey(mutableListOf())
      .supportedSyncModes(
        listOf(
          io.airbyte.api.model.generated.SyncMode.FULL_REFRESH,
          io.airbyte.api.model.generated.SyncMode.INCREMENTAL,
        ),
      )
}
