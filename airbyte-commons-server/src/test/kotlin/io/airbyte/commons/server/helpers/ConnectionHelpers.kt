/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.AirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionSchedule
import io.airbyte.api.server.generated.models.ConnectionScheduleData
import io.airbyte.api.server.generated.models.ConnectionScheduleDataBasicSchedule
import io.airbyte.api.server.generated.models.ConnectionScheduleType
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.DestinationRead
import io.airbyte.api.server.generated.models.DestinationSnippetRead
import io.airbyte.api.server.generated.models.JobStatus
import io.airbyte.api.server.generated.models.NamespaceDefinitionType
import io.airbyte.api.server.generated.models.SchemaChange
import io.airbyte.api.server.generated.models.SchemaChangeBackfillPreference
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SourceSnippetRead
import io.airbyte.api.server.generated.models.Tag
import io.airbyte.api.server.generated.models.WebBackendConnectionListItem
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
    ConnectionSchedule(
      timeUnit = ConnectionSchedule.TimeUnit.entries.first { it.value == BASIC_SCHEDULE_TIME_UNIT },
      units = BASIC_SCHEDULE_UNITS,
    )

  @JvmStatic
  fun generateBasicSchedule(): Schedule? =
    Schedule()
      .withTimeUnit(Schedule.TimeUnit.fromValue(BASIC_SCHEDULE_TIME_UNIT))
      .withUnits(BASIC_SCHEDULE_UNITS)

  @JvmStatic
  fun generateBasicConnectionScheduleData(): ConnectionScheduleData? =
    ConnectionScheduleData(
      basicSchedule =
        ConnectionScheduleDataBasicSchedule(
          timeUnit = ConnectionScheduleDataBasicSchedule.TimeUnit.DAYS,
          units = BASIC_SCHEDULE_UNITS,
        ),
    )

  @JvmStatic
  fun generateBasicScheduleData(): ScheduleData? =
    ScheduleData().withBasicSchedule(
      BasicSchedule()
        .withTimeUnit(BasicSchedule.TimeUnit.fromValue((BASIC_SCHEDULE_DATA_TIME_UNITS)))
        .withUnits(BASIC_SCHEDULE_DATA_UNITS),
    )

  fun generateExpectedConnectionRead(
    connectionId: UUID,
    sourceId: UUID,
    destinationId: UUID,
    operationIds: List<UUID>?,
    sourceCatalogId: UUID?,
    breaking: Boolean,
    notifySchemaChange: Boolean?,
    notifySchemaChangeByEmail: Boolean?,
    backfillPreference: SchemaChangeBackfillPreference?,
    tags: List<Tag>?,
    onDemandEnabled: Boolean = false,
    enableIndexing: Boolean = false,
    status: ConnectionStatus = ConnectionStatus.ACTIVE,
  ): ConnectionRead =
    ConnectionRead(
      connectionId = connectionId,
      status = status,
      sourceId = sourceId,
      destinationId = destinationId,
      operationIds = operationIds,
      name = "presto to hudi",
      namespaceDefinition = NamespaceDefinitionType.SOURCE,
      namespaceFormat = null,
      prefix = "presto_to_hudi",
      schedule = generateBasicConnectionSchedule(),
      scheduleType = ConnectionScheduleType.BASIC,
      scheduleData = generateBasicConnectionScheduleData(),
      syncCatalog = generateBasicApiCatalog(),
      resourceRequirements =
        io.airbyte.api.server.generated.models.ResourceRequirements(
          cpuRequest = TESTING_RESOURCE_REQUIREMENTS.cpuRequest,
          cpuLimit = TESTING_RESOURCE_REQUIREMENTS.cpuLimit,
          memoryRequest = TESTING_RESOURCE_REQUIREMENTS.memoryRequest,
          memoryLimit = TESTING_RESOURCE_REQUIREMENTS.memoryLimit,
        ),
      sourceCatalogId = sourceCatalogId,
      breakingChange = breaking,
      notifySchemaChanges = notifySchemaChange,
      notifySchemaChangesByEmail = notifySchemaChangeByEmail,
      backfillPreference = backfillPreference,
      tags = tags,
      onDemandEnabled = onDemandEnabled,
      enableIndexing = enableIndexing,
    )

  @JvmStatic
  fun generateExpectedConnectionRead(standardSync: StandardSync): ConnectionRead {
    val connectionRead =
      generateExpectedConnectionRead(
        standardSync.connectionId,
        standardSync.sourceId,
        standardSync.destinationId,
        standardSync.operationIds,
        standardSync.sourceCatalogId,
        standardSync.breakingChange,
        standardSync.notifySchemaChanges,
        standardSync.notifySchemaChangesByEmail,
        standardSync.backfillPreference?.convertTo<SchemaChangeBackfillPreference>(),
        standardSync.tags.map { tag -> apiPojoConverters.toApiTag(tag) },
        standardSync.onDemandEnabled,
        standardSync.enableIndexing,
        standardSync.status.convertTo<ConnectionStatus>(),
      )

    val schedule: Schedule? = standardSync.schedule
    return connectionRead.copy(
      schedule =
        schedule?.let {
          ConnectionSchedule(
            timeUnit = ConnectionSchedule.TimeUnit.entries.first { unit -> unit.value == it.timeUnit.value() },
            units = it.units,
          )
        },
    )
  }

  @JvmStatic
  fun connectionReadFromStandardSync(standardSync: StandardSync): ConnectionRead {
    val resourceRequirements: ResourceRequirements? = standardSync.resourceRequirements
    return ConnectionRead(
      connectionId = standardSync.connectionId,
      sourceId = standardSync.sourceId,
      destinationId = standardSync.destinationId,
      operationIds = standardSync.operationIds,
      name = standardSync.name,
      namespaceFormat = standardSync.namespaceFormat,
      prefix = standardSync.prefix,
      sourceCatalogId = standardSync.sourceCatalogId,
      breakingChange = standardSync.breakingChange,
      notifySchemaChanges = standardSync.notifySchemaChanges,
      notifySchemaChangesByEmail = standardSync.notifySchemaChangesByEmail,
      onDemandEnabled = standardSync.onDemandEnabled,
      enableIndexing = standardSync.enableIndexing,
      namespaceDefinition = standardSync.namespaceDefinition?.convertTo<NamespaceDefinitionType>(),
      status = standardSync.status.convertTo<ConnectionStatus>(),
      scheduleType = apiPojoConverters.toApiConnectionScheduleType(standardSync),
      scheduleData = apiPojoConverters.toApiConnectionScheduleData(standardSync),
      schedule = apiPojoConverters.toLegacyConnectionSchedule(standardSync),
      syncCatalog = catalogConverters.toApi(standardSync.catalog, standardSync.fieldSelectionData),
      resourceRequirements =
        resourceRequirements?.let {
          io.airbyte.api.server.generated.models.ResourceRequirements(
            cpuLimit = it.cpuLimit,
            cpuRequest = it.cpuRequest,
            memoryLimit = it.memoryLimit,
            memoryRequest = it.memoryRequest,
          )
        },
    )
  }

  fun generateExpectedWebBackendConnectionListItem(
    standardSync: StandardSync,
    source: SourceRead,
    destination: DestinationRead,
    isSyncing: Boolean,
    latestSyncJobCreatedAt: Long?,
    latestSynJobStatus: JobStatus?,
    schemaChange: SchemaChange,
    sourceActorDefinitionVersion: ActorDefinitionVersionRead,
    destinationActorDefinitionVersion: ActorDefinitionVersionRead,
  ): WebBackendConnectionListItem {
    val connectionListItem =
      WebBackendConnectionListItem(
        connectionId = standardSync.connectionId,
        name = standardSync.name,
        source =
          SourceSnippetRead(
            icon = source.icon,
            name = source.name,
            sourceName = source.sourceName,
            sourceDefinitionId = source.sourceDefinitionId,
            sourceId = source.sourceId,
          ),
        destination =
          DestinationSnippetRead(
            icon = destination.icon,
            name = destination.name,
            destinationName = destination.destinationName,
            destinationDefinitionId = destination.destinationDefinitionId,
            destinationId = destination.destinationId,
          ),
        status = apiPojoConverters.toApiStatus(standardSync.status)!!,
        isSyncing = isSyncing,
        latestSyncJobCreatedAt = latestSyncJobCreatedAt,
        latestSyncJobStatus = latestSynJobStatus,
        scheduleType = apiPojoConverters.toApiConnectionScheduleType(standardSync),
        scheduleData = apiPojoConverters.toApiConnectionScheduleData(standardSync),
        schemaChange = schemaChange,
        onDemandEnabled = standardSync.onDemandEnabled ?: false,
        tags = mutableListOf(),
        sourceActorDefinitionVersion = sourceActorDefinitionVersion,
        destinationActorDefinitionVersion = destinationActorDefinitionVersion,
      )

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
    val streams: MutableList<AirbyteStreamAndConfiguration> = mutableListOf()
    streams.add(
      AirbyteStreamAndConfiguration(
        stream = generateBasicApiStream(null),
        config = generateBasicApiStreamConfig(null),
      ),
    )
    return AirbyteCatalog(streams = streams)
  }

  /**
   * Generates an API catalog that has two fields, both selected.
   *
   * @return AirbyteCatalog
   */
  @JvmStatic
  fun generateApiCatalogWithTwoFields(): AirbyteCatalog =
    AirbyteCatalog(
      streams =
        listOf(
          AirbyteStreamAndConfiguration(
            stream = generateApiStreamWithTwoFields(),
            config = generateBasicApiStreamConfig(null),
          ),
        ),
    )

  @JvmStatic
  fun generateMultipleStreamsApiCatalog(
    uniqueNamespace: Boolean,
    uniqueStreamName: Boolean,
    streamsCount: Int,
  ): AirbyteCatalog {
    val streamAndConfigurations: MutableList<AirbyteStreamAndConfiguration> = mutableListOf()
    for (i in 0..<streamsCount) {
      streamAndConfigurations.add(
        AirbyteStreamAndConfiguration(
          stream =
            ConnectionHelpers.generateApiStreamWithNamespace(
              if (uniqueNamespace) Optional.of("namespace-" + i) else Optional.empty(),
              if (uniqueStreamName) Optional.of("stream-" + i) else Optional.empty(),
            ),
          config = generateBasicApiStreamConfig(i.toString()),
        ),
      )
    }
    return AirbyteCatalog(streams = streamAndConfigurations)
  }

  @JvmStatic
  fun generateMultipleStreamsApiCatalog(streamsCount: Int): AirbyteCatalog {
    val streamAndConfigurations: MutableList<AirbyteStreamAndConfiguration> = mutableListOf()
    for (i in 0..<streamsCount) {
      streamAndConfigurations.add(
        AirbyteStreamAndConfiguration(
          stream = generateBasicApiStream(i.toString()),
          config = generateBasicApiStreamConfig(i.toString()),
        ),
      )
    }
    return AirbyteCatalog(streams = streamAndConfigurations)
  }

  private fun generateBasicApiStreamConfig(nameSuffix: String?): AirbyteStreamConfiguration =
    AirbyteStreamConfiguration(
      syncMode = io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL,
      cursorField = listOf(FIELD_NAME),
      destinationSyncMode = io.airbyte.api.server.generated.models.DestinationSyncMode.APPEND,
      primaryKey = mutableListOf(),
      aliasName = toAlphanumericAndUnderscore(if (nameSuffix == null) STREAM_NAME else STREAM_NAME_BASE + nameSuffix),
      selected = true,
      suggested = false,
      includeFiles = false,
      fieldSelectionEnabled = false,
      selectedFields = mutableListOf(),
    )

  private fun generateBasicApiStream(nameSuffix: String?): io.airbyte.api.server.generated.models.AirbyteStream =
    io.airbyte.api.server.generated.models.AirbyteStream(
      name =
        if (nameSuffix ==
          null
        ) {
          STREAM_NAME
        } else {
          STREAM_NAME_BASE + nameSuffix
        },
      jsonSchema = generateBasicJsonSchema(),
      defaultCursorField = listOf(FIELD_NAME),
      sourceDefinedCursor = false,
      sourceDefinedPrimaryKey = mutableListOf(),
      supportedSyncModes =
        listOf(
          io.airbyte.api.server.generated.models.SyncMode.FULL_REFRESH,
          io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL,
        ),
    )

  private fun generateApiStreamWithTwoFields(): io.airbyte.api.server.generated.models.AirbyteStream =
    io.airbyte.api.server.generated.models.AirbyteStream(
      name = STREAM_NAME,
      jsonSchema = generateJsonSchemaWithTwoFields(),
      defaultCursorField = listOf(FIELD_NAME),
      sourceDefinedCursor = false,
      sourceDefinedPrimaryKey = mutableListOf(),
      supportedSyncModes =
        listOf(
          io.airbyte.api.server.generated.models.SyncMode.FULL_REFRESH,
          io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL,
        ),
    )

  private fun generateApiStreamWithNamespace(
    namespace: Optional<String>,
    name: Optional<String>,
  ): io.airbyte.api.server.generated.models.AirbyteStream =
    io.airbyte.api.server.generated.models.AirbyteStream(
      namespace = namespace.orElse(STREAM_NAMESPACE),
      name = name.orElse(STREAM_NAME),
      jsonSchema = generateJsonSchemaWithTwoFields(),
      defaultCursorField = listOf(FIELD_NAME),
      sourceDefinedCursor = false,
      sourceDefinedPrimaryKey = mutableListOf(),
      supportedSyncModes =
        listOf(
          io.airbyte.api.server.generated.models.SyncMode.FULL_REFRESH,
          io.airbyte.api.server.generated.models.SyncMode.INCREMENTAL,
        ),
    )
}
