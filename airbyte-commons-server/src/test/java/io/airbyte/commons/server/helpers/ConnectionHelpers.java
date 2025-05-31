/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionSchedule;
import io.airbyte.api.model.generated.ConnectionSchedule.TimeUnitEnum;
import io.airbyte.api.model.generated.ConnectionScheduleData;
import io.airbyte.api.model.generated.ConnectionScheduleDataBasicSchedule;
import io.airbyte.api.model.generated.ConnectionScheduleType;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationSnippetRead;
import io.airbyte.api.model.generated.JobStatus;
import io.airbyte.api.model.generated.ResourceRequirements;
import io.airbyte.api.model.generated.SchemaChange;
import io.airbyte.api.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceSnippetRead;
import io.airbyte.api.model.generated.SyncMode;
import io.airbyte.api.model.generated.Tag;
import io.airbyte.api.model.generated.WebBackendConnectionListItem;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.text.Names;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.FieldType;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.Schedule;
import io.airbyte.config.Schedule.TimeUnit;
import io.airbyte.config.ScheduleData;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ConnectionHelpers {

  private static final String STREAM_NAME_BASE = "users-data";
  private static final String STREAM_NAME = STREAM_NAME_BASE + "0";
  private static final String STREAM_NAMESPACE = "namespace";
  public static final String FIELD_NAME = "id";

  public static final String SECOND_FIELD_NAME = "id2";
  private static final String BASIC_SCHEDULE_TIME_UNIT = "days";
  private static final long BASIC_SCHEDULE_UNITS = 1L;
  private static final String BASIC_SCHEDULE_DATA_TIME_UNITS = "days";
  private static final long BASIC_SCHEDULE_DATA_UNITS = 1L;
  private static final String ONE_HUNDRED_G = "100g";
  private static final String STANDARD_SYNC_NAME = "presto to hudi";
  private static final String STANDARD_SYNC_PREFIX = "presto_to_hudi";
  private static final FieldGenerator fieldGenerator = new FieldGenerator();

  private static final CatalogConverter catalogConverters = new CatalogConverter(fieldGenerator, Collections.emptyList());

  private static final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(catalogConverters);

  public static final StreamDescriptor STREAM_DESCRIPTOR = new StreamDescriptor().withName(STREAM_NAME);

  // only intended for unit tests, so intentionally set very high to ensure they aren't being used
  // elsewhere
  public static final io.airbyte.config.ResourceRequirements TESTING_RESOURCE_REQUIREMENTS = new io.airbyte.config.ResourceRequirements()
      .withCpuLimit(ONE_HUNDRED_G)
      .withCpuRequest(ONE_HUNDRED_G)
      .withMemoryLimit(ONE_HUNDRED_G)
      .withMemoryRequest(ONE_HUNDRED_G);

  public static StandardSync generateSyncWithSourceId(final UUID sourceId) {
    final UUID connectionId = UUID.randomUUID();

    return new StandardSync()
        .withConnectionId(connectionId)
        .withName(STANDARD_SYNC_NAME)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(STANDARD_SYNC_PREFIX)
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withSourceId(sourceId)
        .withDestinationId(UUID.randomUUID())
        .withOperationIds(List.of(UUID.randomUUID()))
        .withManual(false)
        .withSchedule(generateBasicSchedule())
        .withResourceRequirements(TESTING_RESOURCE_REQUIREMENTS)
        .withBreakingChange(false);
  }

  public static StandardSync generateSyncWithDestinationId(final UUID destinationId) {
    final UUID connectionId = UUID.randomUUID();

    return new StandardSync()
        .withConnectionId(connectionId)
        .withName(STANDARD_SYNC_NAME)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(STANDARD_SYNC_PREFIX)
        .withStatus(StandardSync.Status.ACTIVE)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withSourceId(UUID.randomUUID())
        .withDestinationId(destinationId)
        .withOperationIds(List.of(UUID.randomUUID()))
        .withManual(true);
  }

  public static StandardSync generateSyncWithSourceAndDestinationId(final UUID sourceId,
                                                                    final UUID destinationId,
                                                                    final boolean isBroken,
                                                                    final Status status) {
    final UUID connectionId = UUID.randomUUID();

    return new StandardSync()
        .withConnectionId(connectionId)
        .withName(STANDARD_SYNC_NAME)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withNamespaceFormat(null)
        .withPrefix(STANDARD_SYNC_PREFIX)
        .withStatus(status)
        .withCatalog(generateBasicConfiguredAirbyteCatalog())
        .withSourceCatalogId(UUID.randomUUID())
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withOperationIds(List.of(UUID.randomUUID()))
        .withManual(true)
        .withBreakingChange(isBroken)
        .withNotifySchemaChanges(false)
        .withNotifySchemaChangesByEmail(true);
  }

  public static ConnectionSchedule generateBasicConnectionSchedule() {
    return new ConnectionSchedule()
        .timeUnit(ConnectionSchedule.TimeUnitEnum.fromValue(BASIC_SCHEDULE_TIME_UNIT))
        .units(BASIC_SCHEDULE_UNITS);
  }

  public static Schedule generateBasicSchedule() {
    return new Schedule()
        .withTimeUnit(TimeUnit.fromValue(BASIC_SCHEDULE_TIME_UNIT))
        .withUnits(BASIC_SCHEDULE_UNITS);
  }

  public static ConnectionScheduleData generateBasicConnectionScheduleData() {
    return new ConnectionScheduleData().basicSchedule(
        new ConnectionScheduleDataBasicSchedule().timeUnit(ConnectionScheduleDataBasicSchedule.TimeUnitEnum.DAYS).units(BASIC_SCHEDULE_UNITS));
  }

  public static ScheduleData generateBasicScheduleData() {
    return new ScheduleData().withBasicSchedule(new BasicSchedule()
        .withTimeUnit(BasicSchedule.TimeUnit.fromValue((BASIC_SCHEDULE_DATA_TIME_UNITS)))
        .withUnits(BASIC_SCHEDULE_DATA_UNITS));
  }

  public static ConnectionRead generateExpectedConnectionRead(final UUID connectionId,
                                                              final UUID sourceId,
                                                              final UUID destinationId,
                                                              final List<UUID> operationIds,
                                                              final UUID sourceCatalogId,
                                                              final UUID dataplaneGroupId,
                                                              final boolean breaking,
                                                              final Boolean notifySchemaChange,
                                                              final Boolean notifySchemaChangeByEmail,
                                                              final SchemaChangeBackfillPreference backfillPreference,
                                                              final List<Tag> tags) {

    return new ConnectionRead()
        .connectionId(connectionId)
        .sourceId(sourceId)
        .destinationId(destinationId)
        .operationIds(operationIds)
        .name("presto to hudi")
        .namespaceDefinition(io.airbyte.api.model.generated.NamespaceDefinitionType.SOURCE)
        .namespaceFormat(null)
        .prefix("presto_to_hudi")
        .schedule(generateBasicConnectionSchedule())
        .scheduleType(ConnectionScheduleType.BASIC)
        .scheduleData(generateBasicConnectionScheduleData())
        .syncCatalog(ConnectionHelpers.generateBasicApiCatalog())
        .resourceRequirements(new ResourceRequirements()
            .cpuRequest(TESTING_RESOURCE_REQUIREMENTS.getCpuRequest())
            .cpuLimit(TESTING_RESOURCE_REQUIREMENTS.getCpuLimit())
            .memoryRequest(TESTING_RESOURCE_REQUIREMENTS.getMemoryRequest())
            .memoryLimit(TESTING_RESOURCE_REQUIREMENTS.getMemoryLimit()))
        .sourceCatalogId(sourceCatalogId)
        .dataplaneGroupId(dataplaneGroupId)
        .breakingChange(breaking)
        .notifySchemaChanges(notifySchemaChange)
        .notifySchemaChangesByEmail(notifySchemaChangeByEmail)
        .backfillPreference(backfillPreference)
        .tags(tags);
  }

  public static ConnectionRead generateExpectedConnectionRead(final StandardSync standardSync) {
    final ConnectionRead connectionRead = generateExpectedConnectionRead(
        standardSync.getConnectionId(),
        standardSync.getSourceId(),
        standardSync.getDestinationId(),
        standardSync.getOperationIds(),
        standardSync.getSourceCatalogId(),
        standardSync.getDataplaneGroupId(),
        standardSync.getBreakingChange(),
        standardSync.getNotifySchemaChanges(),
        standardSync.getNotifySchemaChangesByEmail(),
        Enums.convertTo(standardSync.getBackfillPreference(), SchemaChangeBackfillPreference.class),
        standardSync.getTags().stream().map(apiPojoConverters::toApiTag).toList());

    if (standardSync.getSchedule() == null) {
      connectionRead.schedule(null);
    } else {
      connectionRead.schedule(new ConnectionSchedule()
          .timeUnit(TimeUnitEnum.fromValue(standardSync.getSchedule().getTimeUnit().value()))
          .units(standardSync.getSchedule().getUnits()));
    }

    if (standardSync.getStatus() == Status.INACTIVE) {
      connectionRead.setStatus(ConnectionStatus.INACTIVE);
    } else if (standardSync.getStatus() == Status.ACTIVE) {
      connectionRead.setStatus(ConnectionStatus.ACTIVE);
    } else if (standardSync.getStatus() == Status.DEPRECATED) {
      connectionRead.setStatus(ConnectionStatus.DEPRECATED);
    }

    return connectionRead;
  }

  public static ConnectionRead connectionReadFromStandardSync(final StandardSync standardSync) {
    final ConnectionRead connectionRead = new ConnectionRead();
    connectionRead
        .connectionId(standardSync.getConnectionId())
        .sourceId(standardSync.getSourceId())
        .destinationId(standardSync.getDestinationId())
        .operationIds(standardSync.getOperationIds())
        .name(standardSync.getName())
        .namespaceFormat(standardSync.getNamespaceFormat())
        .prefix(standardSync.getPrefix())
        .sourceCatalogId(standardSync.getSourceCatalogId())
        .dataplaneGroupId(standardSync.getDataplaneGroupId())
        .breakingChange(standardSync.getBreakingChange())
        .notifySchemaChanges(standardSync.getNotifySchemaChanges())
        .notifySchemaChangesByEmail(standardSync.getNotifySchemaChangesByEmail());

    if (standardSync.getNamespaceDefinition() != null) {
      connectionRead
          .namespaceDefinition(io.airbyte.api.model.generated.NamespaceDefinitionType.fromValue(standardSync.getNamespaceDefinition().value()));
    }
    if (standardSync.getStatus() != null) {
      connectionRead.status(io.airbyte.api.model.generated.ConnectionStatus.fromValue(standardSync.getStatus().value()));
    }
    apiPojoConverters.populateConnectionReadSchedule(standardSync, connectionRead);

    if (standardSync.getCatalog() != null) {
      connectionRead.syncCatalog(catalogConverters.toApi(standardSync.getCatalog(), standardSync.getFieldSelectionData()));
    }
    if (standardSync.getResourceRequirements() != null) {
      connectionRead.resourceRequirements(new io.airbyte.api.model.generated.ResourceRequirements()
          .cpuLimit(standardSync.getResourceRequirements().getCpuLimit())
          .cpuRequest(standardSync.getResourceRequirements().getCpuRequest())
          .memoryLimit(standardSync.getResourceRequirements().getMemoryLimit())
          .memoryRequest(standardSync.getResourceRequirements().getMemoryRequest()));
    }
    return connectionRead;
  }

  public static WebBackendConnectionListItem generateExpectedWebBackendConnectionListItem(
                                                                                          final StandardSync standardSync,
                                                                                          final SourceRead source,
                                                                                          final DestinationRead destination,
                                                                                          final boolean isSyncing,
                                                                                          final Long latestSyncJobCreatedAt,
                                                                                          final JobStatus latestSynJobStatus,
                                                                                          final SchemaChange schemaChange) {

    final WebBackendConnectionListItem connectionListItem = new WebBackendConnectionListItem()
        .connectionId(standardSync.getConnectionId())
        .name(standardSync.getName())
        .source(new SourceSnippetRead()
            .icon(source.getIcon())
            .name(source.getName())
            .sourceName(source.getSourceName())
            .sourceDefinitionId(source.getSourceDefinitionId())
            .sourceId(source.getSourceId()))
        .destination(new DestinationSnippetRead()
            .icon(destination.getIcon())
            .name(destination.getName())
            .destinationName(destination.getDestinationName())
            .destinationDefinitionId(destination.getDestinationDefinitionId())
            .destinationId(destination.getDestinationId()))
        .status(apiPojoConverters.toApiStatus(standardSync.getStatus()))
        .isSyncing(isSyncing)
        .latestSyncJobCreatedAt(latestSyncJobCreatedAt)
        .latestSyncJobStatus(latestSynJobStatus)
        .scheduleType(apiPojoConverters.toApiConnectionScheduleType(standardSync))
        .scheduleData(apiPojoConverters.toApiConnectionScheduleData(standardSync))
        .schemaChange(schemaChange)
        .tags(Collections.emptyList());

    return connectionListItem;
  }

  public static JsonNode generateBasicJsonSchema() {
    return CatalogHelpers.fieldsToJsonSchema(Field.of(FIELD_NAME, JsonSchemaType.STRING));
  }

  public static JsonNode generateJsonSchemaWithTwoFields() {
    return CatalogHelpers.fieldsToJsonSchema(
        Field.of(FIELD_NAME, JsonSchemaType.STRING),
        Field.of(SECOND_FIELD_NAME, JsonSchemaType.STRING));
  }

  public static ConfiguredAirbyteCatalog generateBasicConfiguredAirbyteCatalog() {
    return new ConfiguredAirbyteCatalog().withStreams(List.of(generateBasicConfiguredStream(null)));
  }

  public static ConfiguredAirbyteCatalog generateAirbyteCatalogWithTwoFields() {
    return new ConfiguredAirbyteCatalog(List.of(new ConfiguredAirbyteStream.Builder()
        .stream(new io.airbyte.config.AirbyteStream(STREAM_NAME, generateJsonSchemaWithTwoFields(),
            List.of(io.airbyte.config.SyncMode.FULL_REFRESH, io.airbyte.config.SyncMode.INCREMENTAL))
                .withDefaultCursorField(List.of(FIELD_NAME))
                .withSourceDefinedCursor(false)
                .withSourceDefinedPrimaryKey(List.of()))
        .cursorField(List.of(FIELD_NAME))
        .syncMode(io.airbyte.config.SyncMode.INCREMENTAL)
        .destinationSyncMode(DestinationSyncMode.APPEND)
        .fields(fieldGenerator.getFieldsFromSchema(generateJsonSchemaWithTwoFields()))
        .build()));
  }

  public static ConfiguredAirbyteCatalog generateMultipleStreamsConfiguredAirbyteCatalog(final int streamsCount) {
    final List<ConfiguredAirbyteStream> configuredStreams = new ArrayList<>();
    for (int i = 0; i < streamsCount; i++) {
      configuredStreams.add(generateBasicConfiguredStream(String.valueOf(i)));
    }
    return new ConfiguredAirbyteCatalog().withStreams(configuredStreams);
  }

  public static ConfiguredAirbyteStream generateBasicConfiguredStream(final String nameSuffix) {
    return new ConfiguredAirbyteStream.Builder()
        .stream(generateBasicAirbyteStream(nameSuffix))
        .syncMode(io.airbyte.config.SyncMode.INCREMENTAL)
        .destinationSyncMode(DestinationSyncMode.APPEND)
        .cursorField(List.of(FIELD_NAME))
        .fields(List.of(new io.airbyte.config.Field(FIELD_NAME, FieldType.STRING)))
        .build();
  }

  private static io.airbyte.config.AirbyteStream generateBasicAirbyteStream(final String nameSuffix) {
    return CatalogHelpers.createAirbyteStream(
        nameSuffix == null ? STREAM_NAME : STREAM_NAME_BASE + nameSuffix, Field.of(FIELD_NAME, JsonSchemaType.STRING))
        .withDefaultCursorField(List.of(FIELD_NAME))
        .withSourceDefinedCursor(false)
        .withSupportedSyncModes(List.of(io.airbyte.config.SyncMode.FULL_REFRESH, io.airbyte.config.SyncMode.INCREMENTAL));
  }

  public static AirbyteCatalog generateBasicApiCatalog() {
    final List<AirbyteStreamAndConfiguration> streams = new ArrayList<>();
    streams.add(new AirbyteStreamAndConfiguration()
        .stream(generateBasicApiStream(null))
        .config(generateBasicApiStreamConfig(null)));
    return new AirbyteCatalog().streams(streams);
  }

  /**
   * Generates an API catalog that has two fields, both selected.
   *
   * @return AirbyteCatalog
   */
  public static AirbyteCatalog generateApiCatalogWithTwoFields() {
    return new AirbyteCatalog().streams(List.of(new AirbyteStreamAndConfiguration()
        .stream(generateApiStreamWithTwoFields())
        .config(generateBasicApiStreamConfig(null))));
  }

  public static AirbyteCatalog generateMultipleStreamsApiCatalog(final boolean uniqueNamespace,
                                                                 final boolean uniqueStreamName,
                                                                 final int streamsCount) {
    final List<AirbyteStreamAndConfiguration> streamAndConfigurations = new ArrayList<>();
    for (int i = 0; i < streamsCount; i++) {
      streamAndConfigurations.add(new AirbyteStreamAndConfiguration()
          .stream(generateApiStreamWithNamespace(uniqueNamespace ? Optional.of("namespace-" + i) : Optional.empty(),
              uniqueStreamName ? Optional.of("stream-" + i) : Optional.empty()))
          .config(generateBasicApiStreamConfig(String.valueOf(i))));
    }
    return new AirbyteCatalog().streams(streamAndConfigurations);
  }

  public static AirbyteCatalog generateMultipleStreamsApiCatalog(final int streamsCount) {
    final List<AirbyteStreamAndConfiguration> streamAndConfigurations = new ArrayList<>();
    for (int i = 0; i < streamsCount; i++) {
      streamAndConfigurations.add(new AirbyteStreamAndConfiguration()
          .stream(generateBasicApiStream(String.valueOf(i)))
          .config(generateBasicApiStreamConfig(String.valueOf(i))));
    }
    return new AirbyteCatalog().streams(streamAndConfigurations);
  }

  private static AirbyteStreamConfiguration generateBasicApiStreamConfig(final String nameSuffix) {
    return new AirbyteStreamConfiguration()
        .syncMode(SyncMode.INCREMENTAL)
        .cursorField(List.of(FIELD_NAME))
        .destinationSyncMode(io.airbyte.api.model.generated.DestinationSyncMode.APPEND)
        .primaryKey(List.of())
        .aliasName(Names.toAlphanumericAndUnderscore(nameSuffix == null ? STREAM_NAME : STREAM_NAME_BASE + nameSuffix))
        .selected(true)
        .suggested(false)
        .includeFiles(false)
        .fieldSelectionEnabled(false)
        .selectedFields(new ArrayList<>());
  }

  private static AirbyteStream generateBasicApiStream(final String nameSuffix) {
    return new AirbyteStream()
        .name(nameSuffix == null ? STREAM_NAME : STREAM_NAME_BASE + nameSuffix)
        .jsonSchema(generateBasicJsonSchema())
        .defaultCursorField(List.of(FIELD_NAME))
        .sourceDefinedCursor(false)
        .sourceDefinedPrimaryKey(List.of())
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
  }

  private static AirbyteStream generateApiStreamWithTwoFields() {
    return new AirbyteStream()
        .name(STREAM_NAME)
        .jsonSchema(generateJsonSchemaWithTwoFields())
        .defaultCursorField(List.of(FIELD_NAME))
        .sourceDefinedCursor(false)
        .sourceDefinedPrimaryKey(List.of())
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
  }

  private static AirbyteStream generateApiStreamWithNamespace(final Optional<String> namespace, final Optional<String> name) {
    return new AirbyteStream()
        .namespace(namespace.orElse(STREAM_NAMESPACE))
        .name(name.orElse(STREAM_NAME))
        .jsonSchema(generateJsonSchemaWithTwoFields())
        .defaultCursorField(List.of(FIELD_NAME))
        .sourceDefinedCursor(false)
        .sourceDefinedPrimaryKey(List.of())
        .supportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
  }

}
