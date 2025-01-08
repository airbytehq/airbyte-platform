/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.converters.ConnectionHelper.validateCatalogDoesntContainDuplicateStreamNames;
import static io.airbyte.config.Job.REPLICATION_TYPES;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.common.StreamDescriptorUtils;
import io.airbyte.api.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionEventIdRequestBody;
import io.airbyte.api.model.generated.ConnectionEventList;
import io.airbyte.api.model.generated.ConnectionEventType;
import io.airbyte.api.model.generated.ConnectionEventWithDetails;
import io.airbyte.api.model.generated.ConnectionEventsBackfillRequestBody;
import io.airbyte.api.model.generated.ConnectionEventsRequestBody;
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamReadItem;
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.ConnectionStatusRead;
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncStatus;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FailureOrigin;
import io.airbyte.api.model.generated.FailureReason;
import io.airbyte.api.model.generated.FailureType;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobSyncResultRead;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.PostprocessDiscoveredCatalogResult;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamStats;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.problems.model.generated.ProblemMapperErrorData;
import io.airbyte.api.problems.model.generated.ProblemMapperErrorDataMapper;
import io.airbyte.api.problems.model.generated.ProblemMapperErrorsData;
import io.airbyte.api.problems.model.generated.ProblemMessageData;
import io.airbyte.api.problems.throwable.generated.MapperValidationProblem;
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem;
import io.airbyte.commons.converters.ApiConverters;
import io.airbyte.commons.converters.ConnectionHelper;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.CatalogDiffHelpers;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.CatalogDiffConverters;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper;
import io.airbyte.commons.server.handlers.helpers.ApplySchemaChangeHelper.UpdateSchemaResult;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.ConnectionScheduleHelper;
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper;
import io.airbyte.commons.server.handlers.helpers.MapperSecretHelper;
import io.airbyte.commons.server.handlers.helpers.NotificationHelper;
import io.airbyte.commons.server.handlers.helpers.PaginationHelper;
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.server.validation.CatalogValidator;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptWithJobInfo;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.FieldSelectionData;
import io.airbyte.config.Geography;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.Schedule;
import io.airbyte.config.ScheduleData;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.ScheduleType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.config.persistence.StreamGenerationRepository;
import io.airbyte.config.persistence.domain.Generation;
import io.airbyte.config.persistence.helper.CatalogGenerationSetter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.repositories.entities.ConnectionTimelineEvent;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.StreamStatusesService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason;
import io.airbyte.data.services.shared.ConnectionAutoUpdatedReason;
import io.airbyte.data.services.shared.ConnectionEvent;
import io.airbyte.data.services.shared.FailedEvent;
import io.airbyte.data.services.shared.FinalStatusEvent;
import io.airbyte.featureflag.CheckWithCatalog;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.ResetStreamsStateWhenDisabled;
import io.airbyte.featureflag.Workspace;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator.CatalogGenerationResult;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectionsHandler. Javadocs suppressed because api docs should be used as source of truth.
 *
 * @deprecated New connection-related functionality should be added to the ConnectionService
 */
@Singleton
@SuppressWarnings("PMD.PreserveStackTrace")
@Deprecated
public class ConnectionsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionsHandler.class);
  public static final int DEFAULT_PAGE_SIZE = 20;
  public static final int DEFAULT_ROW_OFFSET = 0;

  private final JobPersistence jobPersistence;
  private final Supplier<UUID> uuidGenerator;
  private final WorkspaceHelper workspaceHelper;
  private final TrackingClient trackingClient;
  private final EventRunner eventRunner;
  private final ConnectionHelper connectionHelper;
  private final FeatureFlagClient featureFlagClient;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final ConnectorDefinitionSpecificationHandler connectorSpecHandler;
  private final int maxJobLookback = 10;
  private final StreamRefreshesHandler streamRefreshesHandler;
  private final StreamGenerationRepository streamGenerationRepository;
  private final CatalogGenerationSetter catalogGenerationSetter;
  private final CatalogValidator catalogValidator;
  private final NotificationHelper notificationHelper;
  private final StreamStatusesService streamStatusesService;
  private final ConnectionTimelineEventService connectionTimelineEventService;
  private final ConnectionTimelineEventHelper connectionTimelineEventHelper;
  private final StatePersistence statePersistence;
  private final MapperSecretHelper mapperSecretHelper;

  private final CatalogService catalogService;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final ConnectionService connectionService;
  private final WorkspaceService workspaceService;
  private final DestinationCatalogGenerator destinationCatalogGenerator;
  private final CatalogConverter catalogConverter;
  private final ApplySchemaChangeHelper applySchemaChangeHelper;
  private final ApiPojoConverters apiPojoConverters;

  private final ConnectionScheduleHelper connectionScheduleHelper;

  // TODO: Worth considering how we might refactor this. The arguments list feels a little long.
  @Inject
  public ConnectionsHandler(final StreamRefreshesHandler streamRefreshesHandler,
                            final JobPersistence jobPersistence,
                            final CatalogService catalogService,
                            @Named("uuidGenerator") final Supplier<UUID> uuidGenerator,
                            final WorkspaceHelper workspaceHelper,
                            final TrackingClient trackingClient,
                            final EventRunner eventRunner,
                            final ConnectionHelper connectionHelper,
                            final FeatureFlagClient featureFlagClient,
                            final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                            final ConnectorDefinitionSpecificationHandler connectorSpecHandler,
                            final StreamGenerationRepository streamGenerationRepository,
                            final CatalogGenerationSetter catalogGenerationSetter,
                            final CatalogValidator catalogValidator,
                            final NotificationHelper notificationHelper,
                            final StreamStatusesService streamStatusesService,
                            final ConnectionTimelineEventService connectionTimelineEventService,
                            final ConnectionTimelineEventHelper connectionTimelineEventHelper,
                            final StatePersistence statePersistence,
                            final SourceService sourceService,
                            final DestinationService destinationService,
                            final ConnectionService connectionService,
                            final WorkspaceService workspaceService,
                            final DestinationCatalogGenerator destinationCatalogGenerator,
                            final CatalogConverter catalogConverter,
                            final ApplySchemaChangeHelper applySchemaChangeHelper,
                            final ApiPojoConverters apiPojoConverters,
                            final ConnectionScheduleHelper connectionScheduleHelper,
                            final MapperSecretHelper mapperSecretHelper) {
    this.jobPersistence = jobPersistence;
    this.catalogService = catalogService;
    this.uuidGenerator = uuidGenerator;
    this.workspaceHelper = workspaceHelper;
    this.trackingClient = trackingClient;
    this.eventRunner = eventRunner;
    this.connectionHelper = connectionHelper;
    this.featureFlagClient = featureFlagClient;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.connectorSpecHandler = connectorSpecHandler;
    this.streamRefreshesHandler = streamRefreshesHandler;
    this.streamGenerationRepository = streamGenerationRepository;
    this.catalogGenerationSetter = catalogGenerationSetter;
    this.catalogValidator = catalogValidator;
    this.notificationHelper = notificationHelper;
    this.streamStatusesService = streamStatusesService;
    this.connectionTimelineEventService = connectionTimelineEventService;
    this.connectionTimelineEventHelper = connectionTimelineEventHelper;
    this.statePersistence = statePersistence;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.connectionService = connectionService;
    this.workspaceService = workspaceService;
    this.destinationCatalogGenerator = destinationCatalogGenerator;
    this.catalogConverter = catalogConverter;
    this.applySchemaChangeHelper = applySchemaChangeHelper;
    this.apiPojoConverters = apiPojoConverters;
    this.connectionScheduleHelper = connectionScheduleHelper;
    this.mapperSecretHelper = mapperSecretHelper;
  }

  /**
   * Modifies the given StandardSync by applying changes from a partially-filled ConnectionUpdate
   * patch. Any fields that are null in the patch will be left unchanged.
   */
  private void applyPatchToStandardSync(final StandardSync sync, final ConnectionUpdate patch, final UUID workspaceId)
      throws JsonValidationException, ConfigNotFoundException {
    // update the sync's schedule using the patch's scheduleType and scheduleData. validations occur in
    // the helper to ensure both fields
    // make sense together.
    if (patch.getScheduleType() != null) {
      connectionScheduleHelper.populateSyncFromScheduleTypeAndData(sync, patch.getScheduleType(), patch.getScheduleData());
    }

    // the rest of the fields are straightforward to patch. If present in the patch, set the field to
    // the value
    // in the patch. Otherwise, leave the field unchanged.

    if (patch.getSyncCatalog() != null) {
      validateCatalogDoesntContainDuplicateStreamNames(patch.getSyncCatalog());
      validateCatalogSize(patch.getSyncCatalog(), workspaceId, "update");

      assignIdsToIncomingMappers(patch.getSyncCatalog());
      final ConfiguredAirbyteCatalog configuredCatalog = catalogConverter.toConfiguredInternal(patch.getSyncCatalog());
      validateConfiguredMappers(configuredCatalog);

      final ConfiguredAirbyteCatalog configuredCatalogNoSecrets =
          mapperSecretHelper.updateAndReplaceMapperSecrets(workspaceId, sync.getCatalog(), configuredCatalog);
      sync.setCatalog(configuredCatalogNoSecrets);
      sync.withFieldSelectionData(catalogConverter.getFieldSelectionData(patch.getSyncCatalog()));
    }

    if (patch.getName() != null) {
      sync.setName(patch.getName());
    }

    if (patch.getNamespaceDefinition() != null) {
      sync.setNamespaceDefinition(Enums.convertTo(patch.getNamespaceDefinition(), NamespaceDefinitionType.class));
    }

    if (patch.getNamespaceFormat() != null) {
      sync.setNamespaceFormat(patch.getNamespaceFormat());
    }

    if (patch.getPrefix() != null) {
      sync.setPrefix(patch.getPrefix());
    }

    if (patch.getOperationIds() != null) {
      sync.setOperationIds(patch.getOperationIds());
    }

    if (patch.getStatus() != null) {
      sync.setStatus(apiPojoConverters.toPersistenceStatus(patch.getStatus()));
    }

    if (patch.getSourceCatalogId() != null) {
      sync.setSourceCatalogId(patch.getSourceCatalogId());
    }

    if (patch.getResourceRequirements() != null) {
      sync.setResourceRequirements(apiPojoConverters.resourceRequirementsToInternal(patch.getResourceRequirements()));
    }

    if (patch.getGeography() != null) {
      sync.setGeography(apiPojoConverters.toPersistenceGeography(patch.getGeography()));
    }

    if (patch.getBreakingChange() != null) {
      sync.setBreakingChange(patch.getBreakingChange());
    }

    if (patch.getNotifySchemaChanges() != null) {
      sync.setNotifySchemaChanges(patch.getNotifySchemaChanges());
    }

    if (patch.getNotifySchemaChangesByEmail() != null) {
      sync.setNotifySchemaChangesByEmail(patch.getNotifySchemaChangesByEmail());
    }

    if (patch.getNonBreakingChangesPreference() != null) {
      sync.setNonBreakingChangesPreference(apiPojoConverters.toPersistenceNonBreakingChangesPreference(patch.getNonBreakingChangesPreference()));
    }

    if (patch.getBackfillPreference() != null) {
      sync.setBackfillPreference(apiPojoConverters.toPersistenceBackfillPreference(patch.getBackfillPreference()));
    }
  }

  private static String getFrequencyStringFromScheduleType(final ScheduleType scheduleType, final ScheduleData scheduleData) {
    switch (scheduleType) {
      case MANUAL -> {
        return "manual";
      }
      case BASIC_SCHEDULE -> {
        return TimeUnit.SECONDS.toMinutes(ScheduleHelpers.getIntervalInSecond(scheduleData.getBasicSchedule())) + " min";
      }
      case CRON -> {
        // TODO(https://github.com/airbytehq/airbyte/issues/2170): consider something more detailed.
        return "cron";
      }
      default -> {
        throw new RuntimeException("Unexpected schedule type");
      }
    }
  }

  private void assignIdsToIncomingMappers(final AirbyteCatalog catalog) {
    catalog.getStreams().forEach(stream -> stream.getConfig().getMappers().forEach(mapper -> {
      if (mapper.getId() == null) {
        mapper.setId(uuidGenerator.get());
      }
    }));
  }

  private void validateConfiguredMappers(final ConfiguredAirbyteCatalog configuredCatalog) {
    final CatalogGenerationResult result = destinationCatalogGenerator.generateDestinationCatalog(configuredCatalog);
    if (result.getErrors().isEmpty()) {
      return;
    }

    final List<ProblemMapperErrorData> errors = result.getErrors().entrySet().stream()
        .flatMap(streamErrors -> streamErrors.getValue().entrySet().stream().map(mapperError -> new ProblemMapperErrorData()
            .stream(streamErrors.getKey().getName())
            .error(mapperError.getValue().getType().name())
            .mapper(
                new ProblemMapperErrorDataMapper()
                    .id(mapperError.getKey().id())
                    .type(mapperError.getKey().name())
                    .mapperConfiguration(mapperError.getKey().config()))))
        .toList();

    if (!errors.isEmpty()) {
      throw new MapperValidationProblem(new ProblemMapperErrorsData().errors(errors));
    }
  }

  public ConnectionRead createConnection(final ConnectionCreate connectionCreate)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {

    // Validate source and destination
    final SourceConnection sourceConnection = sourceService.getSourceConnection(connectionCreate.getSourceId());
    final DestinationConnection destinationConnection = destinationService.getDestinationConnection(connectionCreate.getDestinationId());

    // Set this as default name if connectionCreate doesn't have it
    final String defaultName = sourceConnection.getName() + " <> " + destinationConnection.getName();

    final List<UUID> operationIds = connectionCreate.getOperationIds() != null ? connectionCreate.getOperationIds() : Collections.emptyList();

    ConnectionHelper.validateWorkspace(workspaceHelper,
        connectionCreate.getSourceId(),
        connectionCreate.getDestinationId(),
        operationIds);

    final UUID workspaceId = workspaceHelper.getWorkspaceForDestinationId(connectionCreate.getDestinationId());
    final UUID connectionId = uuidGenerator.get();

    // If not specified, default the NamespaceDefinition to 'source'
    final NamespaceDefinitionType namespaceDefinitionType =
        connectionCreate.getNamespaceDefinition() == null
            ? NamespaceDefinitionType.SOURCE
            : Enums.convertTo(connectionCreate.getNamespaceDefinition(), NamespaceDefinitionType.class);

    // persist sync
    final StandardSync standardSync = new StandardSync()
        .withConnectionId(connectionId)
        .withName(connectionCreate.getName() != null ? connectionCreate.getName() : defaultName)
        .withNamespaceDefinition(namespaceDefinitionType)
        .withNamespaceFormat(connectionCreate.getNamespaceFormat())
        .withPrefix(connectionCreate.getPrefix())
        .withSourceId(connectionCreate.getSourceId())
        .withDestinationId(connectionCreate.getDestinationId())
        .withOperationIds(operationIds)
        .withStatus(apiPojoConverters.toPersistenceStatus(connectionCreate.getStatus()))
        .withSourceCatalogId(connectionCreate.getSourceCatalogId())
        .withGeography(getGeographyFromConnectionCreateOrWorkspace(connectionCreate))
        .withBreakingChange(false)
        .withNotifySchemaChanges(connectionCreate.getNotifySchemaChanges())
        .withNonBreakingChangesPreference(
            apiPojoConverters.toPersistenceNonBreakingChangesPreference(connectionCreate.getNonBreakingChangesPreference()))
        .withBackfillPreference(apiPojoConverters.toPersistenceBackfillPreference(connectionCreate.getBackfillPreference()));
    if (connectionCreate.getResourceRequirements() != null) {
      standardSync.withResourceRequirements(apiPojoConverters.resourceRequirementsToInternal(connectionCreate.getResourceRequirements()));
    }

    // TODO Undesirable behavior: sending a null configured catalog should not be valid?
    if (connectionCreate.getSyncCatalog() != null) {
      validateCatalogDoesntContainDuplicateStreamNames(connectionCreate.getSyncCatalog());
      validateCatalogSize(connectionCreate.getSyncCatalog(), workspaceId, "create");

      assignIdsToIncomingMappers(connectionCreate.getSyncCatalog());
      final ConfiguredAirbyteCatalog configuredCatalog =
          catalogConverter.toConfiguredInternal(connectionCreate.getSyncCatalog());
      validateConfiguredMappers(configuredCatalog);

      final ConfiguredAirbyteCatalog configuredCatalogNoSecrets = mapperSecretHelper.createAndReplaceMapperSecrets(workspaceId, configuredCatalog);
      standardSync.withCatalog(configuredCatalogNoSecrets);
      standardSync.withFieldSelectionData(catalogConverter.getFieldSelectionData(connectionCreate.getSyncCatalog()));
    } else {
      standardSync.withCatalog(new ConfiguredAirbyteCatalog().withStreams(Collections.emptyList()));
      standardSync.withFieldSelectionData(new FieldSelectionData());
    }

    if (connectionCreate.getSchedule() != null && connectionCreate.getScheduleType() != null) {
      throw new JsonValidationException("supply old or new schedule schema but not both");
    }

    if (connectionCreate.getScheduleType() != null) {
      connectionScheduleHelper.populateSyncFromScheduleTypeAndData(standardSync, connectionCreate.getScheduleType(),
          connectionCreate.getScheduleData());
    } else {
      populateSyncFromLegacySchedule(standardSync, connectionCreate);
    }
    if (workspaceId != null && featureFlagClient.boolVariation(CheckWithCatalog.INSTANCE, new Workspace(workspaceId))) {
      // TODO this is the hook for future check with catalog work
      LOGGER.info("Entered into Dark Launch Code for Check with Catalog");
    }
    connectionService.writeStandardSync(standardSync);

    trackNewConnection(standardSync);

    try {
      LOGGER.info("Starting a connection manager workflow");
      eventRunner.createConnectionManagerWorkflow(connectionId);
    } catch (final Exception e) {
      LOGGER.error("Start of the connection manager workflow failed", e);
      // deprecate the newly created connection and also delete the newly created workflow.
      deleteConnection(connectionId);
      throw e;
    }

    return buildConnectionRead(connectionId);
  }

  private Geography getGeographyFromConnectionCreateOrWorkspace(final ConnectionCreate connectionCreate)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {

    if (connectionCreate.getGeography() != null) {
      return apiPojoConverters.toPersistenceGeography(connectionCreate.getGeography());
    }

    // connectionCreate didn't specify a geography, so use the workspace default geography if one exists
    final UUID workspaceId = workspaceHelper.getWorkspaceForSourceId(connectionCreate.getSourceId());
    final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true);

    if (workspace.getDefaultGeography() != null) {
      return workspace.getDefaultGeography();
    }

    // if the workspace doesn't have a default geography, default to 'auto'
    return Geography.AUTO;
  }

  private void populateSyncFromLegacySchedule(final StandardSync standardSync, final ConnectionCreate connectionCreate) {
    if (connectionCreate.getSchedule() != null) {
      final Schedule schedule = new Schedule()
          .withTimeUnit(apiPojoConverters.toPersistenceTimeUnit(connectionCreate.getSchedule().getTimeUnit()))
          .withUnits(connectionCreate.getSchedule().getUnits());
      // Populate the legacy field.
      // TODO(https://github.com/airbytehq/airbyte/issues/11432): remove.
      standardSync
          .withManual(false)
          .withSchedule(schedule);
      // Also write into the new field. This one will be consumed if populated.
      standardSync
          .withScheduleType(ScheduleType.BASIC_SCHEDULE);
      standardSync.withScheduleData(new ScheduleData().withBasicSchedule(
          new BasicSchedule().withTimeUnit(apiPojoConverters.toBasicScheduleTimeUnit(connectionCreate.getSchedule().getTimeUnit()))
              .withUnits(connectionCreate.getSchedule().getUnits())));
    } else {
      standardSync.withManual(true);
      standardSync.withScheduleType(ScheduleType.MANUAL);
    }
  }

  private void trackNewConnection(final StandardSync standardSync) {
    try {
      final UUID workspaceId = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(standardSync.getConnectionId());
      final Builder<String, Object> metadataBuilder = generateMetadata(standardSync);
      trackingClient.track(workspaceId, ScopeType.WORKSPACE, "New Connection - Backend", metadataBuilder.build());
    } catch (final Exception e) {
      LOGGER.error("failed while reporting usage.", e);
    }
  }

  private void trackUpdateConnection(final StandardSync standardSync) {
    try {
      final UUID workspaceId = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(standardSync.getConnectionId());
      final Builder<String, Object> metadataBuilder = generateMetadata(standardSync);
      trackingClient.track(workspaceId, ScopeType.WORKSPACE, "Updated Connection - Backend", metadataBuilder.build());
    } catch (final Exception e) {
      LOGGER.error("failed while reporting usage.", e);
    }
  }

  private Builder<String, Object> generateMetadata(final StandardSync standardSync) {
    final Builder<String, Object> metadata = ImmutableMap.builder();

    final UUID connectionId = standardSync.getConnectionId();
    final StandardSourceDefinition sourceDefinition = sourceService
        .getSourceDefinitionFromConnection(connectionId);
    final StandardDestinationDefinition destinationDefinition = destinationService
        .getDestinationDefinitionFromConnection(connectionId);

    metadata.put("connector_source", sourceDefinition.getName());
    metadata.put("connector_source_definition_id", sourceDefinition.getSourceDefinitionId());
    metadata.put("connector_destination", destinationDefinition.getName());
    metadata.put("connector_destination_definition_id", destinationDefinition.getDestinationDefinitionId());
    metadata.put("connection_id", standardSync.getConnectionId());
    metadata.put("source_id", standardSync.getSourceId());
    metadata.put("destination_id", standardSync.getDestinationId());

    final String frequencyString;
    if (standardSync.getScheduleType() != null) {
      frequencyString = getFrequencyStringFromScheduleType(standardSync.getScheduleType(), standardSync.getScheduleData());
    } else if (standardSync.getManual()) {
      frequencyString = "manual";
    } else {
      final long intervalInMinutes = TimeUnit.SECONDS.toMinutes(ScheduleHelpers.getIntervalInSecond(standardSync.getSchedule()));
      frequencyString = intervalInMinutes + " min";
    }
    boolean fieldSelectionEnabled = false;
    if (standardSync.getFieldSelectionData() != null && standardSync.getFieldSelectionData().getAdditionalProperties() != null) {
      fieldSelectionEnabled = standardSync.getFieldSelectionData().getAdditionalProperties()
          .entrySet().stream().anyMatch(Entry::getValue);
    }
    metadata.put("field_selection_active", fieldSelectionEnabled);
    metadata.put("frequency", frequencyString);
    return metadata;
  }

  public ConnectionRead updateConnection(final ConnectionUpdate connectionPatch, final String updateReason, final Boolean autoUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {

    final UUID connectionId = connectionPatch.getConnectionId();
    final UUID workspaceId = workspaceHelper.getWorkspaceForConnectionId(connectionId);

    LOGGER.debug("Starting updateConnection for connectionId {}, workspaceId {}...", connectionId, workspaceId);
    LOGGER.debug("incoming connectionPatch: {}", connectionPatch);

    final StandardSync sync = connectionService.getStandardSync(connectionId);
    LOGGER.debug("initial StandardSync: {}", sync);

    validateConnectionPatch(workspaceHelper, sync, connectionPatch);

    final ConnectionRead initialConnectionRead = apiPojoConverters.internalToConnectionRead(sync);
    LOGGER.debug("initial ConnectionRead: {}", initialConnectionRead);

    if (connectionPatch.getSyncCatalog() != null
        && featureFlagClient.boolVariation(ResetStreamsStateWhenDisabled.INSTANCE, new Workspace(workspaceId))) {
      final var newCatalogActiveStream = connectionPatch.getSyncCatalog().getStreams().stream().filter(s -> s.getConfig().getSelected())
          .map(s -> new StreamDescriptor().namespace(s.getStream().getNamespace()).name(s.getStream().getName()))
          .toList();
      final var deactivatedStreams = sync.getCatalog().getStreams().stream()
          .map(s -> new StreamDescriptor().name(s.getStream().getName()).namespace(s.getStream().getNamespace()))
          .collect(Collectors.toSet());
      newCatalogActiveStream.forEach(deactivatedStreams::remove);
      LOGGER.debug("Wiping out the state of deactivated streams: [{}]",
          String.join(", ", deactivatedStreams.stream().map(StreamDescriptorUtils::buildFullyQualifiedName).toList()));
      statePersistence.bulkDelete(connectionId,
          deactivatedStreams.stream().map(ApiConverters::toInternal).collect(Collectors.toSet()));
    }
    applyPatchToStandardSync(sync, connectionPatch, workspaceId);

    LOGGER.debug("patched StandardSync before persisting: {}", sync);
    connectionService.writeStandardSync(sync);

    eventRunner.update(connectionId);

    final ConnectionRead updatedRead = buildConnectionRead(connectionId);
    LOGGER.debug("final connectionRead: {}", updatedRead);

    trackUpdateConnection(sync);

    // Log settings change event in connection timeline.
    connectionTimelineEventHelper.logConnectionSettingsChangedEventInConnectionTimeline(connectionId, initialConnectionRead, connectionPatch,
        updateReason, autoUpdate);

    return updatedRead;
  }

  private void validateConnectionPatch(final WorkspaceHelper workspaceHelper, final StandardSync persistedSync, final ConnectionUpdate patch) {
    // sanity check that we're updating the right connection
    Preconditions.checkArgument(persistedSync.getConnectionId().equals(patch.getConnectionId()));

    // make sure all operationIds belong to the same workspace as the connection
    ConnectionHelper.validateWorkspace(
        workspaceHelper, persistedSync.getSourceId(), persistedSync.getDestinationId(), patch.getOperationIds());

    // make sure the incoming schedule update is sensible. Note that schedule details are further
    // validated in ConnectionScheduleHelper, this just
    // sanity checks that fields are populated when they should be.
    Preconditions.checkArgument(
        patch.getSchedule() == null,
        "ConnectionUpdate should only make changes to the schedule by setting scheduleType and scheduleData. 'schedule' is no longer supported.");

    if (patch.getScheduleType() == null) {
      Preconditions.checkArgument(
          patch.getScheduleData() == null,
          "ConnectionUpdate should not include any scheduleData without also specifying a valid scheduleType.");
    } else {
      switch (patch.getScheduleType()) {
        case MANUAL -> Preconditions.checkArgument(
            patch.getScheduleData() == null,
            "ConnectionUpdate should not include any scheduleData when setting the Connection scheduleType to MANUAL.");
        case BASIC -> Preconditions.checkArgument(
            patch.getScheduleData() != null,
            "ConnectionUpdate should include scheduleData when setting the Connection scheduleType to BASIC.");
        case CRON -> Preconditions.checkArgument(
            patch.getScheduleData() != null,
            "ConnectionUpdate should include scheduleData when setting the Connection scheduleType to CRON.");
        // shouldn't be possible to reach this case
        default -> throw new RuntimeException("Unrecognized scheduleType!");
      }
    }
  }

  @Trace
  public ConnectionReadList listConnectionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    ApmTraceUtils.addTagsToTrace(Map.of(MetricTags.WORKSPACE_ID, workspaceIdRequestBody.getWorkspaceId().toString()));
    return listConnectionsForWorkspace(workspaceIdRequestBody, false);
  }

  @Trace
  public ConnectionReadList listConnectionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody, final boolean includeDeleted)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<ConnectionRead> connectionReads = Lists.newArrayList();

    for (final StandardSync standardSync : connectionService.listWorkspaceStandardSyncs(workspaceIdRequestBody.getWorkspaceId(), includeDeleted)) {
      connectionReads.add(apiPojoConverters.internalToConnectionRead(standardSync));
    }

    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionReadList listAllConnectionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listConnectionsForWorkspace(workspaceIdRequestBody, true);
  }

  public ConnectionReadList listConnectionsForSource(final UUID sourceId, final boolean includeDeleted) throws IOException {
    final List<ConnectionRead> connectionReads = Lists.newArrayList();
    for (final StandardSync standardSync : connectionService.listConnectionsBySource(sourceId, includeDeleted)) {
      connectionReads.add(apiPojoConverters.internalToConnectionRead(standardSync));
    }
    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionReadList listConnections() throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<ConnectionRead> connectionReads = Lists.newArrayList();

    for (final StandardSync standardSync : connectionService.listStandardSyncs()) {
      if (standardSync.getStatus() == StandardSync.Status.DEPRECATED) {
        continue;
      }
      connectionReads.add(apiPojoConverters.internalToConnectionRead(standardSync));
    }

    return new ConnectionReadList().connections(connectionReads);
  }

  @Trace
  public ConnectionRead getConnection(final UUID connectionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildConnectionRead(connectionId);
  }

  public ConnectionRead getConnectionForJob(final UUID connectionId, final Long jobId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildConnectionRead(connectionId, jobId);
  }

  public CatalogDiff getDiff(final AirbyteCatalog oldCatalog,
                             final AirbyteCatalog newCatalog,
                             final ConfiguredAirbyteCatalog configuredCatalog,
                             final UUID connectionId)
      throws JsonValidationException {

    return new CatalogDiff().transforms(CatalogDiffHelpers.getCatalogDiff(
        CatalogHelpers.configuredCatalogToCatalog(catalogConverter.toProtocolKeepAllStreams(oldCatalog)),
        CatalogHelpers.configuredCatalogToCatalog(catalogConverter.toProtocolKeepAllStreams(newCatalog)), configuredCatalog)
        .stream()
        .map(CatalogDiffConverters::streamTransformToApi)
        .toList());
  }

  public CatalogDiff getDiff(final ConnectionRead connectionRead, final AirbyteCatalog discoveredCatalog)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {

    final var catalogWithSelectedFieldsAnnotated = connectionRead.getSyncCatalog();
    final var configuredCatalog = catalogConverter.toConfiguredInternal(catalogWithSelectedFieldsAnnotated);
    final var rawCatalog = getConnectionAirbyteCatalog(connectionRead.getConnectionId());

    return getDiff(rawCatalog.orElse(catalogWithSelectedFieldsAnnotated), discoveredCatalog, configuredCatalog, connectionRead.getConnectionId());
  }

  /**
   * Returns the list of the streamDescriptor that have their config updated.
   *
   * @param oldCatalog the old catalog
   * @param newCatalog the new catalog
   * @return the list of StreamDescriptor that have their configuration changed
   */
  public Set<StreamDescriptor> getConfigurationDiff(final AirbyteCatalog oldCatalog, final AirbyteCatalog newCatalog) {
    final Map<StreamDescriptor, AirbyteStreamConfiguration> oldStreams = catalogToPerStreamConfiguration(oldCatalog);
    final Map<StreamDescriptor, AirbyteStreamConfiguration> newStreams = catalogToPerStreamConfiguration(newCatalog);

    final Set<StreamDescriptor> streamWithDifferentConf = new HashSet<>();

    newStreams.forEach(((streamDescriptor, airbyteStreamConfiguration) -> {
      final AirbyteStreamConfiguration oldConfig = oldStreams.get(streamDescriptor);

      if (oldConfig != null && haveConfigChange(oldConfig, airbyteStreamConfiguration)) {
        streamWithDifferentConf.add(streamDescriptor);
      }
    }));

    return streamWithDifferentConf;
  }

  private boolean haveConfigChange(final AirbyteStreamConfiguration oldConfig, final AirbyteStreamConfiguration newConfig) {
    final List<String> oldCursors = oldConfig.getCursorField();
    final List<String> newCursors = newConfig.getCursorField();
    final boolean hasCursorChanged = !(oldCursors.equals(newCursors));

    final boolean hasSyncModeChanged = !oldConfig.getSyncMode().equals(newConfig.getSyncMode());

    final boolean hasDestinationSyncModeChanged = !oldConfig.getDestinationSyncMode().equals(newConfig.getDestinationSyncMode());

    final Set<List<String>> convertedOldPrimaryKey = new HashSet<>(oldConfig.getPrimaryKey());
    final Set<List<String>> convertedNewPrimaryKey = new HashSet<>(newConfig.getPrimaryKey());
    final boolean hasPrimaryKeyChanged = !(convertedOldPrimaryKey.equals(convertedNewPrimaryKey));

    return hasCursorChanged || hasSyncModeChanged || hasDestinationSyncModeChanged || hasPrimaryKeyChanged;
  }

  private Map<StreamDescriptor, AirbyteStreamConfiguration> catalogToPerStreamConfiguration(final AirbyteCatalog catalog) {
    return catalog.getStreams().stream().collect(Collectors.toMap(stream -> new StreamDescriptor()
        .name(stream.getStream().getName())
        .namespace(stream.getStream().getNamespace()),
        AirbyteStreamAndConfiguration::getConfig));
  }

  @Trace
  public Optional<AirbyteCatalog> getConnectionAirbyteCatalog(final UUID connectionId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync connection = connectionService.getStandardSync(connectionId);
    if (connection.getSourceCatalogId() == null) {
      return Optional.empty();
    }
    final ActorCatalog catalog = catalogService.getActorCatalogById(connection.getSourceCatalogId());
    final StandardSourceDefinition sourceDefinition = sourceService.getSourceDefinitionFromSource(connection.getSourceId());
    final SourceConnection sourceConnection = sourceService.getSourceConnection(connection.getSourceId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceConnection.getWorkspaceId(), connection.getSourceId());
    final io.airbyte.protocol.models.AirbyteCatalog jsonCatalog = Jsons.object(catalog.getCatalog(), io.airbyte.protocol.models.AirbyteCatalog.class);
    final StandardDestinationDefinition destination = destinationService.getDestinationDefinitionFromConnection(connectionId);
    // Note: we're using the workspace from the source to save an extra db request.
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destination, sourceConnection.getWorkspaceId());
    final List<DestinationSyncMode> supportedDestinationSyncModes =
        Enums.convertListTo(destinationVersion.getSpec().getSupportedDestinationSyncModes(), DestinationSyncMode.class);
    final var convertedCatalog = Optional.of(catalogConverter.toApi(jsonCatalog, sourceVersion));
    if (convertedCatalog.isPresent()) {
      convertedCatalog.get().getStreams().forEach((streamAndConfiguration) -> {
        catalogConverter.ensureCompatibleDestinationSyncMode(streamAndConfiguration, supportedDestinationSyncModes);
      });
    }
    return convertedCatalog;
  }

  public void deleteConnection(final UUID connectionId) throws JsonValidationException, ConfigNotFoundException, IOException {
    connectionHelper.deleteConnection(connectionId);
    eventRunner.forceDeleteConnection(connectionId);
    streamRefreshesHandler.deleteRefreshesForConnection(connectionId);
  }

  public ConnectionRead buildConnectionRead(final UUID connectionId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSync standardSync = connectionService.getStandardSync(connectionId);

    final ConfiguredAirbyteCatalog maskedCatalog = mapperSecretHelper.maskMapperSecrets(standardSync.getCatalog());
    standardSync.setCatalog(maskedCatalog);

    return apiPojoConverters.internalToConnectionRead(standardSync);
  }

  private ConnectionRead buildConnectionRead(final UUID connectionId, final Long jobId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSync standardSync = connectionService.getStandardSync(connectionId);
    final Job job = jobPersistence.getJob(jobId);
    final List<Generation> generations = streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId);
    final Optional<ConfiguredAirbyteCatalog> catalogWithGeneration;
    if (job.getConfigType() == ConfigType.SYNC) {
      catalogWithGeneration = Optional.of(catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
          standardSync.getCatalog(),
          jobId,
          List.of(),
          generations));
    } else if (job.getConfigType() == ConfigType.REFRESH) {
      catalogWithGeneration = Optional.of(catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
          standardSync.getCatalog(),
          jobId,
          job.getConfig().getRefresh().getStreamsToRefresh(),
          generations));
    } else if (job.getConfigType() == ConfigType.RESET_CONNECTION || job.getConfigType() == ConfigType.CLEAR) {
      catalogWithGeneration = Optional.of(catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformationForClear(
          standardSync.getCatalog(),
          jobId,
          Set.copyOf(job.getConfig().getResetConnection().getResetSourceConfiguration().getStreamsToReset()),
          generations));
    } else {
      catalogWithGeneration = Optional.empty();
    }

    catalogWithGeneration.ifPresent(standardSync::setCatalog);
    return apiPojoConverters.internalToConnectionRead(standardSync);
  }

  public ConnectionReadList listConnectionsForWorkspaces(final ListConnectionsForWorkspacesRequestBody listConnectionsForWorkspacesRequestBody)
      throws IOException {

    final List<ConnectionRead> connectionReads = Lists.newArrayList();

    final Map<UUID, List<StandardSync>> workspaceIdToStandardSyncsMap = connectionService.listWorkspaceStandardSyncsPaginated(
        listConnectionsForWorkspacesRequestBody.getWorkspaceIds(),
        listConnectionsForWorkspacesRequestBody.getIncludeDeleted(),
        PaginationHelper.pageSize(listConnectionsForWorkspacesRequestBody.getPagination()),
        PaginationHelper.rowOffset(listConnectionsForWorkspacesRequestBody.getPagination()));

    for (final Entry<UUID, List<StandardSync>> entry : workspaceIdToStandardSyncsMap.entrySet()) {
      final UUID workspaceId = entry.getKey();
      for (final StandardSync standardSync : entry.getValue()) {
        final ConnectionRead connectionRead = apiPojoConverters.internalToConnectionRead(standardSync);
        connectionRead.setWorkspaceId(workspaceId);
        connectionReads.add(connectionRead);
      }
    }
    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionReadList listConnectionsForActorDefinition(final ActorDefinitionRequestBody actorDefinitionRequestBody)
      throws IOException {

    final List<ConnectionRead> connectionReads = new ArrayList<>();

    final List<StandardSync> standardSyncs = connectionService.listConnectionsByActorDefinitionIdAndType(
        actorDefinitionRequestBody.getActorDefinitionId(),
        actorDefinitionRequestBody.getActorType().toString(),
        false, true);

    for (final StandardSync standardSync : standardSyncs) {
      final ConnectionRead connectionRead = apiPojoConverters.internalToConnectionRead(standardSync);
      connectionReads.add(connectionRead);
    }
    return new ConnectionReadList().connections(connectionReads);
  }

  public FailureReason mapFailureReason(final io.airbyte.config.FailureReason data) {
    final FailureReason failureReason = new FailureReason();
    failureReason.setFailureOrigin(Enums.convertTo(data.getFailureOrigin(), FailureOrigin.class));
    failureReason.setFailureType(Enums.convertTo(data.getFailureType(), FailureType.class));
    failureReason.setExternalMessage(data.getExternalMessage());
    failureReason.setInternalMessage(data.getInternalMessage());
    failureReason.setStacktrace(data.getStacktrace());
    failureReason.setRetryable(data.getRetryable());
    failureReason.setTimestamp(data.getTimestamp());
    return failureReason;
  }

  @Trace
  public List<ConnectionStatusRead> getConnectionStatuses(
                                                          final ConnectionStatusesRequestBody connectionStatusesRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    ApmTraceUtils.addTagsToTrace(Map.of(MetricTags.CONNECTION_IDS, connectionStatusesRequestBody.getConnectionIds().toString()));
    final List<UUID> connectionIds = connectionStatusesRequestBody.getConnectionIds();
    final List<ConnectionStatusRead> result = new ArrayList<>();
    for (final UUID connectionId : connectionIds) {
      final List<Job> jobs = jobPersistence.listJobsLight(REPLICATION_TYPES,
          connectionId.toString(),
          maxJobLookback);
      final Optional<Job> activeJob = jobs.stream().findFirst().filter(job -> JobStatus.NON_TERMINAL_STATUSES.contains(job.getStatus()));
      final boolean isRunning = activeJob.isPresent();

      final Optional<Job> lastSucceededOrFailedJob =
          jobs.stream().filter(job -> JobStatus.TERMINAL_STATUSES.contains(job.getStatus()) && job.getStatus() != JobStatus.CANCELLED).findFirst();
      final Optional<JobStatus> lastSyncStatus = lastSucceededOrFailedJob.map(Job::getStatus);
      final io.airbyte.api.model.generated.JobStatus lastSyncJobStatus = Enums.convertTo(lastSyncStatus.orElse(null),
          io.airbyte.api.model.generated.JobStatus.class);
      final boolean lastJobWasCancelled = !jobs.isEmpty() && jobs.getFirst().getStatus() == JobStatus.CANCELLED;
      final boolean lastJobWasResetOrClear = !jobs.isEmpty()
          && (jobs.getFirst().getConfigType() == ConfigType.RESET_CONNECTION || jobs.getFirst().getConfigType() == ConfigType.CLEAR);

      final Optional<Job> lastSuccessfulJob = jobs.stream().filter(job -> job.getStatus() == JobStatus.SUCCEEDED).findFirst();
      final Optional<Long> lastSuccessTimestamp = lastSuccessfulJob.map(Job::getUpdatedAtInSecond);

      final ConnectionRead connectionRead = buildConnectionRead(connectionId);
      final boolean hasBreakingSchemaChange = connectionRead.getBreakingChange() != null && connectionRead.getBreakingChange();

      final ConnectionStatusRead connectionStatus = new ConnectionStatusRead()
          .connectionId(connectionId)
          .activeJob(activeJob.map(JobConverter::getJobRead).orElse(null))
          .lastSuccessfulSync(lastSuccessTimestamp.orElse(null))
          .scheduleData(connectionRead.getScheduleData());
      if (lastSucceededOrFailedJob.isPresent()) {
        connectionStatus.lastSyncJobId(lastSucceededOrFailedJob.get().getId());
        final Optional<Attempt> lastAttempt = lastSucceededOrFailedJob.get().getLastAttempt();
        lastAttempt.ifPresent(attempt -> connectionStatus.lastSyncAttemptNumber(attempt.getAttemptNumber()));
      }
      final Optional<io.airbyte.api.model.generated.FailureReason> failureReason = lastSucceededOrFailedJob.flatMap(Job::getLastFailedAttempt)
          .flatMap(Attempt::getFailureSummary)
          .flatMap(s -> s.getFailures().stream().findFirst())
          .map(this::mapFailureReason);
      if (failureReason.isPresent() && lastSucceededOrFailedJob.get().getStatus() == JobStatus.FAILED) {
        connectionStatus.setFailureReason(failureReason.get());
      }

      boolean hasConfigError = false;
      if (lastSucceededOrFailedJob.isPresent() && lastSucceededOrFailedJob.get().getStatus() == JobStatus.FAILED) {
        final Optional<List<io.airbyte.api.model.generated.FailureReason>> failureReasons =
            lastSucceededOrFailedJob.flatMap(Job::getLastFailedAttempt)
                .flatMap(Attempt::getFailureSummary)
                .map(s -> s.getFailures().stream()
                    .map(this::mapFailureReason)
                    .collect(Collectors.toList()));

        if (failureReasons.isPresent() && !failureReasons.get().isEmpty()) {
          connectionStatus.setFailureReason(failureReasons.get().getFirst());

          hasConfigError = failureReasons.get().stream().anyMatch(reason -> reason.getFailureType() == FailureType.CONFIG_ERROR);
        }
      }

      final Optional<JobRead> latestSyncJob = jobPersistence.getLastSyncJob(connectionId).map(JobConverter::getJobRead);
      latestSyncJob.ifPresent(job -> {
        connectionStatus.setLastSyncJobCreatedAt(job.getCreatedAt());
      });

      if (isRunning) {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.RUNNING);
      } else if (hasBreakingSchemaChange || hasConfigError) {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.FAILED);
      } else if (connectionRead.getStatus() != ConnectionStatus.ACTIVE) {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.PAUSED);
      } else if (lastJobWasCancelled) {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.INCOMPLETE);
      } else if (lastSyncJobStatus == null || lastJobWasResetOrClear) {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.PENDING);
      } else if (lastSyncJobStatus == io.airbyte.api.model.generated.JobStatus.FAILED) {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.INCOMPLETE);
      } else {
        connectionStatus.setConnectionSyncStatus(ConnectionSyncStatus.SYNCED);
      }

      result.add(connectionStatus);
    }

    return result;
  }

  private List<ConnectionEvent.Type> convertConnectionType(final List<ConnectionEventType> eventTypes) {
    if (eventTypes == null) {
      return null;
    }
    return eventTypes.stream().map(eventType -> ConnectionEvent.Type.valueOf(eventType.name())).collect(Collectors.toList());
  }

  private io.airbyte.api.model.generated.ConnectionEvent convertConnectionEvent(final ConnectionTimelineEvent event) {
    final io.airbyte.api.model.generated.ConnectionEvent connectionEvent = new io.airbyte.api.model.generated.ConnectionEvent();
    connectionEvent.id(event.getId());
    connectionEvent.eventType(ConnectionEventType.fromString(event.getEventType()));
    connectionEvent.createdAt(event.getCreatedAt().toEpochSecond());
    connectionEvent.connectionId(event.getConnectionId());
    connectionEvent.summary(Jsons.deserialize(event.getSummary()));
    if (event.getUserId() != null) {
      connectionEvent.user(connectionTimelineEventHelper.getUserReadInConnectionEvent(event.getUserId(), event.getConnectionId()));
    }
    return connectionEvent;
  }

  private ConnectionEventList convertConnectionEventList(final List<ConnectionTimelineEvent> events) {
    final List<io.airbyte.api.model.generated.ConnectionEvent> eventsRead =
        events.stream().map(this::convertConnectionEvent).collect(Collectors.toList());
    return new ConnectionEventList().events(eventsRead);
  }

  public ConnectionEventList listConnectionEvents(final ConnectionEventsRequestBody connectionEventsRequestBody) {
    // 1. set page size and offset
    final int pageSize = (connectionEventsRequestBody.getPagination() != null && connectionEventsRequestBody.getPagination().getPageSize() != null)
        ? connectionEventsRequestBody.getPagination().getPageSize()
        : DEFAULT_PAGE_SIZE;
    final int rowOffset = (connectionEventsRequestBody.getPagination() != null && connectionEventsRequestBody.getPagination().getRowOffset() != null)
        ? connectionEventsRequestBody.getPagination().getRowOffset()
        : DEFAULT_ROW_OFFSET;
    // 2. get list of events
    final List<ConnectionTimelineEvent> events = connectionTimelineEventService.listEvents(
        connectionEventsRequestBody.getConnectionId(),
        convertConnectionType(connectionEventsRequestBody.getEventTypes()),
        connectionEventsRequestBody.getCreatedAtStart(),
        connectionEventsRequestBody.getCreatedAtEnd(),
        pageSize,
        rowOffset);
    return convertConnectionEventList(events);
  }

  public ConnectionEventWithDetails getConnectionEvent(final ConnectionEventIdRequestBody connectionEventIdRequestBody) {
    final ConnectionTimelineEvent eventData = connectionTimelineEventService.getEvent(connectionEventIdRequestBody.getConnectionEventId());
    return hydrateConnectionEvent(eventData);
  }

  private ConnectionEventWithDetails hydrateConnectionEvent(final ConnectionTimelineEvent event) {
    final ConnectionEventWithDetails connectionEventWithDetails = new ConnectionEventWithDetails();
    connectionEventWithDetails.id(event.getId());
    connectionEventWithDetails.connectionId(event.getConnectionId());
    // enforce event type consistency
    connectionEventWithDetails.eventType(ConnectionEventType.fromString(event.getEventType()));
    connectionEventWithDetails.summary(Jsons.deserialize(event.getSummary()));
    // TODO(@keyi): implement details generation for different types of events.
    connectionEventWithDetails.details(null);
    connectionEventWithDetails.createdAt(event.getCreatedAt().toEpochSecond());
    if (event.getUserId() != null) {
      connectionEventWithDetails.user(connectionTimelineEventHelper.getUserReadInConnectionEvent(event.getUserId(), event.getConnectionId()));
    }
    return connectionEventWithDetails;
  }

  /**
   * Backfill jobs to timeline events. Supported event types:
   * <p>
   * 1. SYNC_CANCELLED 2. SYNC_SUCCEEDED 3. SYNC_FAILED 4. SYNC_INCOMPLETE 5. REFRESH_SUCCEEDED 6.
   * REFRESH_FAILED 7. REFRESH_INCOMPLETE 8. REFRESH_CANCELLED 9. CLEAR_SUCCEEDED 10. CLEAR_FAILED 11.
   * CLEAR_INCOMPLETE 12. CLEAR_CANCELLED
   * </p>
   * Notes:
   * <p>
   * 1. Manually started events (X_STARTED) will NOT be backfilled because we don't have that
   * information from Jobs table.
   * </p>
   * <p>
   * 2. Manually cancelled events (X_CANCELLED) will be backfilled, but the associated user ID will be
   * missing as it is not trackable from Jobs table.
   * </p>
   * <p>
   * 3. RESET_CONNECTION is just the old enum name of CLEAR.
   * </p>
   */
  public void backfillConnectionEvents(final ConnectionEventsBackfillRequestBody connectionEventsBackfillRequestBody) throws IOException {
    final UUID connectionId = connectionEventsBackfillRequestBody.getConnectionId();
    final OffsetDateTime startTime = connectionEventsBackfillRequestBody.getCreatedAtStart();
    final OffsetDateTime endTime = connectionEventsBackfillRequestBody.getCreatedAtEnd();
    LOGGER.info("Backfilled events from {} to {} for connection {}", startTime, endTime, connectionId);
    // 1. list all jobs within a given time window
    final List<Job> allJobsToMigrate = jobPersistence.listJobsForConvertingToEvents(
        Set.of(ConfigType.SYNC, ConfigType.REFRESH, ConfigType.RESET_CONNECTION),
        Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.INCOMPLETE, JobStatus.CANCELLED),
        startTime,
        endTime);
    LOGGER.info("Verify listing jobs. {} jobs found.", allJobsToMigrate.size());
    // 2. For each job, log a timeline event
    allJobsToMigrate.forEach(job -> {
      final JobWithAttemptsRead jobRead = JobConverter.getJobWithAttemptsRead(job);
      // Construct a timeline event
      final FinalStatusEvent event;
      if (job.getStatus() == JobStatus.FAILED || job.getStatus() == JobStatus.INCOMPLETE) {
        // We need to log a failed event with job stats and the first failure reason.
        event = new FailedEvent(
            job.getId(),
            job.getCreatedAtInSecond(),
            job.getUpdatedAtInSecond(),
            jobRead.getAttempts().stream()
                .mapToLong(attempt -> attempt.getBytesSynced() != null ? attempt.getBytesSynced() : 0)
                .sum(),
            jobRead.getAttempts().stream()
                .mapToLong(attempt -> attempt.getRecordsSynced() != null ? attempt.getRecordsSynced() : 0)
                .sum(),
            job.getAttemptsCount(),
            job.getConfigType().name(),
            job.getStatus().name(),
            JobConverter.getStreamsAssociatedWithJob(job),
            job.getLastAttempt()
                .flatMap(Attempt::getFailureSummary)
                .flatMap(summary -> summary.getFailures().stream().findFirst()));
      } else { // SUCCEEDED and CANCELLED
        // We need to log a timeline event with job stats.
        event = new FinalStatusEvent(
            job.getId(),
            job.getCreatedAtInSecond(),
            job.getUpdatedAtInSecond(),
            jobRead.getAttempts().stream()
                .mapToLong(attempt -> attempt.getBytesSynced() != null ? attempt.getBytesSynced() : 0)
                .sum(),
            jobRead.getAttempts().stream()
                .mapToLong(attempt -> attempt.getRecordsSynced() != null ? attempt.getRecordsSynced() : 0)
                .sum(),
            job.getAttemptsCount(),
            job.getConfigType().name(),
            job.getStatus().name(),
            JobConverter.getStreamsAssociatedWithJob(job));
      }
      // Save an event
      connectionTimelineEventService.writeEventWithTimestamp(
          UUID.fromString(job.getScope()), event, null, Instant.ofEpochSecond(job.getUpdatedAtInSecond()).atOffset(ZoneOffset.UTC));
    });
  }

  /**
   * Returns data history for the given connection for requested number of jobs.
   *
   * @param connectionDataHistoryRequestBody connection Id and number of jobs
   * @return list of JobSyncResultRead
   */
  public List<JobSyncResultRead> getConnectionDataHistory(final ConnectionDataHistoryRequestBody connectionDataHistoryRequestBody) {

    final List<Job> jobs;
    try {
      jobs = jobPersistence.listJobs(
          Job.SYNC_REPLICATION_TYPES,
          Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED),
          connectionDataHistoryRequestBody.getConnectionId().toString(),
          connectionDataHistoryRequestBody.getNumberOfJobs());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final Map<Long, JobWithAttemptsRead> jobIdToJobRead = StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(jobs, jobPersistence);

    final List<JobSyncResultRead> result = new ArrayList<>();
    jobs.forEach((job) -> {
      final Long jobId = job.getId();
      final JobRead jobRead = jobIdToJobRead.get(jobId).getJob();
      final JobAggregatedStats aggregatedStats = jobRead.getAggregatedStats();
      final JobSyncResultRead jobResult = new JobSyncResultRead()
          .jobId(jobId)
          .configType(jobRead.getConfigType())
          .jobCreatedAt(jobRead.getCreatedAt())
          .jobUpdatedAt(jobRead.getUpdatedAt())
          .bytesEmitted(aggregatedStats.getBytesEmitted())
          .bytesCommitted(aggregatedStats.getBytesCommitted())
          .recordsEmitted(aggregatedStats.getRecordsEmitted())
          .recordsCommitted(aggregatedStats.getRecordsCommitted());
      result.add(jobResult);
    });

    // Sort the results by date
    return result.stream()
        .sorted(Comparator.comparing(JobSyncResultRead::getJobUpdatedAt))
        .collect(Collectors.toList());
  }

  /**
   * Returns records synced per stream per day for the given connection for the last 30 days in the
   * given timezone.
   *
   * @param connectionStreamHistoryRequestBody the connection id and timezone string
   * @return list of ConnectionStreamHistoryReadItems (timestamp, stream namespace, stream name,
   *         records synced)
   */

  public List<ConnectionStreamHistoryReadItem> getConnectionStreamHistory(
                                                                          final ConnectionStreamHistoryRequestBody connectionStreamHistoryRequestBody)
      throws IOException {

    // Start time in designated timezone
    final ZonedDateTime endTimeInUserTimeZone = Instant.now().atZone(ZoneId.of(connectionStreamHistoryRequestBody.getTimezone()));
    final ZonedDateTime startTimeInUserTimeZone = endTimeInUserTimeZone.minusDays(30);
    // Convert start time to UTC (since that's what the database uses)
    final Instant startTimeInUTC = startTimeInUserTimeZone.toInstant();

    final List<AttemptWithJobInfo> attempts = jobPersistence.listAttemptsForConnectionAfterTimestamp(
        connectionStreamHistoryRequestBody.getConnectionId(),
        ConfigType.SYNC,
        startTimeInUTC);

    final NavigableMap<LocalDate, Map<List<String>, Long>> connectionStreamHistoryReadItemsByDate = new TreeMap<>();
    final ZoneId userTimeZone = ZoneId.of(connectionStreamHistoryRequestBody.getTimezone());

    final LocalDate startDate = startTimeInUserTimeZone.toLocalDate();
    final LocalDate endDate = endTimeInUserTimeZone.toLocalDate();
    for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
      connectionStreamHistoryReadItemsByDate.put(date, new HashMap<>());
    }

    for (final AttemptWithJobInfo attempt : attempts) {
      final Optional<Long> endedAtOptional = attempt.getAttempt().getEndedAtInSecond();

      if (endedAtOptional.isPresent()) {
        // Convert the endedAt timestamp from the database to the designated timezone
        final Instant attemptEndedAt = Instant.ofEpochSecond(endedAtOptional.get());
        final LocalDate attemptDateInUserTimeZone = attemptEndedAt.atZone(ZoneId.of(connectionStreamHistoryRequestBody.getTimezone()))
            .toLocalDate();

        // Merge it with the records synced from the attempt
        final Optional<JobOutput> attemptOutput = attempt.getAttempt().getOutput();
        if (attemptOutput.isPresent()) {
          final List<StreamSyncStats> streamSyncStats = attemptOutput.get().getSync().getStandardSyncSummary().getStreamStats();
          for (final StreamSyncStats streamSyncStat : streamSyncStats) {
            final String streamName = streamSyncStat.getStreamName();
            final String streamNamespace = streamSyncStat.getStreamNamespace();
            final long recordsCommitted = streamSyncStat.getStats().getRecordsCommitted();

            // Update the records loaded for the corresponding stream for that day
            final Map<List<String>, Long> existingItem = connectionStreamHistoryReadItemsByDate.get(attemptDateInUserTimeZone);
            final List<String> key = List.of(streamNamespace, streamName);
            if (existingItem.containsKey(key)) {
              existingItem.put(key, existingItem.get(key) + recordsCommitted);
            } else {
              existingItem.put(key, recordsCommitted);
            }
          }
        }
      }
    }

    final List<ConnectionStreamHistoryReadItem> result = new ArrayList<>();
    for (final Entry<LocalDate, Map<List<String>, Long>> entry : connectionStreamHistoryReadItemsByDate.entrySet()) {
      final LocalDate date = entry.getKey();
      final Map<List<String>, Long> streamRecordsByStream = entry.getValue();

      streamRecordsByStream.entrySet().stream()
          .sorted(Comparator.comparing((Entry<List<String>, Long> e) -> e.getKey().get(0))
              .thenComparing(e -> e.getKey().get(1)))
          .forEach(streamRecords -> {
            final List<String> streamNamespaceAndName = streamRecords.getKey();
            final Long recordsCommitted = streamRecords.getValue();

            result.add(new ConnectionStreamHistoryReadItem()
                .timestamp(Math.toIntExact(date.atStartOfDay(userTimeZone).toEpochSecond()))
                .streamNamespace(streamNamespaceAndName.get(0))
                .streamName(streamNamespaceAndName.get(1))
                .recordsCommitted(recordsCommitted));
          });
    }
    return result;
  }

  public ConnectionAutoPropagateResult applySchemaChange(final ConnectionAutoPropagateSchemaChange request)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    return applySchemaChange(request.getConnectionId(), request.getWorkspaceId(), request.getCatalogId(), request.getCatalog(), true);
  }

  public ConnectionAutoPropagateResult applySchemaChange(
                                                         final UUID connectionId,
                                                         final UUID workspaceId,
                                                         final UUID catalogId,
                                                         final AirbyteCatalog catalog,
                                                         final Boolean autoApply)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {

    LOGGER.info("Applying schema change for connection '{}' only", connectionId);
    final ConnectionRead connection = buildConnectionRead(connectionId);
    final Optional<io.airbyte.api.model.generated.AirbyteCatalog> catalogUsedToMakeConfiguredCatalog =
        getConnectionAirbyteCatalog(connectionId);
    final io.airbyte.api.model.generated.AirbyteCatalog currentCatalog = connection.getSyncCatalog();
    final CatalogDiff diffToApply = getDiff(
        catalogUsedToMakeConfiguredCatalog.orElse(currentCatalog),
        catalog,
        catalogConverter.toConfiguredInternal(currentCatalog),
        connectionId);
    final ConnectionUpdate updateObject =
        new ConnectionUpdate().connectionId(connection.getConnectionId());
    final UUID destinationDefinitionId =
        destinationService.getDestinationDefinitionFromConnection(connection.getConnectionId()).getDestinationDefinitionId();
    final var supportedDestinationSyncModes =
        connectorSpecHandler.getDestinationSpecification(new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId)
            .workspaceId(workspaceId)).getSupportedDestinationSyncModes();
    final var workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false);
    final var source = sourceService.getSourceConnection(connection.getSourceId());
    final CatalogDiff appliedDiff;
    if (applySchemaChangeHelper.shouldAutoPropagate(diffToApply, connection)) {
      // NOTE: appliedDiff is the part of the diff that were actually applied.
      appliedDiff = applySchemaChangeInternal(updateObject.getConnectionId(),
          workspaceId,
          updateObject,
          currentCatalog,
          catalog,
          diffToApply.getTransforms(),
          catalogId,
          connection.getNonBreakingChangesPreference(), supportedDestinationSyncModes);
      updateConnection(updateObject, ConnectionAutoUpdatedReason.SCHEMA_CHANGE_AUTO_PROPAGATE.name(), autoApply);
      LOGGER.info("Propagating changes for connectionId: '{}', new catalogId '{}'",
          connection.getConnectionId(), catalogId);
      connectionTimelineEventHelper.logSchemaChangeAutoPropagationEventInConnectionTimeline(connectionId, appliedDiff);
      LOGGER.info("Sending notification of schema auto propagation for connectionId: '{}'", connection.getConnectionId());
      notificationHelper.notifySchemaPropagated(
          workspace.getNotificationSettings(),
          appliedDiff,
          workspace,
          connection,
          source,
          workspace.getEmail());
    } else {
      appliedDiff = null;
      // Send notification to the user if schema change needs to be manually applied.
      if (applySchemaChangeHelper.shouldManuallyApply(diffToApply, connection)) {
        LOGGER.info("Sending notification of manually applying schema change for connectionId: '{}'", connection.getConnectionId());
        notificationHelper.notifySchemaDiffToApply(
            workspace.getNotificationSettings(),
            diffToApply,
            workspace,
            connection,
            source,
            workspace.getEmail());
      }
    }
    return new ConnectionAutoPropagateResult().propagatedDiff(appliedDiff);
  }

  private CatalogDiff applySchemaChangeInternal(final UUID connectionId,
                                                final UUID workspaceId,
                                                final ConnectionUpdate updateObject,
                                                final io.airbyte.api.model.generated.AirbyteCatalog currentSyncCatalog,
                                                final io.airbyte.api.model.generated.AirbyteCatalog newCatalog,
                                                final List<StreamTransform> transformations,
                                                final UUID sourceCatalogId,
                                                final NonBreakingChangesPreference nonBreakingChangesPreference,
                                                final List<DestinationSyncMode> supportedDestinationSyncModes) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.SCHEMA_CHANGE_AUTO_PROPAGATED, 1,
        new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    final ApplySchemaChangeHelper.UpdateSchemaResult propagateResult = applySchemaChangeHelper.getUpdatedSchema(
        currentSyncCatalog,
        newCatalog,
        transformations,
        nonBreakingChangesPreference,
        supportedDestinationSyncModes);
    updateObject.setSyncCatalog(propagateResult.catalog());
    updateObject.setSourceCatalogId(sourceCatalogId);
    trackSchemaChange(workspaceId, connectionId, propagateResult);
    return propagateResult.appliedDiff();
  }

  private void validateCatalogSize(final AirbyteCatalog catalog, final UUID workspaceId, final String operationName) {
    final var validationContext = new Workspace(workspaceId);
    final var validationError = catalogValidator.fieldCount(catalog, validationContext);
    if (validationError != null) {
      MetricClientFactory.getMetricClient().count(
          OssMetricsRegistry.CATALOG_SIZE_VALIDATION_ERROR,
          1,
          new MetricAttribute(MetricTags.CRUD_OPERATION, operationName),
          new MetricAttribute(MetricTags.WORKSPACE_ID, workspaceId.toString()));

      throw new BadRequestException(validationError.getMessage());
    }
  }

  public void trackSchemaChange(final UUID workspaceId, final UUID connectionId, final UpdateSchemaResult propagateResult) {
    try {
      final String changeEventTimeline = Instant.now().toString();
      for (final StreamTransform streamTransform : propagateResult.appliedDiff().getTransforms()) {
        final Map<String, Object> payload = new HashMap<>();
        payload.put("workspace_id", workspaceId);
        payload.put("connection_id", connectionId);
        payload.put("schema_change_event_date", changeEventTimeline);
        payload.put("stream_change_type", streamTransform.getTransformType().toString());
        final StreamDescriptor streamDescriptor = streamTransform.getStreamDescriptor();
        if (streamDescriptor.getNamespace() != null) {
          payload.put("stream_namespace", streamDescriptor.getNamespace());
        }
        payload.put("stream_name", streamDescriptor.getName());
        if (streamTransform.getTransformType() == TransformTypeEnum.UPDATE_STREAM) {
          payload.put("stream_field_changes", Jsons.serialize(streamTransform.getUpdateStream()));
        }
        trackingClient.track(workspaceId, ScopeType.WORKSPACE, "Schema Changes", payload);
      }
    } catch (final Exception e) {
      LOGGER.error("Error while sending tracking event for schema change", e);
    }
  }

  @Trace
  public List<ConnectionLastJobPerStreamReadItem> getConnectionLastJobPerStream(final ConnectionLastJobPerStreamRequestBody req) {
    ApmTraceUtils.addTagsToTrace(Map.of(MetricTags.CONNECTION_ID, req.getConnectionId().toString()));

    // determine the latest job ID with stats for each stream by calling the streamStatsService
    final Map<io.airbyte.config.StreamDescriptor, Long> streamToLastJobIdWithStats =
        streamStatusesService.getLastJobIdWithStatsByStream(req.getConnectionId());

    // retrieve the full job information for each of those latest jobs
    final List<Job> jobs;
    try {
      jobs = jobPersistence.listJobsLight(new HashSet<>(streamToLastJobIdWithStats.values()));
    } catch (final IOException e) {
      throw new UnexpectedProblem("Failed to retrieve the latest job per stream", new ProblemMessageData().message(e.getMessage()));
    }

    // hydrate those jobs with their aggregated stats
    final Map<Long, JobWithAttemptsRead> jobIdToJobRead = StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(jobs, jobPersistence);

    // build a map of stream descriptor to job read
    final Map<io.airbyte.config.StreamDescriptor, JobWithAttemptsRead> streamToJobRead = streamToLastJobIdWithStats.entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> jobIdToJobRead.get(entry.getValue())));

    // memoize the process of building a stat-by-stream map for each job
    final Map<Long, Map<io.airbyte.config.StreamDescriptor, StreamStats>> memo = new HashMap<>();

    // convert the hydrated jobs to the response format
    return streamToJobRead.entrySet().stream()
        .map(entry -> buildLastJobPerStreamReadItem(entry.getKey(), entry.getValue().getJob(), memo))
        .collect(Collectors.toList());
  }

  /**
   * Does all secondary steps from a source discover for a connection. Currently, it calculates the
   * diff, conditionally disables and auto-propagates schema changes.
   */
  public PostprocessDiscoveredCatalogResult postprocessDiscoveredCatalog(final UUID connectionId, final UUID discoveredCatalogId)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final var connection = connectionService.getStandardSync(connectionId);
    final var mostRecentCatalog = catalogService.getMostRecentSourceActorCatalog(connection.getSourceId());
    final var mostRecentCatalogId = mostRecentCatalog.map(ActorCatalogWithUpdatedAt::getId).orElse(discoveredCatalogId);
    final var read = diffCatalogAndConditionallyDisable(connectionId, mostRecentCatalogId);

    final var autoPropResult =
        applySchemaChange(connectionId, workspaceHelper.getWorkspaceForConnectionId(connectionId), mostRecentCatalogId, read.getCatalog(), true);
    final var diff = autoPropResult.getPropagatedDiff();

    return new PostprocessDiscoveredCatalogResult().appliedDiff(diff);
  }

  /**
   *
   * Disable the connection if: 1. there are schema breaking changes 2. there are non-breaking schema
   * changes but the connection is configured to disable for any schema changes
   *
   */
  public ConnectionRead disableConnectionIfNeeded(final ConnectionRead connectionRead,
                                                  final boolean containsBreakingChange,
                                                  final CatalogDiff diff)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID connectionId = connectionRead.getConnectionId();
    // Monitor the schema change detection
    if (containsBreakingChange) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.BREAKING_SCHEMA_CHANGE_DETECTED, 1,
          new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    } else {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NON_BREAKING_SCHEMA_CHANGE_DETECTED, 1,
          new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    }
    // Update connection
    // 1. update flag for breaking changes
    final var patch = new ConnectionUpdate()
        .breakingChange(containsBreakingChange)
        .connectionId(connectionId);
    // 2. disable connection and log a timeline event (connection_disabled) if needed
    ConnectionAutoDisabledReason autoDisabledReason = null;
    if (containsBreakingChange) {
      patch.status(ConnectionStatus.INACTIVE);
      autoDisabledReason = ConnectionAutoDisabledReason.SCHEMA_CHANGES_ARE_BREAKING;
    } else if (connectionRead.getNonBreakingChangesPreference() == NonBreakingChangesPreference.DISABLE
        && applySchemaChangeHelper.containsChanges(diff)) {
      patch.status(ConnectionStatus.INACTIVE);
      autoDisabledReason = ConnectionAutoDisabledReason.DISABLE_CONNECTION_IF_ANY_SCHEMA_CHANGES;
    }
    final var updated = updateConnection(patch, autoDisabledReason != null ? autoDisabledReason.name() : null, true);
    return updated;
  }

  /**
   * For a given discovered catalog and connection, calculate a catalog diff, determine if there are
   * breaking changes then disable the connection if necessary.
   */
  public SourceDiscoverSchemaRead diffCatalogAndConditionallyDisable(final UUID connectionId, final UUID discoveredCatalogId)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final var connectionRead = getConnection(connectionId);
    final var source = sourceService.getSourceConnection(connectionRead.getSourceId());
    final var sourceDef = sourceService.getStandardSourceDefinition(source.getSourceDefinitionId());
    final var sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), connectionRead.getSourceId());

    final var discoveredCatalog = retrieveDiscoveredCatalog(discoveredCatalogId, sourceVersion);

    final var diff = getDiff(connectionRead, discoveredCatalog);
    final boolean containsBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff);
    final ConnectionRead updatedConnection = disableConnectionIfNeeded(connectionRead, containsBreakingChange, diff);
    return new SourceDiscoverSchemaRead()
        .breakingChange(containsBreakingChange)
        .catalogDiff(diff)
        .catalog(discoveredCatalog)
        .catalogId(discoveredCatalogId)
        .connectionStatus(updatedConnection.getStatus());
  }

  private AirbyteCatalog retrieveDiscoveredCatalog(final UUID catalogId, final ActorDefinitionVersion sourceVersion)
      throws IOException, io.airbyte.data.exceptions.ConfigNotFoundException {

    final ActorCatalog catalog = catalogService.getActorCatalogById(catalogId);
    final io.airbyte.protocol.models.AirbyteCatalog persistenceCatalog = Jsons.object(
        catalog.getCatalog(),
        io.airbyte.protocol.models.AirbyteCatalog.class);
    return catalogConverter.toApi(persistenceCatalog, sourceVersion);
  }

  /**
   * Build a ConnectionLastJobPerStreamReadItem from a stream descriptor and a job read. This method
   * memoizes the stat-by-stream map for each job to avoid redundant computation in the case where
   * multiple streams are associated with the same job.
   */
  @SuppressWarnings("LineLength")
  private ConnectionLastJobPerStreamReadItem buildLastJobPerStreamReadItem(
                                                                           final io.airbyte.config.StreamDescriptor streamDescriptor,
                                                                           final JobRead jobRead,
                                                                           final Map<Long, Map<io.airbyte.config.StreamDescriptor, StreamStats>> memo) {
    // if this is the first time encountering the job, compute the stat-by-stream map for it
    memo.putIfAbsent(jobRead.getId(), buildStreamStatsMap(jobRead));

    // retrieve the stat for the stream of interest from the memo
    final Optional<StreamStats> statsForThisStream = Optional.ofNullable(memo.get(jobRead.getId()).get(streamDescriptor));

    return new ConnectionLastJobPerStreamReadItem()
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(jobRead.getId())
        .configType(jobRead.getConfigType())
        .jobStatus(jobRead.getStatus())
        .startedAt(jobRead.getStartedAt())
        .endedAt(jobRead.getUpdatedAt()) // assumes the job ended at the last updated time
        .bytesCommitted(statsForThisStream.map(StreamStats::getBytesCommitted).orElse(null))
        .recordsCommitted(statsForThisStream.map(StreamStats::getRecordsCommitted).orElse(null));
  }

  /**
   * Build a map of stream descriptor to stream stats for a given job. This is only called at most
   * once per job, because the result is memoized.
   */
  private Map<io.airbyte.config.StreamDescriptor, StreamStats> buildStreamStatsMap(final JobRead jobRead) {
    final Map<io.airbyte.config.StreamDescriptor, StreamStats> map = new HashMap<>();
    for (final StreamStats stat : jobRead.getStreamAggregatedStats()) {
      final var streamDescriptor = new io.airbyte.config.StreamDescriptor()
          .withName(stat.getStreamName())
          .withNamespace(stat.getStreamNamespace());
      map.put(streamDescriptor, stat);
    }
    return map;
  }

}
