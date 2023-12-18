/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.converters.ConnectionHelper.validateCatalogDoesntContainDuplicateStreamNames;
import static io.airbyte.persistence.job.JobNotifier.CONNECTION_DISABLED_NOTIFICATION;
import static io.airbyte.persistence.job.JobNotifier.CONNECTION_DISABLED_WARNING_NOTIFICATION;
import static io.airbyte.persistence.job.models.Job.REPLICATION_TYPES;
import static java.time.temporal.ChronoUnit.DAYS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionDataHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionSearch;
import io.airbyte.api.model.generated.ConnectionStatusRead;
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationSearch;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.FailureOrigin;
import io.airbyte.api.model.generated.FailureReason;
import io.airbyte.api.model.generated.FailureType;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.converters.ConnectionHelper;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.CatalogDiffConverters;
import io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper;
import io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.UpdateSchemaResult;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.ConnectionMatcher;
import io.airbyte.commons.server.handlers.helpers.ConnectionScheduleHelper;
import io.airbyte.commons.server.handlers.helpers.DestinationMatcher;
import io.airbyte.commons.server.handlers.helpers.PaginationHelper;
import io.airbyte.commons.server.handlers.helpers.SourceMatcher;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BasicSchedule;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.FieldSelectionData;
import io.airbyte.config.Geography;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.Schedule;
import io.airbyte.config.ScheduleData;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.ScheduleType;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.CheckWithCatalog;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.AttemptWithJobInfo;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.persistence.job.models.JobWithStatusAndTimestamp;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
 */
@Singleton
public class ConnectionsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionsHandler.class);

  private final JobPersistence jobPersistence;
  private final ConfigRepository configRepository;
  private final Supplier<UUID> uuidGenerator;
  private final WorkspaceHelper workspaceHelper;
  private final TrackingClient trackingClient;
  private final EventRunner eventRunner;
  private final ConnectionHelper connectionHelper;
  private final FeatureFlagClient featureFlagClient;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final ConnectorDefinitionSpecificationHandler connectorSpecHandler;
  private final JobNotifier jobNotifier;
  private final Integer maxDaysOfOnlyFailedJobsBeforeConnectionDisable;
  private final Integer maxFailedJobsInARowBeforeConnectionDisable;
  private final int maxJobLookback = 10;

  @Inject
  public ConnectionsHandler(
                            final JobPersistence jobPersistence,
                            final ConfigRepository configRepository,
                            @Named("uuidGenerator") final Supplier<UUID> uuidGenerator,
                            final WorkspaceHelper workspaceHelper,
                            final TrackingClient trackingClient,
                            final EventRunner eventRunner,
                            final ConnectionHelper connectionHelper,
                            final FeatureFlagClient featureFlagClient,
                            final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                            final ConnectorDefinitionSpecificationHandler connectorSpecHandler,
                            final JobNotifier jobNotifier,
                            @Value("${airbyte.server.connection.disable.max-days}") final Integer maxDaysOfOnlyFailedJobsBeforeConnectionDisable,
                            @Value("${airbyte.server.connection.disable.max-jobs}") final Integer maxFailedJobsInARowBeforeConnectionDisable) {
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.uuidGenerator = uuidGenerator;
    this.workspaceHelper = workspaceHelper;
    this.trackingClient = trackingClient;
    this.eventRunner = eventRunner;
    this.connectionHelper = connectionHelper;
    this.featureFlagClient = featureFlagClient;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.connectorSpecHandler = connectorSpecHandler;
    this.jobNotifier = jobNotifier;
    this.maxDaysOfOnlyFailedJobsBeforeConnectionDisable = maxDaysOfOnlyFailedJobsBeforeConnectionDisable;
    this.maxFailedJobsInARowBeforeConnectionDisable = maxFailedJobsInARowBeforeConnectionDisable;
  }

  /**
   * Modifies the given StandardSync by applying changes from a partially-filled ConnectionUpdate
   * patch. Any fields that are null in the patch will be left unchanged.
   */
  private static void applyPatchToStandardSync(final StandardSync sync, final ConnectionUpdate patch) throws JsonValidationException {
    // update the sync's schedule using the patch's scheduleType and scheduleData. validations occur in
    // the helper to ensure both fields
    // make sense together.
    if (patch.getScheduleType() != null) {
      ConnectionScheduleHelper.populateSyncFromScheduleTypeAndData(sync, patch.getScheduleType(), patch.getScheduleData());
    }

    // the rest of the fields are straightforward to patch. If present in the patch, set the field to
    // the value
    // in the patch. Otherwise, leave the field unchanged.

    if (patch.getSyncCatalog() != null) {
      validateCatalogDoesntContainDuplicateStreamNames(patch.getSyncCatalog());
      sync.setCatalog(CatalogConverter.toConfiguredProtocol(patch.getSyncCatalog()));
      sync.withFieldSelectionData(CatalogConverter.getFieldSelectionData(patch.getSyncCatalog()));
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
      sync.setStatus(ApiPojoConverters.toPersistenceStatus(patch.getStatus()));
    }

    if (patch.getSourceCatalogId() != null) {
      sync.setSourceCatalogId(patch.getSourceCatalogId());
    }

    if (patch.getResourceRequirements() != null) {
      sync.setResourceRequirements(ApiPojoConverters.resourceRequirementsToInternal(patch.getResourceRequirements()));
    }

    if (patch.getGeography() != null) {
      sync.setGeography(ApiPojoConverters.toPersistenceGeography(patch.getGeography()));
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
      sync.setNonBreakingChangesPreference(ApiPojoConverters.toPersistenceNonBreakingChangesPreference(patch.getNonBreakingChangesPreference()));
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

  public InternalOperationResult autoDisableConnection(final UUID connectionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return autoDisableConnection(connectionId, Instant.now());
  }

  @VisibleForTesting
  InternalOperationResult autoDisableConnection(final UUID connectionId, final Instant timestamp)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // if connection is already inactive, no need to disable
    final StandardSync standardSync = configRepository.getStandardSync(connectionId);
    if (standardSync.getStatus() == Status.INACTIVE) {
      return new InternalOperationResult().succeeded(false);
    }

    final int maxDaysOfOnlyFailedJobs = maxDaysOfOnlyFailedJobsBeforeConnectionDisable;
    final int maxDaysOfOnlyFailedJobsBeforeWarning = maxDaysOfOnlyFailedJobs / 2;
    final int maxFailedJobsInARowBeforeConnectionDisableWarning = maxFailedJobsInARowBeforeConnectionDisable / 2;
    final long currTimestampInSeconds = timestamp.getEpochSecond();
    final Optional<Job> optionalLastJob = jobPersistence.getLastReplicationJob(connectionId);
    final Optional<Job> optionalFirstJob = jobPersistence.getFirstReplicationJob(connectionId);

    if (optionalLastJob.isEmpty()) {
      LOGGER.error("Auto-Disable Connection should not have been attempted if can't get latest replication job.");
      return new InternalOperationResult().succeeded(false);
    }

    if (optionalFirstJob.isEmpty()) {
      LOGGER.error("Auto-Disable Connection should not have been attempted if no replication job has been run.");
      return new InternalOperationResult().succeeded(false);
    }

    final List<JobWithStatusAndTimestamp> jobs = jobPersistence.listJobStatusAndTimestampWithConnection(connectionId,
        REPLICATION_TYPES, timestamp.minus(maxDaysOfOnlyFailedJobs, DAYS));

    int numFailures = 0;
    Optional<Long> successTimestamp = Optional.empty();

    for (final JobWithStatusAndTimestamp job : jobs) {
      final JobStatus jobStatus = job.getStatus();
      if (jobStatus == JobStatus.FAILED) {
        numFailures++;
      } else if (jobStatus == JobStatus.SUCCEEDED) {
        successTimestamp = Optional.of(job.getUpdatedAtInSecond());
        break;
      }
    }

    final boolean warningPreviouslySentForMaxDays =
        warningPreviouslySentForMaxDays(numFailures, successTimestamp, maxDaysOfOnlyFailedJobsBeforeWarning, optionalFirstJob.get(), jobs);

    if (numFailures == 0) {
      return new InternalOperationResult().succeeded(false);
    } else if (numFailures >= maxFailedJobsInARowBeforeConnectionDisable) {
      // disable connection if max consecutive failed jobs limit has been hit
      disableConnection(standardSync, optionalLastJob.get());
      return new InternalOperationResult().succeeded(true);
    } else if (numFailures == maxFailedJobsInARowBeforeConnectionDisableWarning && !warningPreviouslySentForMaxDays) {
      // warn if number of consecutive failures hits 50% of MaxFailedJobsInARow
      jobNotifier.autoDisableConnectionWarning(optionalLastJob.get());
      // explicitly send to email if customer.io api key is set, since email notification cannot be set by
      // configs through UI yet
      jobNotifier.notifyJobByEmail(null, CONNECTION_DISABLED_WARNING_NOTIFICATION, optionalLastJob.get());
      return new InternalOperationResult().succeeded(false);
    }

    // calculate the number of days this connection first tried a replication job, used to ensure not to
    // disable or warn for `maxDaysOfOnlyFailedJobs` if the first job is younger than
    // `maxDaysOfOnlyFailedJobs` days, This avoids cases such as "the very first job run was a failure".
    final int numDaysSinceFirstReplicationJob = getDaysSinceTimestamp(currTimestampInSeconds, optionalFirstJob.get().getCreatedAtInSecond());
    final boolean firstReplicationOlderThanMaxDisableDays = numDaysSinceFirstReplicationJob >= maxDaysOfOnlyFailedJobs;
    final boolean noPreviousSuccess = successTimestamp.isEmpty();

    // disable connection if only failed jobs in the past maxDaysOfOnlyFailedJobs days
    if (firstReplicationOlderThanMaxDisableDays && noPreviousSuccess) {
      disableConnection(standardSync, optionalLastJob.get());
      return new InternalOperationResult().succeeded(true);
    }

    // skip warning if previously sent
    if (warningPreviouslySentForMaxDays || numFailures > maxFailedJobsInARowBeforeConnectionDisableWarning) {
      LOGGER.info("Warning was previously sent for connection: {}", connectionId);
      return new InternalOperationResult().succeeded(false);
    }

    final boolean firstReplicationOlderThanMaxDisableWarningDays = numDaysSinceFirstReplicationJob >= maxDaysOfOnlyFailedJobsBeforeWarning;
    final boolean successOlderThanPrevFailureByMaxWarningDays = // set to true if no previous success is found
        noPreviousSuccess || getDaysSinceTimestamp(currTimestampInSeconds, successTimestamp.get()) >= maxDaysOfOnlyFailedJobsBeforeWarning;

    // send warning if there are only failed jobs in the past maxDaysOfOnlyFailedJobsBeforeWarning days
    // _unless_ a warning should have already been sent in the previous failure
    if (firstReplicationOlderThanMaxDisableWarningDays && successOlderThanPrevFailureByMaxWarningDays) {
      jobNotifier.autoDisableConnectionWarning(optionalLastJob.get());
      // explicitly send to email if customer.io api key is set, since email notification cannot be set by
      // configs through UI yet
      jobNotifier.notifyJobByEmail(null, CONNECTION_DISABLED_WARNING_NOTIFICATION, optionalLastJob.get());
    }
    return new InternalOperationResult().succeeded(false);
  }

  private void disableConnection(final StandardSync standardSync, final Job lastJob) throws IOException {
    standardSync.setStatus(Status.INACTIVE);
    configRepository.writeStandardSync(standardSync);

    jobNotifier.autoDisableConnection(lastJob);
    // explicitly send to email if customer.io api key is set, since email notification cannot be set by
    // configs through UI yet
    jobNotifier.notifyJobByEmail(null, CONNECTION_DISABLED_NOTIFICATION, lastJob);
  }

  private int getDaysSinceTimestamp(final long currentTimestampInSeconds, final long timestampInSeconds) {
    return Math.toIntExact(TimeUnit.SECONDS.toDays(currentTimestampInSeconds - timestampInSeconds));
  }

  // Checks to see if warning should have been sent in the previous failure, if so skip sending of
  // warning to avoid spam
  // Assume warning has been sent if either of the following is true:
  // 1. no success found in the time span and the previous failure occurred
  // maxDaysOfOnlyFailedJobsBeforeWarning days after the first job
  // 2. success found and the previous failure occurred maxDaysOfOnlyFailedJobsBeforeWarning days
  // after that success
  private boolean warningPreviouslySentForMaxDays(final int numFailures,
                                                  final Optional<Long> successTimestamp,
                                                  final int maxDaysOfOnlyFailedJobsBeforeWarning,
                                                  final Job firstJob,
                                                  final List<JobWithStatusAndTimestamp> jobs) {
    // no previous warning sent if there was no previous failure
    if (numFailures <= 1 || jobs.size() <= 1) {
      return false;
    }

    // get previous failed job (skipping first job since that's considered "current" job)
    JobWithStatusAndTimestamp prevFailedJob = jobs.get(1);
    for (int i = 2; i < jobs.size(); i++) {
      if (prevFailedJob.getStatus() == JobStatus.FAILED) {
        break;
      }
      prevFailedJob = jobs.get(i);
    }

    final boolean successExists = successTimestamp.isPresent();
    boolean successOlderThanPrevFailureByMaxWarningDays = false;
    if (successExists) {
      successOlderThanPrevFailureByMaxWarningDays =
          getDaysSinceTimestamp(prevFailedJob.getUpdatedAtInSecond(), successTimestamp.get()) >= maxDaysOfOnlyFailedJobsBeforeWarning;
    }
    final boolean prevFailureOlderThanFirstJobByMaxWarningDays =
        getDaysSinceTimestamp(prevFailedJob.getUpdatedAtInSecond(), firstJob.getUpdatedAtInSecond()) >= maxDaysOfOnlyFailedJobsBeforeWarning;

    return (successExists && successOlderThanPrevFailureByMaxWarningDays)
        || (!successExists && prevFailureOlderThanFirstJobByMaxWarningDays);
  }

  public ConnectionRead createConnection(final ConnectionCreate connectionCreate)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    // Validate source and destination
    final SourceConnection sourceConnection = configRepository.getSourceConnection(connectionCreate.getSourceId());
    final DestinationConnection destinationConnection = configRepository.getDestinationConnection(connectionCreate.getDestinationId());

    // Set this as default name if connectionCreate doesn't have it
    final String defaultName = sourceConnection.getName() + " <> " + destinationConnection.getName();

    final List<UUID> operationIds = connectionCreate.getOperationIds() != null ? connectionCreate.getOperationIds() : Collections.emptyList();

    ConnectionHelper.validateWorkspace(workspaceHelper,
        connectionCreate.getSourceId(),
        connectionCreate.getDestinationId(),
        operationIds);

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
        .withStatus(ApiPojoConverters.toPersistenceStatus(connectionCreate.getStatus()))
        .withSourceCatalogId(connectionCreate.getSourceCatalogId())
        .withGeography(getGeographyFromConnectionCreateOrWorkspace(connectionCreate))
        .withBreakingChange(false)
        .withNonBreakingChangesPreference(
            ApiPojoConverters.toPersistenceNonBreakingChangesPreference(connectionCreate.getNonBreakingChangesPreference()));
    if (connectionCreate.getResourceRequirements() != null) {
      standardSync.withResourceRequirements(ApiPojoConverters.resourceRequirementsToInternal(connectionCreate.getResourceRequirements()));
    }

    // TODO Undesirable behavior: sending a null configured catalog should not be valid?
    if (connectionCreate.getSyncCatalog() != null) {
      validateCatalogDoesntContainDuplicateStreamNames(connectionCreate.getSyncCatalog());
      standardSync.withCatalog(CatalogConverter.toConfiguredProtocol(connectionCreate.getSyncCatalog()));
      standardSync.withFieldSelectionData(CatalogConverter.getFieldSelectionData(connectionCreate.getSyncCatalog()));
    } else {
      standardSync.withCatalog(new ConfiguredAirbyteCatalog().withStreams(Collections.emptyList()));
      standardSync.withFieldSelectionData(new FieldSelectionData());
    }

    if (connectionCreate.getSchedule() != null && connectionCreate.getScheduleType() != null) {
      throw new JsonValidationException("supply old or new schedule schema but not both");
    }

    if (connectionCreate.getScheduleType() != null) {
      ConnectionScheduleHelper.populateSyncFromScheduleTypeAndData(standardSync, connectionCreate.getScheduleType(),
          connectionCreate.getScheduleData());
    } else {
      populateSyncFromLegacySchedule(standardSync, connectionCreate);
    }
    final UUID workspaceId = workspaceHelper.getWorkspaceForDestinationId(connectionCreate.getDestinationId());
    if (workspaceId != null && featureFlagClient.boolVariation(CheckWithCatalog.INSTANCE, new Workspace(workspaceId))) {
      // TODO this is the hook for future check with catalog work
      LOGGER.info("Entered into Dark Launch Code for Check with Catalog");
    }
    configRepository.writeStandardSync(standardSync);

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
      throws JsonValidationException, ConfigNotFoundException, IOException {

    if (connectionCreate.getGeography() != null) {
      return ApiPojoConverters.toPersistenceGeography(connectionCreate.getGeography());
    }

    // connectionCreate didn't specify a geography, so use the workspace default geography if one exists
    final UUID workspaceId = workspaceHelper.getWorkspaceForSourceId(connectionCreate.getSourceId());
    final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, true);

    if (workspace.getDefaultGeography() != null) {
      return workspace.getDefaultGeography();
    }

    // if the workspace doesn't have a default geography, default to 'auto'
    return Geography.AUTO;
  }

  private void populateSyncFromLegacySchedule(final StandardSync standardSync, final ConnectionCreate connectionCreate) {
    if (connectionCreate.getSchedule() != null) {
      final Schedule schedule = new Schedule()
          .withTimeUnit(ApiPojoConverters.toPersistenceTimeUnit(connectionCreate.getSchedule().getTimeUnit()))
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
          new BasicSchedule().withTimeUnit(ApiPojoConverters.toBasicScheduleTimeUnit(connectionCreate.getSchedule().getTimeUnit()))
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
      trackingClient.track(workspaceId, "New Connection - Backend", metadataBuilder.build());
    } catch (final Exception e) {
      LOGGER.error("failed while reporting usage.", e);
    }
  }

  private Builder<String, Object> generateMetadata(final StandardSync standardSync) {
    final Builder<String, Object> metadata = ImmutableMap.builder();

    final UUID connectionId = standardSync.getConnectionId();
    final StandardSourceDefinition sourceDefinition = configRepository
        .getSourceDefinitionFromConnection(connectionId);
    final StandardDestinationDefinition destinationDefinition = configRepository
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
    metadata.put("frequency", frequencyString);
    return metadata;
  }

  public ConnectionRead updateConnection(final ConnectionUpdate connectionPatch)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    final UUID connectionId = connectionPatch.getConnectionId();

    LOGGER.debug("Starting updateConnection for connectionId {}...", connectionId);
    LOGGER.debug("incoming connectionPatch: {}", connectionPatch);

    final StandardSync sync = configRepository.getStandardSync(connectionId);
    LOGGER.debug("initial StandardSync: {}", sync);

    validateConnectionPatch(workspaceHelper, sync, connectionPatch);

    final ConnectionRead initialConnectionRead = ApiPojoConverters.internalToConnectionRead(sync);
    LOGGER.debug("initial ConnectionRead: {}", initialConnectionRead);

    applyPatchToStandardSync(sync, connectionPatch);

    LOGGER.debug("patched StandardSync before persisting: {}", sync);
    configRepository.writeStandardSync(sync);

    eventRunner.update(connectionId);

    final ConnectionRead updatedRead = buildConnectionRead(connectionId);
    LOGGER.debug("final connectionRead: {}", updatedRead);

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

  public ConnectionReadList listConnectionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listConnectionsForWorkspace(workspaceIdRequestBody, false);
  }

  public ConnectionReadList listConnectionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody, final boolean includeDeleted)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<ConnectionRead> connectionReads = Lists.newArrayList();

    for (final StandardSync standardSync : configRepository.listWorkspaceStandardSyncs(workspaceIdRequestBody.getWorkspaceId(), includeDeleted)) {
      connectionReads.add(ApiPojoConverters.internalToConnectionRead(standardSync));
    }

    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionReadList listAllConnectionsForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listConnectionsForWorkspace(workspaceIdRequestBody, true);
  }

  public ConnectionReadList listConnectionsForSource(final UUID sourceId, final boolean includeDeleted) throws IOException {
    final List<ConnectionRead> connectionReads = Lists.newArrayList();
    for (final StandardSync standardSync : configRepository.listConnectionsBySource(sourceId, includeDeleted)) {
      connectionReads.add(ApiPojoConverters.internalToConnectionRead(standardSync));
    }
    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionReadList listConnections() throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<ConnectionRead> connectionReads = Lists.newArrayList();

    for (final StandardSync standardSync : configRepository.listStandardSyncs()) {
      if (standardSync.getStatus() == StandardSync.Status.DEPRECATED) {
        continue;
      }
      connectionReads.add(ApiPojoConverters.internalToConnectionRead(standardSync));
    }

    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionRead getConnection(final UUID connectionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return buildConnectionRead(connectionId);
  }

  public CatalogDiff getDiff(final AirbyteCatalog oldCatalog, final AirbyteCatalog newCatalog, final ConfiguredAirbyteCatalog configuredCatalog)
      throws JsonValidationException {
    return new CatalogDiff().transforms(CatalogHelpers.getCatalogDiff(
        CatalogHelpers.configuredCatalogToCatalog(CatalogConverter.toProtocolKeepAllStreams(oldCatalog)),
        CatalogHelpers.configuredCatalogToCatalog(CatalogConverter.toProtocolKeepAllStreams(newCatalog)), configuredCatalog)
        .stream()
        .map(CatalogDiffConverters::streamTransformToApi)
        .toList());
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
        stream -> stream.getConfig()));
  }

  public Optional<AirbyteCatalog> getConnectionAirbyteCatalog(final UUID connectionId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSync connection = configRepository.getStandardSync(connectionId);
    if (connection.getSourceCatalogId() == null) {
      return Optional.empty();
    }
    final ActorCatalog catalog = configRepository.getActorCatalogById(connection.getSourceCatalogId());
    final StandardSourceDefinition sourceDefinition = configRepository.getSourceDefinitionFromSource(connection.getSourceId());
    final SourceConnection sourceConnection = configRepository.getSourceConnection(connection.getSourceId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, sourceConnection.getWorkspaceId(), connection.getSourceId());
    final io.airbyte.protocol.models.AirbyteCatalog jsonCatalog = Jsons.object(catalog.getCatalog(), io.airbyte.protocol.models.AirbyteCatalog.class);
    final StandardDestinationDefinition destination = configRepository.getDestinationDefinitionFromConnection(connectionId);
    // Note: we're using the workspace from the source to save an extra db request.
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destination, sourceConnection.getWorkspaceId());
    final List<DestinationSyncMode> supportedDestinationSyncModes =
        Enums.convertListTo(destinationVersion.getSpec().getSupportedDestinationSyncModes(), DestinationSyncMode.class);
    final var convertedCatalog = Optional.of(CatalogConverter.toApi(jsonCatalog, sourceVersion));
    if (convertedCatalog.isPresent()) {
      convertedCatalog.get().getStreams().forEach((streamAndConfiguration) -> {
        CatalogConverter.ensureCompatibleDestinationSyncMode(streamAndConfiguration, supportedDestinationSyncModes);
      });
    }
    return convertedCatalog;
  }

  public ConnectionReadList searchConnections(final ConnectionSearch connectionSearch)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final List<ConnectionRead> reads = Lists.newArrayList();
    for (final StandardSync standardSync : configRepository.listStandardSyncs()) {
      if (standardSync.getStatus() != StandardSync.Status.DEPRECATED) {
        final ConnectionRead connectionRead = ApiPojoConverters.internalToConnectionRead(standardSync);
        if (matchSearch(connectionSearch, connectionRead)) {
          reads.add(connectionRead);
        }
      }
    }

    return new ConnectionReadList().connections(reads);
  }

  public boolean matchSearch(final ConnectionSearch connectionSearch, final ConnectionRead connectionRead)
      throws JsonValidationException, ConfigNotFoundException, IOException {

    final SourceConnection sourceConnection = configRepository.getSourceConnection(connectionRead.getSourceId());
    final StandardSourceDefinition sourceDefinition =
        configRepository.getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    final SourceRead sourceRead = SourceHandler.toSourceRead(sourceConnection, sourceDefinition);

    final DestinationConnection destinationConnection = configRepository.getDestinationConnection(connectionRead.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(destinationConnection.getDestinationDefinitionId());
    final DestinationRead destinationRead = DestinationHandler.toDestinationRead(destinationConnection, destinationDefinition);

    final ConnectionMatcher connectionMatcher = new ConnectionMatcher(connectionSearch);
    final ConnectionRead connectionReadFromSearch = connectionMatcher.match(connectionRead);

    return (connectionReadFromSearch == null || connectionReadFromSearch.equals(connectionRead))
        && matchSearch(connectionSearch.getSource(), sourceRead)
        && matchSearch(connectionSearch.getDestination(), destinationRead);
  }

  // todo (cgardens) - make this static. requires removing one bad dependency in SourceHandlerTest
  public boolean matchSearch(final SourceSearch sourceSearch, final SourceRead sourceRead) {
    final SourceMatcher sourceMatcher = new SourceMatcher(sourceSearch);
    final SourceRead sourceReadFromSearch = sourceMatcher.match(sourceRead);

    return (sourceReadFromSearch == null || sourceReadFromSearch.equals(sourceRead));
  }

  // todo (cgardens) - make this static. requires removing one bad dependency in
  // DestinationHandlerTest
  public boolean matchSearch(final DestinationSearch destinationSearch, final DestinationRead destinationRead) {
    final DestinationMatcher destinationMatcher = new DestinationMatcher(destinationSearch);
    final DestinationRead destinationReadFromSearch = destinationMatcher.match(destinationRead);

    return (destinationReadFromSearch == null || destinationReadFromSearch.equals(destinationRead));
  }

  public void deleteConnection(final UUID connectionId) throws JsonValidationException, ConfigNotFoundException, IOException {
    connectionHelper.deleteConnection(connectionId);
    eventRunner.forceDeleteConnection(connectionId);
  }

  private ConnectionRead buildConnectionRead(final UUID connectionId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSync standardSync = configRepository.getStandardSync(connectionId);
    return ApiPojoConverters.internalToConnectionRead(standardSync);
  }

  public ConnectionReadList listConnectionsForWorkspaces(final ListConnectionsForWorkspacesRequestBody listConnectionsForWorkspacesRequestBody)
      throws IOException {

    final List<ConnectionRead> connectionReads = Lists.newArrayList();

    final Map<UUID, List<StandardSync>> workspaceIdToStandardSyncsMap = configRepository.listWorkspaceStandardSyncsPaginated(
        listConnectionsForWorkspacesRequestBody.getWorkspaceIds(),
        listConnectionsForWorkspacesRequestBody.getIncludeDeleted(),
        PaginationHelper.pageSize(listConnectionsForWorkspacesRequestBody.getPagination()),
        PaginationHelper.rowOffset(listConnectionsForWorkspacesRequestBody.getPagination()));

    for (final Entry<UUID, List<StandardSync>> entry : workspaceIdToStandardSyncsMap.entrySet()) {
      final UUID workspaceId = entry.getKey();
      for (final StandardSync standardSync : entry.getValue()) {
        final ConnectionRead connectionRead = ApiPojoConverters.internalToConnectionRead(standardSync);
        connectionRead.setWorkspaceId(workspaceId);
        connectionReads.add(connectionRead);
      }
    }
    return new ConnectionReadList().connections(connectionReads);
  }

  public ConnectionReadList listConnectionsForActorDefinition(final ActorDefinitionRequestBody actorDefinitionRequestBody)
      throws IOException {

    final List<ConnectionRead> connectionReads = new ArrayList<>();

    final List<StandardSync> standardSyncs = configRepository.listConnectionsByActorDefinitionIdAndType(
        actorDefinitionRequestBody.getActorDefinitionId(),
        actorDefinitionRequestBody.getActorType().toString(),
        false);

    for (final StandardSync standardSync : standardSyncs) {
      final ConnectionRead connectionRead = ApiPojoConverters.internalToConnectionRead(standardSync);
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

  public List<ConnectionStatusRead> getConnectionStatuses(
                                                          final ConnectionStatusesRequestBody connectionStatusesRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final List<UUID> connectionIds = connectionStatusesRequestBody.getConnectionIds();
    final List<ConnectionStatusRead> result = new ArrayList<>();
    for (final UUID connectionId : connectionIds) {
      final List<Job> jobs = jobPersistence.listJobs(Set.of(JobConfig.ConfigType.SYNC, JobConfig.ConfigType.RESET_CONNECTION),
          connectionId.toString(),
          maxJobLookback);
      final boolean isRunning = jobs.stream().anyMatch(job -> JobStatus.NON_TERMINAL_STATUSES.contains(job.getStatus()));

      final Optional<Job> lastSucceededOrFailedJob =
          jobs.stream().filter(job -> JobStatus.TERMINAL_STATUSES.contains(job.getStatus()) && job.getStatus() != JobStatus.CANCELLED).findFirst();
      final Optional<JobStatus> lastSyncStatus = lastSucceededOrFailedJob.map(job -> job.getStatus());

      final Optional<Job> lastSuccessfulJob = jobs.stream().filter(job -> job.getStatus() == JobStatus.SUCCEEDED).findFirst();
      final Optional<Long> lastSuccessTimestamp = lastSuccessfulJob.map(job -> job.getUpdatedAtInSecond());

      final ConnectionStatusRead connectionStatus = new ConnectionStatusRead()
          .connectionId(connectionId)
          .isRunning(isRunning)
          .lastSyncJobStatus(Enums.convertTo(lastSyncStatus.orElse(null),
              io.airbyte.api.model.generated.JobStatus.class))
          .lastSuccessfulSync(lastSuccessTimestamp.orElse(null))
          .nextSync(null)
          .isLastCompletedJobReset(lastSucceededOrFailedJob.map(job -> job.getConfigType() == ConfigType.RESET_CONNECTION).orElse(false));
      if (lastSucceededOrFailedJob.isPresent()) {
        connectionStatus.lastSyncJobId(lastSucceededOrFailedJob.get().getId());
        final Optional<Attempt> lastAttempt = lastSucceededOrFailedJob.get().getLastAttempt();
        if (lastAttempt.isPresent()) {
          connectionStatus.lastSyncAttemptNumber(lastAttempt.get().getAttemptNumber());
        }
      }
      final Optional<io.airbyte.api.model.generated.FailureReason> failureReason = lastSucceededOrFailedJob.flatMap(Job::getLastFailedAttempt)
          .flatMap(Attempt::getFailureSummary)
          .flatMap(s -> s.getFailures().stream().findFirst())
          .map(reason -> mapFailureReason(reason));
      if (failureReason.isPresent() && lastSucceededOrFailedJob.get().getStatus() == JobStatus.FAILED) {
        connectionStatus.setFailureReason(failureReason.get());
      }
      result.add(connectionStatus);
    }

    return result;
  }

  /**
   * Returns bytes committed per day for the given connection for the last 30 days in the given
   * timezone.
   *
   * @param connectionDataHistoryRequestBody the connectionId and timezone string
   * @return list of ConnectionDataHistoryReadItems (timestamp and bytes committed)
   */
  public List<ConnectionDataHistoryReadItem> getConnectionDataHistory(final ConnectionDataHistoryRequestBody connectionDataHistoryRequestBody)
      throws IOException {

    final ZoneId requestZone = ZoneId.of(connectionDataHistoryRequestBody.getTimezone());

    // Start time in designated timezone
    final ZonedDateTime endTimeInUserTimeZone = Instant.now().atZone(ZoneId.of(connectionDataHistoryRequestBody.getTimezone()));
    final ZonedDateTime startTimeInUserTimeZone = endTimeInUserTimeZone.toLocalDate().atStartOfDay(requestZone).minusDays(29);
    // Convert start time to UTC (since that's what the database uses)
    final Instant startTimeInUTC = startTimeInUserTimeZone.toInstant();

    final List<AttemptWithJobInfo> attempts = jobPersistence.listAttemptsForConnectionAfterTimestamp(
        connectionDataHistoryRequestBody.getConnectionId(),
        ConfigType.SYNC,
        startTimeInUTC);

    // we want an entry per day - even if it's empty
    final Map<LocalDate, ConnectionDataHistoryReadItem> connectionDataHistoryReadItemsByDate = new HashMap<>();
    final LocalDate startDate = startTimeInUserTimeZone.toLocalDate();
    final LocalDate endDate = endTimeInUserTimeZone.toLocalDate();
    for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
      connectionDataHistoryReadItemsByDate.put(date, new ConnectionDataHistoryReadItem()
          .timestamp(Math.toIntExact(date.atStartOfDay(requestZone).toEpochSecond()))
          .recordsCommitted(0L));
    }

    for (final AttemptWithJobInfo attempt : attempts) {
      final Optional<Long> endedAtOptional = attempt.getAttempt().getEndedAtInSecond();

      if (endedAtOptional.isPresent()) {
        // Convert the endedAt timestamp from the database to the designated timezone
        final Instant attemptEndedAt = Instant.ofEpochSecond(endedAtOptional.get());
        final LocalDate attemptDateInUserTimeZone = attemptEndedAt.atZone(requestZone)
            .toLocalDate();

        // Merge it with the bytes synced from the attempt
        long recordsCommitted = 0;
        final Optional<JobOutput> attemptOutput = attempt.getAttempt().getOutput();
        if (attemptOutput.isPresent()) {
          recordsCommitted = attemptOutput.get().getSync().getStandardSyncSummary().getTotalStats().getRecordsCommitted();
        }

        // Update the bytes synced for the corresponding day
        final ConnectionDataHistoryReadItem existingItem = connectionDataHistoryReadItemsByDate.get(attemptDateInUserTimeZone);
        existingItem.setRecordsCommitted(existingItem.getRecordsCommitted() + recordsCommitted);
      }
    }

    // Sort the results by date
    return connectionDataHistoryReadItemsByDate.values().stream()
        .sorted(Comparator.comparing(ConnectionDataHistoryReadItem::getTimestamp))
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

    final TreeMap<LocalDate, Map<List<String>, Long>> connectionStreamHistoryReadItemsByDate = new TreeMap<>();
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
      throws JsonValidationException, ConfigNotFoundException, IOException {
    LOGGER.info("Applying schema change for connection '{}' only", request.getConnectionId());
    final ConnectionRead connection = buildConnectionRead(request.getConnectionId());
    final Optional<io.airbyte.api.model.generated.AirbyteCatalog> catalogUsedToMakeConfiguredCatalog =
        getConnectionAirbyteCatalog(request.getConnectionId());
    final io.airbyte.api.model.generated.AirbyteCatalog currentCatalog = connection.getSyncCatalog();
    final CatalogDiff diffToApply = getDiff(
        catalogUsedToMakeConfiguredCatalog.orElse(currentCatalog),
        request.getCatalog(),
        CatalogConverter.toConfiguredProtocol(currentCatalog));
    final ConnectionUpdate updateObject =
        new ConnectionUpdate().connectionId(connection.getConnectionId());
    final UUID destinationDefinitionId =
        configRepository.getDestinationDefinitionFromConnection(connection.getConnectionId()).getDestinationDefinitionId();
    final var supportedDestinationSyncModes =
        connectorSpecHandler.getDestinationSpecification(new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId)
            .workspaceId(request.getWorkspaceId())).getSupportedDestinationSyncModes();
    final CatalogDiff appliedDiff;
    if (AutoPropagateSchemaChangeHelper.shouldAutoPropagate(diffToApply, connection)) {
      // NOTE: appliedDiff is the part of the diff that were actually applied.
      appliedDiff = applySchemaChangeInternal(updateObject.getConnectionId(),
          request.getWorkspaceId(),
          updateObject,
          currentCatalog,
          request.getCatalog(),
          diffToApply.getTransforms(),
          request.getCatalogId(),
          connection.getNonBreakingChangesPreference(), supportedDestinationSyncModes);
      updateConnection(updateObject);
      LOGGER.info("Propagating changes for connectionId: '{}', new catalogId '{}'",
          connection.getConnectionId(), request.getCatalogId());
    } else {
      appliedDiff = null;
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
    final AutoPropagateSchemaChangeHelper.UpdateSchemaResult propagateResult = AutoPropagateSchemaChangeHelper.getUpdatedSchema(
        currentSyncCatalog,
        newCatalog,
        transformations,
        nonBreakingChangesPreference,
        supportedDestinationSyncModes,
        featureFlagClient, workspaceId);
    updateObject.setSyncCatalog(propagateResult.catalog());
    updateObject.setSourceCatalogId(sourceCatalogId);
    trackSchemaChange(workspaceId, connectionId, propagateResult);
    return propagateResult.appliedDiff();
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
        payload.put("stream_namespace", streamTransform.getStreamDescriptor().getNamespace());
        payload.put("stream_name", streamTransform.getStreamDescriptor().getName());
        if (streamTransform.getTransformType() == TransformTypeEnum.UPDATE_STREAM) {
          payload.put("stream_field_changes", Jsons.serialize(streamTransform.getUpdateStream()));
        }
        trackingClient.track(workspaceId, "Schema Changes", payload);
      }
    } catch (final Exception e) {
      LOGGER.error("Error while sending tracking event for schema change", e);
    }
  }

}
