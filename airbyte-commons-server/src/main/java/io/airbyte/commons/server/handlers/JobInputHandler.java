/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.Job.REPLICATION_TYPES;
import static io.airbyte.config.Job.SYNC_REPLICATION_TYPES;
import static io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirementsForJobType;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.CheckInput;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SyncInput;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.converters.ConfigReplacer;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.handlers.helpers.ContextBuilder;
import io.airbyte.commons.temporal.TemporalWorkflowUtils;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.JobTypeResourceLimit.JobType;
import io.airbyte.config.RefreshConfig;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.State;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.UseAsyncReplicate;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler that allow to fetch a job input.
 */
@Singleton
public class JobInputHandler {

  private final JobPersistence jobPersistence;
  private final FeatureFlagClient featureFlagClient;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ConfigInjector configInjector;
  private final AttemptHandler attemptHandler;
  private final StateHandler stateHandler;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final ContextBuilder contextBuilder;
  private final ConnectionService connectionService;
  private final SourceService sourceService;
  private final DestinationService destinationService;

  private static final Logger LOGGER = LoggerFactory.getLogger(JobInputHandler.class);

  @SuppressWarnings("ParameterName")
  public JobInputHandler(final JobPersistence jobPersistence,
                         final FeatureFlagClient featureFlagClient,
                         final OAuthConfigSupplier oAuthConfigSupplier,
                         final ConfigInjector configInjector,
                         final AttemptHandler attemptHandler,
                         final StateHandler stateHandler,
                         final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                         final ContextBuilder contextBuilder,
                         final ConnectionService connectionService,
                         final SourceService sourceService,
                         final DestinationService destinationService) {
    this.jobPersistence = jobPersistence;
    this.featureFlagClient = featureFlagClient;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.configInjector = configInjector;
    this.attemptHandler = attemptHandler;
    this.stateHandler = stateHandler;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.contextBuilder = contextBuilder;
    this.connectionService = connectionService;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
  }

  /**
   * Generate a job input.
   */
  public Object getJobInput(final SyncInput input) {
    try {
      ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, input.getAttemptNumber(), JOB_ID_KEY, input.getJobId()));
      final long jobId = input.getJobId();
      final int attempt = Math.toIntExact(input.getAttemptNumber());

      final Job job = jobPersistence.getJob(jobId);
      final JobSyncConfig config = getJobSyncConfig(jobId, job.getConfig());

      ActorDefinitionVersion sourceVersion = null;

      final UUID connectionId = UUID.fromString(job.getScope());
      final StandardSync standardSync = connectionService.getStandardSync(connectionId);

      final AttemptSyncConfig attemptSyncConfig = new AttemptSyncConfig();
      getCurrentConnectionState(connectionId).ifPresent(attemptSyncConfig::setState);

      final JobConfig.ConfigType jobConfigType = job.getConfig().getConfigType();

      if (SYNC_REPLICATION_TYPES.contains(jobConfigType)) {
        final SourceConnection source = sourceService.getSourceConnection(standardSync.getSourceId());
        sourceVersion = actorDefinitionVersionHelper.getSourceVersion(
            sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()),
            source.getWorkspaceId(),
            source.getSourceId());
        final JsonNode sourceConfiguration = oAuthConfigSupplier.injectSourceOAuthParameters(
            source.getSourceDefinitionId(),
            source.getSourceId(),
            source.getWorkspaceId(),
            source.getConfiguration());
        attemptSyncConfig.setSourceConfiguration(configInjector.injectConfig(sourceConfiguration, source.getSourceDefinitionId()));
      } else if (JobConfig.ConfigType.RESET_CONNECTION.equals(jobConfigType)) {
        final JobResetConnectionConfig resetConnection = job.getConfig().getResetConnection();
        final ResetSourceConfiguration resetSourceConfiguration = resetConnection.getResetSourceConfiguration();
        attemptSyncConfig
            .setSourceConfiguration(resetSourceConfiguration == null ? Jsons.emptyObject() : Jsons.jsonNode(resetSourceConfiguration));
      }

      final JobRunConfig jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt);

      final DestinationConnection destination = destinationService.getDestinationConnection(standardSync.getDestinationId());
      final ActorDefinitionVersion destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(
              destinationService.getStandardDestinationDefinition(destination.getDestinationDefinitionId()),
              destination.getWorkspaceId(),
              destination.getDestinationId());
      final JsonNode destinationConfiguration = oAuthConfigSupplier.injectDestinationOAuthParameters(
          destination.getDestinationDefinitionId(),
          destination.getDestinationId(),
          destination.getWorkspaceId(),
          destination.getConfiguration());
      attemptSyncConfig.setDestinationConfiguration(configInjector.injectConfig(destinationConfiguration, destination.getDestinationDefinitionId()));

      final IntegrationLauncherConfig sourceLauncherConfig = getSourceIntegrationLauncherConfig(
          jobId,
          attempt,
          connectionId,
          config,
          sourceVersion,
          attemptSyncConfig.getSourceConfiguration());

      final IntegrationLauncherConfig destinationLauncherConfig = getDestinationIntegrationLauncherConfig(
          jobId,
          attempt,
          connectionId,
          config,
          destinationVersion,
          attemptSyncConfig.getDestinationConfiguration(),
          Map.of());

      final List<Context> featureFlagContext = new ArrayList<>();
      featureFlagContext.add(new Workspace(config.getWorkspaceId()));
      if (standardSync.getConnectionId() != null) {
        featureFlagContext.add(new Connection(standardSync.getConnectionId()));
      }

      final boolean useAsyncReplicate = featureFlagClient.boolVariation(UseAsyncReplicate.INSTANCE, new Multi(featureFlagContext));

      final ConnectionContext connectionContext = contextBuilder.fromConnectionId(connectionId);

      final StandardSyncInput syncInput = new StandardSyncInput()
          .withNamespaceDefinition(config.getNamespaceDefinition())
          .withNamespaceFormat(config.getNamespaceFormat())
          .withPrefix(config.getPrefix())
          .withSourceId(standardSync.getSourceId())
          .withDestinationId(standardSync.getDestinationId())
          .withSourceConfiguration(attemptSyncConfig.getSourceConfiguration())
          .withDestinationConfiguration(attemptSyncConfig.getDestinationConfiguration())
          .withOperationSequence(config.getOperationSequence())
          .withWebhookOperationConfigs(config.getWebhookOperationConfigs())
          .withSyncResourceRequirements(config.getSyncResourceRequirements())
          .withConnectionId(connectionId)
          .withWorkspaceId(config.getWorkspaceId())
          .withIsReset(JobConfig.ConfigType.RESET_CONNECTION.equals(jobConfigType))
          .withConnectionContext(connectionContext)
          .withUseAsyncReplicate(useAsyncReplicate);

      saveAttemptSyncConfig(jobId, attempt, connectionId, attemptSyncConfig);
      return new JobInput(jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, syncInput);
    } catch (final Exception e) {
      throw new RetryableException(e);
    }
  }

  /**
   * Generate a check job input.
   */
  public Object getCheckJobInput(final CheckInput input) {
    try {
      final Long jobId = input.getJobId();
      final Integer attemptNumber = input.getAttemptNumber();

      final Job job = jobPersistence.getJob(jobId);
      final JobConfig jobConfig = job.getConfig();
      final JobSyncConfig jobSyncConfig = getJobSyncConfig(jobId, jobConfig);

      final UUID connectionId = UUID.fromString(job.getScope());
      final StandardSync standardSync = connectionService.getStandardSync(connectionId);

      final DestinationConnection destination = destinationService.getDestinationConnection(standardSync.getDestinationId());
      final StandardDestinationDefinition destinationDefinition =
          destinationService.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
      final ActorDefinitionVersion destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), destination.getDestinationId());

      final SourceConnection source = sourceService.getSourceConnection(standardSync.getSourceId());
      final StandardSourceDefinition sourceDefinition =
          sourceService.getStandardSourceDefinition(source.getSourceDefinitionId());
      final ActorDefinitionVersion sourceVersion =
          actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId());

      final JsonNode sourceConfiguration = getSourceConfiguration(source);
      final JsonNode destinationConfiguration = getDestinationConfiguration(destination);

      final IntegrationLauncherConfig sourceLauncherConfig = getSourceIntegrationLauncherConfig(
          jobId,
          attemptNumber,
          connectionId,
          jobSyncConfig,
          sourceVersion,
          sourceConfiguration);

      final IntegrationLauncherConfig destinationLauncherConfig =
          getDestinationIntegrationLauncherConfig(
              jobId,
              attemptNumber,
              connectionId,
              jobSyncConfig,
              destinationVersion,
              destinationConfiguration,
              Collections.emptyMap());

      final ResourceRequirements sourceCheckResourceRequirements =
          getResourceRequirementsForJobType(sourceDefinition.getResourceRequirements(), JobType.CHECK_CONNECTION).orElse(null);

      ActorContext sourceContext = contextBuilder.fromSource(source);

      final StandardCheckConnectionInput sourceCheckConnectionInput = new StandardCheckConnectionInput()
          .withActorType(ActorType.SOURCE)
          .withActorId(source.getSourceId())
          .withConnectionConfiguration(sourceConfiguration)
          .withResourceRequirements(sourceCheckResourceRequirements)
          .withActorContext(sourceContext);

      final ResourceRequirements destinationCheckResourceRequirements =
          getResourceRequirementsForJobType(destinationDefinition.getResourceRequirements(), JobType.CHECK_CONNECTION).orElse(null);

      ActorContext destinationContext = contextBuilder.fromDestination(destination);

      final StandardCheckConnectionInput destinationCheckConnectionInput = new StandardCheckConnectionInput()
          .withActorType(ActorType.DESTINATION)
          .withActorId(destination.getDestinationId())
          .withConnectionConfiguration(destinationConfiguration)
          .withResourceRequirements(destinationCheckResourceRequirements)
          .withActorContext(destinationContext);
      return new SyncJobCheckConnectionInputs(
          sourceLauncherConfig,
          destinationLauncherConfig,
          sourceCheckConnectionInput,
          destinationCheckConnectionInput);
    } catch (final Exception e) {
      throw new RetryableException(e);
    }
  }

  private void saveAttemptSyncConfig(final long jobId, final int attemptNumber, final UUID connectionId, final AttemptSyncConfig attemptSyncConfig) {

    attemptHandler.saveSyncConfig(new SaveAttemptSyncConfigRequestBody()
        .jobId(jobId)
        .attemptNumber(attemptNumber)
        .syncConfig(ApiPojoConverters.attemptSyncConfigToApi(attemptSyncConfig, connectionId)));
  }

  /**
   * Returns a Job's JobSyncConfig, converting it from a JobResetConnectionConfig if necessary.
   */
  private JobSyncConfig getJobSyncConfig(final long jobId, final JobConfig jobConfig) {
    final JobConfig.ConfigType jobConfigType = jobConfig.getConfigType();
    if (JobConfig.ConfigType.SYNC.equals(jobConfigType)) {
      return jobConfig.getSync();
    } else if (JobConfig.ConfigType.RESET_CONNECTION.equals(jobConfigType)) {
      final JobResetConnectionConfig resetConnection = jobConfig.getResetConnection();

      return new JobSyncConfig()
          .withNamespaceDefinition(resetConnection.getNamespaceDefinition())
          .withNamespaceFormat(resetConnection.getNamespaceFormat())
          .withPrefix(resetConnection.getPrefix())
          .withSourceDockerImage(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB)
          .withDestinationDockerImage(resetConnection.getDestinationDockerImage())
          .withDestinationProtocolVersion(resetConnection.getDestinationProtocolVersion())
          .withConfiguredAirbyteCatalog(resetConnection.getConfiguredAirbyteCatalog())
          .withOperationSequence(resetConnection.getOperationSequence())
          .withSyncResourceRequirements(resetConnection.getSyncResourceRequirements())
          .withIsSourceCustomConnector(resetConnection.getIsSourceCustomConnector())
          .withIsDestinationCustomConnector(resetConnection.getIsDestinationCustomConnector())
          .withWebhookOperationConfigs(resetConnection.getWebhookOperationConfigs())
          .withWorkspaceId(resetConnection.getWorkspaceId());
    } else if (JobConfig.ConfigType.REFRESH.equals(jobConfigType)) {
      final RefreshConfig refreshConfig = jobConfig.getRefresh();

      return new JobSyncConfig()
          .withNamespaceDefinition(refreshConfig.getNamespaceDefinition())
          .withNamespaceFormat(refreshConfig.getNamespaceFormat())
          .withPrefix(refreshConfig.getPrefix())
          .withSourceDockerImage(refreshConfig.getSourceDockerImage())
          .withSourceProtocolVersion(refreshConfig.getSourceProtocolVersion())
          .withSourceDefinitionVersionId(refreshConfig.getSourceDefinitionVersionId())
          .withDestinationDockerImage(refreshConfig.getDestinationDockerImage())
          .withDestinationProtocolVersion(refreshConfig.getDestinationProtocolVersion())
          .withDestinationDefinitionVersionId(refreshConfig.getDestinationDefinitionVersionId())
          .withConfiguredAirbyteCatalog(refreshConfig.getConfiguredAirbyteCatalog())
          .withOperationSequence(refreshConfig.getOperationSequence())
          .withSyncResourceRequirements(refreshConfig.getSyncResourceRequirements())
          .withIsSourceCustomConnector(refreshConfig.getIsSourceCustomConnector())
          .withIsDestinationCustomConnector(refreshConfig.getIsDestinationCustomConnector())
          .withWebhookOperationConfigs(refreshConfig.getWebhookOperationConfigs())
          .withWorkspaceId(refreshConfig.getWorkspaceId());
    } else {
      throw new IllegalStateException(
          String.format("Unexpected config type %s for job %d. The only supported config types for this activity are (%s)",
              jobConfigType,
              jobId,
              REPLICATION_TYPES));
    }
  }

  private Optional<State> getCurrentConnectionState(final UUID connectionId) throws IOException {
    final ConnectionState state = stateHandler.getState(new ConnectionIdRequestBody().connectionId(connectionId));

    if (state.getStateType() == ConnectionStateType.NOT_SET) {
      return Optional.empty();
    }

    final StateWrapper internalState = StateConverter.toInternal(state);
    return Optional.of(StateMessageHelper.getState(internalState));
  }

  private IntegrationLauncherConfig getSourceIntegrationLauncherConfig(final long jobId,
                                                                       final int attempt,
                                                                       final UUID connectionId,
                                                                       final JobSyncConfig config,
                                                                       @Nullable final ActorDefinitionVersion sourceVersion,
                                                                       final JsonNode sourceConfiguration)
      throws IOException {
    final ConfigReplacer configReplacer = new ConfigReplacer(LOGGER);

    final IntegrationLauncherConfig sourceLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(jobId))
        .withAttemptId((long) attempt)
        .withConnectionId(connectionId)
        .withWorkspaceId(config.getWorkspaceId())
        .withDockerImage(config.getSourceDockerImage())
        .withProtocolVersion(config.getSourceProtocolVersion())
        .withIsCustomConnector(config.getIsSourceCustomConnector());

    if (sourceVersion != null) {
      sourceLauncherConfig.setAllowedHosts(configReplacer.getAllowedHosts(sourceVersion.getAllowedHosts(), sourceConfiguration));
    }

    return sourceLauncherConfig;
  }

  private IntegrationLauncherConfig getDestinationIntegrationLauncherConfig(final long jobId,
                                                                            final int attempt,
                                                                            final UUID connectionId,
                                                                            final JobSyncConfig config,
                                                                            final ActorDefinitionVersion destinationVersion,
                                                                            final JsonNode destinationConfiguration,
                                                                            final Map<String, String> additionalEnviornmentVariables)
      throws IOException {
    final ConfigReplacer configReplacer = new ConfigReplacer(LOGGER);

    return new IntegrationLauncherConfig()
        .withJobId(String.valueOf(jobId))
        .withAttemptId((long) attempt)
        .withConnectionId(connectionId)
        .withWorkspaceId(config.getWorkspaceId())
        .withDockerImage(config.getDestinationDockerImage())
        .withProtocolVersion(config.getDestinationProtocolVersion())
        .withIsCustomConnector(config.getIsDestinationCustomConnector())
        .withAllowedHosts(configReplacer.getAllowedHosts(destinationVersion.getAllowedHosts(), destinationConfiguration))
        .withAdditionalEnvironmentVariables(additionalEnviornmentVariables);
  }

  private JsonNode getSourceConfiguration(final SourceConnection source) throws IOException {
    return configInjector.injectConfig(oAuthConfigSupplier.injectSourceOAuthParameters(
        source.getSourceDefinitionId(),
        source.getSourceId(),
        source.getWorkspaceId(),
        source.getConfiguration()), source.getSourceDefinitionId());
  }

  private JsonNode getDestinationConfiguration(final DestinationConnection destination) throws IOException {
    return configInjector.injectConfig(oAuthConfigSupplier.injectDestinationOAuthParameters(
        destination.getDestinationDefinitionId(),
        destination.getDestinationId(),
        destination.getWorkspaceId(),
        destination.getConfiguration()), destination.getDestinationDefinitionId());
  }

}
