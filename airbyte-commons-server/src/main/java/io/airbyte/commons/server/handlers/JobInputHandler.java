/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

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
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.helper.NormalizationInDestinationHelper;
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
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.JobTypeResourceLimit.JobType;
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
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.NormalizationInDestination;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler that allow to fetch a job input.
 */
@Singleton
public class JobInputHandler {

  private final JobPersistence jobPersistence;
  private final ConfigRepository configRepository;
  private final FeatureFlags featureFlags;
  private final FeatureFlagClient featureFlagClient;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ConfigInjector configInjector;
  private final AttemptHandler attemptHandler;
  private final StateHandler stateHandler;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final ContextBuilder contextBuilder;

  private static final Logger LOGGER = LoggerFactory.getLogger(JobInputHandler.class);

  @SuppressWarnings("ParameterName")
  public JobInputHandler(final JobPersistence jobPersistence,
                         final ConfigRepository configRepository,
                         final FeatureFlags featureFlags,
                         final FeatureFlagClient featureFlagClient,
                         final OAuthConfigSupplier oAuthConfigSupplier,
                         final ConfigInjector configInjector,
                         final AttemptHandler attemptHandler,
                         final StateHandler stateHandler,
                         final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                         final ContextBuilder contextBuilder) {
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.featureFlags = featureFlags;
    this.featureFlagClient = featureFlagClient;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.configInjector = configInjector;
    this.attemptHandler = attemptHandler;
    this.stateHandler = stateHandler;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.contextBuilder = contextBuilder;
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
      final StandardSync standardSync = configRepository.getStandardSync(connectionId);

      final AttemptSyncConfig attemptSyncConfig = new AttemptSyncConfig();
      getCurrentConnectionState(connectionId).ifPresent(attemptSyncConfig::setState);

      final JobConfig.ConfigType jobConfigType = job.getConfig().getConfigType();

      if (JobConfig.ConfigType.SYNC.equals(jobConfigType)) {
        final SourceConnection source = configRepository.getSourceConnection(standardSync.getSourceId());
        sourceVersion = actorDefinitionVersionHelper.getSourceVersion(
            configRepository.getStandardSourceDefinition(source.getSourceDefinitionId()),
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

      final DestinationConnection destination = configRepository.getDestinationConnection(standardSync.getDestinationId());
      final ActorDefinitionVersion destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(
              configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId()),
              destination.getWorkspaceId(),
              destination.getDestinationId());
      final JsonNode destinationConfiguration = oAuthConfigSupplier.injectDestinationOAuthParameters(
          destination.getDestinationDefinitionId(),
          destination.getDestinationId(),
          destination.getWorkspaceId(),
          destination.getConfiguration());
      attemptSyncConfig.setDestinationConfiguration(configInjector.injectConfig(destinationConfiguration, destination.getDestinationDefinitionId()));

      final List<Context> normalizationInDestinationContext = List.of(
          new DestinationDefinition(destination.getDestinationDefinitionId()),
          new Workspace(destination.getWorkspaceId()));

      final var normalizationInDestinationMinSupportedVersion = featureFlagClient.stringVariation(
          NormalizationInDestination.INSTANCE, new Multi(normalizationInDestinationContext));
      final var shouldNormalizeInDestination = NormalizationInDestinationHelper
          .shouldNormalizeInDestination(config.getOperationSequence(),
              config.getDestinationDockerImage(),
              normalizationInDestinationMinSupportedVersion);

      reportNormalizationInDestinationMetrics(shouldNormalizeInDestination, config, connectionId);

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
          NormalizationInDestinationHelper.getAdditionalEnvironmentVariables(shouldNormalizeInDestination));

      final List<Context> featureFlagContext = new ArrayList<>();
      featureFlagContext.add(new Workspace(config.getWorkspaceId()));
      if (standardSync.getConnectionId() != null) {
        featureFlagContext.add(new Connection(standardSync.getConnectionId()));
      }

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
          .withCatalog(config.getConfiguredAirbyteCatalog())
          .withState(attemptSyncConfig.getState())
          .withSyncResourceRequirements(config.getSyncResourceRequirements())
          .withConnectionId(connectionId)
          .withWorkspaceId(config.getWorkspaceId())
          .withNormalizeInDestinationContainer(shouldNormalizeInDestination)
          .withIsReset(JobConfig.ConfigType.RESET_CONNECTION.equals(jobConfigType))
          .withConnectionContext(connectionContext);

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
      final StandardSync standardSync = configRepository.getStandardSync(connectionId);

      final DestinationConnection destination = configRepository.getDestinationConnection(standardSync.getDestinationId());
      final StandardDestinationDefinition destinationDefinition =
          configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
      final ActorDefinitionVersion destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), destination.getDestinationId());

      final SourceConnection source = configRepository.getSourceConnection(standardSync.getSourceId());
      final StandardSourceDefinition sourceDefinition =
          configRepository.getStandardSourceDefinition(source.getSourceDefinitionId());
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
    } else {
      throw new IllegalStateException(
          String.format("Unexpected config type %s for job %d. The only supported config types for this activity are (%s)",
              jobConfigType,
              jobId,
              List.of(JobConfig.ConfigType.SYNC, JobConfig.ConfigType.RESET_CONNECTION)));
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

  private void reportNormalizationInDestinationMetrics(final boolean shouldNormalizeInDestination,
                                                       final JobSyncConfig config,
                                                       final UUID connectionId) {
    if (shouldNormalizeInDestination) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NORMALIZATION_IN_DESTINATION_CONTAINER, 1,
          new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    } else if (NormalizationInDestinationHelper.normalizationStepRequired(config.getOperationSequence())) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NORMALIZATION_IN_NORMALIZATION_CONTAINER, 1,
          new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    }
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
    final String destinationNormalizationDockerImage = destinationVersion.getNormalizationConfig() != null
        ? destinationVersion.getNormalizationConfig().getNormalizationRepository() + ":"
            + destinationVersion.getNormalizationConfig().getNormalizationTag()
        : null;
    final String normalizationIntegrationType =
        destinationVersion.getNormalizationConfig() != null ? destinationVersion.getNormalizationConfig().getNormalizationIntegrationType()
            : null;

    return new IntegrationLauncherConfig()
        .withJobId(String.valueOf(jobId))
        .withAttemptId((long) attempt)
        .withConnectionId(connectionId)
        .withWorkspaceId(config.getWorkspaceId())
        .withDockerImage(config.getDestinationDockerImage())
        .withProtocolVersion(config.getDestinationProtocolVersion())
        .withIsCustomConnector(config.getIsDestinationCustomConnector())
        .withNormalizationDockerImage(destinationNormalizationDockerImage)
        .withSupportsDbt(destinationVersion.getSupportsDbt())
        .withNormalizationIntegrationType(normalizationIntegrationType)
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
