/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateType;
import io.airbyte.api.client.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.helper.NormalizationInDestinationHelper;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.temporal.TemporalWorkflowUtils;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.ResetSourceConfiguration;
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
import io.airbyte.workers.WorkerConstants;
import io.airbyte.workers.utils.ConfigReplacer;
import io.micronaut.context.annotation.Requires;
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
 * Generate input for a workflow.
 */
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class GenerateInputActivityImpl implements GenerateInputActivity {

  private final JobPersistence jobPersistence;
  private final ConfigRepository configRepository;
  private final AttemptApi attemptApi;
  private final StateApi stateApi;
  private final FeatureFlags featureFlags;
  private final FeatureFlagClient featureFlagClient;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private final ConfigInjector configInjector;

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateInputActivity.class);

  @SuppressWarnings("ParameterName")
  public GenerateInputActivityImpl(final JobPersistence jobPersistence,
                                   final ConfigRepository configRepository,
                                   final StateApi stateApi,
                                   final AttemptApi attemptApi,
                                   final FeatureFlags featureFlags,
                                   final FeatureFlagClient featureFlagClient,
                                   final OAuthConfigSupplier oAuthConfigSupplier,
                                   final ConfigInjector configInjector,
                                   final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.stateApi = stateApi;
    this.attemptApi = attemptApi;
    this.configInjector = configInjector;
    this.featureFlags = featureFlags;
    this.featureFlagClient = featureFlagClient;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
  }

  private Optional<State> getCurrentConnectionState(final UUID connectionId) {
    final ConnectionState state = AirbyteApiClient.retryWithJitter(
        () -> stateApi.getState(new ConnectionIdRequestBody().connectionId(connectionId)),
        "get state");

    if (state.getStateType() == ConnectionStateType.NOT_SET) {
      return Optional.empty();
    }

    final StateWrapper internalState = StateConverter.clientToInternal(state);
    return Optional.of(StateMessageHelper.getState(internalState));
  }

  private void saveAttemptSyncConfig(final long jobId, final int attemptNumber, final UUID connectionId, final AttemptSyncConfig attemptSyncConfig) {
    AirbyteApiClient.retryWithJitter(
        () -> attemptApi.saveSyncConfig(new SaveAttemptSyncConfigRequestBody()
            .jobId(jobId)
            .attemptNumber(attemptNumber)
            .syncConfig(ApiPojoConverters.attemptSyncConfigToClient(attemptSyncConfig, connectionId, featureFlags.useStreamCapableState()))),
        "set attempt sync config");
  }

  private IntegrationLauncherConfig getSourceIntegrationLauncherConfig(final long jobId,
                                                                       final int attempt,
                                                                       final JobSyncConfig config,
                                                                       @Nullable final ActorDefinitionVersion sourceVersion,
                                                                       final JsonNode sourceConfiguration)
      throws IOException {
    final ConfigReplacer configReplacer = new ConfigReplacer(LOGGER);

    final IntegrationLauncherConfig sourceLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(jobId))
        .withAttemptId((long) attempt)
        .withDockerImage(config.getSourceDockerImage())
        .withProtocolVersion(config.getSourceProtocolVersion())
        .withIsCustomConnector(config.getIsSourceCustomConnector());

    if (sourceVersion != null) {
      sourceLauncherConfig.setAllowedHosts(configReplacer.getAllowedHosts(sourceVersion.getAllowedHosts(), sourceConfiguration));
    }

    return sourceLauncherConfig;
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

  private IntegrationLauncherConfig getDestinationIntegrationLauncherConfig(final long jobId,
                                                                            final int attempt,
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
        .withDockerImage(config.getDestinationDockerImage())
        .withProtocolVersion(config.getDestinationProtocolVersion())
        .withIsCustomConnector(config.getIsDestinationCustomConnector())
        .withNormalizationDockerImage(destinationNormalizationDockerImage)
        .withSupportsDbt(destinationVersion.getSupportsDbt())
        .withNormalizationIntegrationType(normalizationIntegrationType)
        .withAllowedHosts(configReplacer.getAllowedHosts(destinationVersion.getAllowedHosts(), destinationConfiguration))
        .withAdditionalEnvironmentVariables(additionalEnviornmentVariables);
  }

  /**
   * Returns a Job's JobSyncConfig, converting it from a JobResetConnectionConfig if necessary.
   */
  private JobSyncConfig getJobSyncConfig(final long jobId, final JobConfig jobConfig) {
    final ConfigType jobConfigType = jobConfig.getConfigType();
    if (ConfigType.SYNC.equals(jobConfigType)) {
      return jobConfig.getSync();
    } else if (ConfigType.RESET_CONNECTION.equals(jobConfigType)) {
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
          .withResourceRequirements(resetConnection.getResourceRequirements())
          .withIsSourceCustomConnector(resetConnection.getIsSourceCustomConnector())
          .withIsDestinationCustomConnector(resetConnection.getIsDestinationCustomConnector())
          .withWebhookOperationConfigs(resetConnection.getWebhookOperationConfigs())
          .withWorkspaceId(resetConnection.getWorkspaceId());
    } else {
      throw new IllegalStateException(
          String.format("Unexpected config type %s for job %d. The only supported config types for this activity are (%s)",
              jobConfigType,
              jobId,
              List.of(ConfigType.SYNC, ConfigType.RESET_CONNECTION)));
    }
  }

  @Override
  public SyncJobCheckConnectionInputs getCheckConnectionInputs(final SyncInputWithAttemptNumber input) {
    final long jobId = input.getJobId();
    final int attemptNumber = input.getAttemptNumber();

    try {
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
          jobSyncConfig,
          sourceVersion,
          sourceConfiguration);

      final IntegrationLauncherConfig destinationLauncherConfig =
          getDestinationIntegrationLauncherConfig(
              jobId,
              attemptNumber,
              jobSyncConfig,
              destinationVersion,
              destinationConfiguration,
              Collections.emptyMap());

      final StandardCheckConnectionInput sourceCheckConnectionInput = new StandardCheckConnectionInput()
          .withActorType(ActorType.SOURCE)
          .withActorId(source.getSourceId())
          .withConnectionConfiguration(sourceConfiguration);

      final StandardCheckConnectionInput destinationCheckConnectionInput = new StandardCheckConnectionInput()
          .withActorType(ActorType.DESTINATION)
          .withActorId(destination.getDestinationId())
          .withConnectionConfiguration(destinationConfiguration);

      return new SyncJobCheckConnectionInputs(
          sourceLauncherConfig,
          destinationLauncherConfig,
          sourceCheckConnectionInput,
          destinationCheckConnectionInput);

    } catch (final Exception e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public GeneratedJobInput getSyncWorkflowInput(final SyncInput input) {
    try {
      ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, input.getAttemptId(), JOB_ID_KEY, input.getJobId()));
      final long jobId = input.getJobId();
      final int attempt = input.getAttemptId();

      final Job job = jobPersistence.getJob(jobId);
      final JobSyncConfig config = getJobSyncConfig(jobId, job.getConfig());

      final UUID connectionId = UUID.fromString(job.getScope());
      final StandardSync standardSync = configRepository.getStandardSync(connectionId);

      final AttemptSyncConfig attemptSyncConfig = new AttemptSyncConfig();
      getCurrentConnectionState(connectionId).ifPresent(attemptSyncConfig::setState);

      final ConfigType jobConfigType = job.getConfig().getConfigType();

      ActorDefinitionVersion sourceVersion = null;

      if (ConfigType.SYNC.equals(jobConfigType)) {
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
      } else if (ConfigType.RESET_CONNECTION.equals(jobConfigType)) {
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
          config,
          sourceVersion,
          attemptSyncConfig.getSourceConfiguration());

      final IntegrationLauncherConfig destinationLauncherConfig = getDestinationIntegrationLauncherConfig(
          jobId,
          attempt,
          config,
          destinationVersion,
          attemptSyncConfig.getDestinationConfiguration(),
          NormalizationInDestinationHelper.getAdditionalEnvironmentVariables(shouldNormalizeInDestination));

      final List<Context> featureFlagContext = new ArrayList<>();
      featureFlagContext.add(new Workspace(config.getWorkspaceId()));
      if (standardSync.getConnectionId() != null) {
        featureFlagContext.add(new Connection(standardSync.getConnectionId()));
      }

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
          .withResourceRequirements(config.getResourceRequirements())
          .withSourceResourceRequirements(config.getSourceResourceRequirements())
          .withDestinationResourceRequirements(config.getDestinationResourceRequirements())
          .withConnectionId(standardSync.getConnectionId())
          .withWorkspaceId(config.getWorkspaceId())
          .withNormalizeInDestinationContainer(shouldNormalizeInDestination)
          .withIsReset(ConfigType.RESET_CONNECTION.equals(jobConfigType));

      saveAttemptSyncConfig(jobId, attempt, connectionId, attemptSyncConfig);

      return new GeneratedJobInput(jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, syncInput);
    } catch (final Exception e) {
      throw new RetryableException(e);
    }
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

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public GeneratedJobInput getSyncWorkflowInputWithAttemptNumber(final SyncInputWithAttemptNumber input) {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, input.getJobId()));
    return getSyncWorkflowInput(new SyncInput(
        input.getAttemptNumber(),
        input.getJobId()));
  }

}
