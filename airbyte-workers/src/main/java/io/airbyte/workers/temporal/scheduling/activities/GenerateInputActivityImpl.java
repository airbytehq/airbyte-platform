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
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.converters.ConfigReplacer;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.workers.models.JobInput;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
  private final JobsApi jobsApi;

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
                                   final JobsApi jobsApi,
                                   final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.stateApi = stateApi;
    this.attemptApi = attemptApi;
    this.configInjector = configInjector;
    this.featureFlags = featureFlags;
    this.featureFlagClient = featureFlagClient;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.jobsApi = jobsApi;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
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
  public JobInput getSyncWorkflowInput(final SyncInput input) throws Exception {
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, input.getAttemptId(), JOB_ID_KEY, input.getJobId()));
    return Jsons.convertValue(AirbyteApiClient.retryWithJitterThrows(
        () -> jobsApi.getJobInput(new io.airbyte.api.client.model.generated.SyncInput().jobId(input.getJobId())
            .attemptNumber(input.getAttemptId())),
        "Create job input."), JobInput.class);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public JobInput getSyncWorkflowInputWithAttemptNumber(final SyncInputWithAttemptNumber input) throws Exception {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, input.getJobId()));
    return getSyncWorkflowInput(new SyncInput(
        input.getAttemptNumber(),
        input.getJobId()));
  }

}
