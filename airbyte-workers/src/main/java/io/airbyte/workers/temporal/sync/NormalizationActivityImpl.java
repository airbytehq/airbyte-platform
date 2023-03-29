/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.CancellationHandler;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.WorkerConfigs;
import io.airbyte.workers.general.DefaultNormalizationWorker;
import io.airbyte.workers.normalization.DefaultNormalizationRunner;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.NormalizationLauncherWorker;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Normalization temporal activity impl.
 */
@Singleton
@Slf4j
public class NormalizationActivityImpl implements NormalizationActivity {

  private final Optional<ContainerOrchestratorConfig> containerOrchestratorConfig;
  private final WorkerConfigs workerConfigs;
  private final ProcessFactory processFactory;
  private final SecretsHydrator secretsHydrator;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final String airbyteVersion;
  private final Integer serverPort;
  private final AirbyteConfigValidator airbyteConfigValidator;
  private final TemporalUtils temporalUtils;
  private final ResourceRequirements normalizationResourceRequirements;
  private final AirbyteApiClient airbyteApiClient;

  public NormalizationActivityImpl(@Named("containerOrchestratorConfig") final Optional<ContainerOrchestratorConfig> containerOrchestratorConfig,
                                   @Named("defaultWorkerConfigs") final WorkerConfigs workerConfigs,
                                   @Named("defaultProcessFactory") final ProcessFactory processFactory,
                                   final SecretsHydrator secretsHydrator,
                                   @Named("workspaceRoot") final Path workspaceRoot,
                                   final WorkerEnvironment workerEnvironment,
                                   final LogConfigs logConfigs,
                                   @Value("${airbyte.version}") final String airbyteVersion,
                                   @Value("${micronaut.server.port}") final Integer serverPort,
                                   final AirbyteConfigValidator airbyteConfigValidator,
                                   final TemporalUtils temporalUtils,
                                   @Named("normalizationResourceRequirements") final ResourceRequirements normalizationResourceRequirements,
                                   final AirbyteApiClient airbyteApiClient) {
    this.containerOrchestratorConfig = containerOrchestratorConfig;
    this.workerConfigs = workerConfigs;
    this.processFactory = processFactory;
    this.secretsHydrator = secretsHydrator;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteVersion = airbyteVersion;
    this.serverPort = serverPort;
    this.airbyteConfigValidator = airbyteConfigValidator;
    this.temporalUtils = temporalUtils;
    this.normalizationResourceRequirements = normalizationResourceRequirements;
    this.airbyteApiClient = airbyteApiClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public NormalizationSummary normalize(final JobRunConfig jobRunConfig,
                                        final IntegrationLauncherConfig destinationLauncherConfig,
                                        final NormalizationInput input) {
    ApmTraceUtils.addTagsToTrace(
        Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), JOB_ID_KEY, jobRunConfig.getJobId(), DESTINATION_DOCKER_IMAGE_KEY,
            destinationLauncherConfig.getDockerImage()));
    final ActivityExecutionContext context = Activity.getExecutionContext();
    return temporalUtils.withBackgroundHeartbeat(() -> {
      final var fullDestinationConfig = secretsHydrator.hydrate(input.getDestinationConfiguration());
      final var fullInput = Jsons.clone(input).withDestinationConfiguration(fullDestinationConfig);

      final Supplier<NormalizationInput> inputSupplier = () -> {
        airbyteConfigValidator.ensureAsRuntime(ConfigSchema.NORMALIZATION_INPUT, Jsons.jsonNode(fullInput));
        return fullInput;
      };

      final CheckedSupplier<Worker<NormalizationInput, NormalizationSummary>, Exception> workerFactory;

      log.info("Using normalization: " + destinationLauncherConfig.getNormalizationDockerImage());
      if (containerOrchestratorConfig.isPresent()) {
        workerFactory = getContainerLauncherWorkerFactory(workerConfigs, destinationLauncherConfig, jobRunConfig,
            () -> context, input.getConnectionId());
      } else {
        workerFactory = getLegacyWorkerFactory(destinationLauncherConfig, jobRunConfig);
      }

      final TemporalAttemptExecution<NormalizationInput, NormalizationSummary> temporalAttemptExecution = new TemporalAttemptExecution<>(
          workspaceRoot, workerEnvironment, logConfigs,
          jobRunConfig,
          workerFactory,
          inputSupplier,
          new CancellationHandler.TemporalCancellationHandler(context),
          airbyteApiClient,
          airbyteVersion,
          () -> context);

      return temporalAttemptExecution.get();
    },
        () -> context);
  }

  /**
   * This activity is deprecated. It is using a big payload which is not needed, it has been replace
   * by generateNormalizationInputWithMinimumPayload
   *
   * @param syncInput sync input
   * @param syncOutput sync output
   * @return normalization output
   */
  @SuppressWarnings("InvalidJavadocPosition")
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  @Deprecated(forRemoval = true)
  public NormalizationInput generateNormalizationInput(final StandardSyncInput syncInput, final StandardSyncOutput syncOutput) {
    return new NormalizationInput()
        .withConnectionId(syncInput.getConnectionId())
        .withDestinationConfiguration(syncInput.getDestinationConfiguration())
        .withCatalog(syncOutput.getOutputCatalog())
        .withResourceRequirements(normalizationResourceRequirements)
        .withWorkspaceId(syncInput.getWorkspaceId());
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public NormalizationInput generateNormalizationInputWithMinimumPayload(final JsonNode destinationConfiguration,
                                                                         final ConfiguredAirbyteCatalog airbyteCatalog,
                                                                         final UUID workspaceId) {
    return new NormalizationInput()
        .withDestinationConfiguration(destinationConfiguration)
        .withCatalog(airbyteCatalog)
        .withResourceRequirements(normalizationResourceRequirements)
        .withWorkspaceId(workspaceId);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public NormalizationInput generateNormalizationInputWithMinimumPayloadWithConnectionId(final JsonNode destinationConfiguration,
                                                                                         final ConfiguredAirbyteCatalog airbyteCatalog,
                                                                                         final UUID workspaceId,
                                                                                         final UUID connectionId) {
    return new NormalizationInput()
        .withConnectionId(connectionId)
        .withDestinationConfiguration(destinationConfiguration)
        .withCatalog(airbyteCatalog)
        .withResourceRequirements(normalizationResourceRequirements)
        .withWorkspaceId(workspaceId);
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<NormalizationInput, NormalizationSummary>, Exception> getLegacyWorkerFactory(
                                                                                                              final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                                              final JobRunConfig jobRunConfig) {
    return () -> new DefaultNormalizationWorker(
        jobRunConfig.getJobId(),
        Math.toIntExact(jobRunConfig.getAttemptId()),
        new DefaultNormalizationRunner(
            processFactory,
            destinationLauncherConfig.getNormalizationDockerImage(),
            destinationLauncherConfig.getNormalizationIntegrationType()),
        workerEnvironment);
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<NormalizationInput, NormalizationSummary>, Exception> getContainerLauncherWorkerFactory(
                                                                                                                         final WorkerConfigs workerConfigs,
                                                                                                                         final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                                                         final JobRunConfig jobRunConfig,
                                                                                                                         final Supplier<ActivityExecutionContext> activityContext,
                                                                                                                         final UUID connectionId) {
    return () -> new NormalizationLauncherWorker(
        connectionId,
        destinationLauncherConfig,
        jobRunConfig,
        workerConfigs,
        containerOrchestratorConfig.get(),
        activityContext,
        serverPort,
        temporalUtils);
  }

}
