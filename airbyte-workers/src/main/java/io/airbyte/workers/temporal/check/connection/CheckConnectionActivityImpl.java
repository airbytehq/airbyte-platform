/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ScopeType;
import io.airbyte.api.client.model.generated.SecretPersistenceConfig;
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.temporal.HeartbeatUtils;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.general.DefaultCheckConnectionWorker;
import io.airbyte.workers.helpers.SecretPersistenceConfigHelper;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Check connection activity temporal implementation for the control plane.
 */
@Singleton
public class CheckConnectionActivityImpl implements CheckConnectionActivity {

  private final WorkerConfigsProvider workerConfigsProvider;
  private final ProcessFactory processFactory;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final AirbyteApiClient airbyteApiClient;
  private final String airbyteVersion;
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final FeatureFlags featureFlags;
  private final FeatureFlagClient featureFlagClient;

  public CheckConnectionActivityImpl(final WorkerConfigsProvider workerConfigsProvider,
                                     final ProcessFactory processFactory,
                                     final SecretsRepositoryReader secretsRepositoryReader,
                                     @Named("workspaceRoot") final Path workspaceRoot,
                                     final WorkerEnvironment workerEnvironment,
                                     final LogConfigs logConfigs,
                                     final AirbyteApiClient airbyteApiClient,
                                     @Value("${airbyte.version}") final String airbyteVersion,
                                     final AirbyteMessageSerDeProvider serDeProvider,
                                     final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                     final FeatureFlags featureFlags,
                                     final FeatureFlagClient featureFlagClient) {
    this.workerConfigsProvider = workerConfigsProvider;
    this.processFactory = processFactory;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteApiClient = airbyteApiClient;
    this.airbyteVersion = airbyteVersion;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
    this.featureFlagClient = featureFlagClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithJobOutput(final CheckConnectionInput args) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_CHECK_CONNECTION, 1);

    ApmTraceUtils
        .addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, args.getJobRunConfig().getAttemptId(), JOB_ID_KEY, args.getJobRunConfig().getJobId(),
            DOCKER_IMAGE_KEY, args.getLauncherConfig().getDockerImage()));
    final StandardCheckConnectionInput rawInput = args.getConnectionConfiguration();
    final JsonNode fullConfig;
    final UUID organizationId = rawInput.getActorContext().getOrganizationId();

    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      try {
        final SecretPersistenceConfig secretPersistenceConfig = airbyteApiClient.getSecretPersistenceConfigApi().getSecretsPersistenceConfig(
            new SecretPersistenceConfigGetRequestBody().scopeType(ScopeType.ORGANIZATION).scopeId(organizationId));
        final RuntimeSecretPersistence runtimeSecretPersistence =
            SecretPersistenceConfigHelper.fromApiSecretPersistenceConfig(secretPersistenceConfig);
        fullConfig =
            secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(rawInput.getConnectionConfiguration(), runtimeSecretPersistence);
      } catch (final ApiException e) {
        throw new RuntimeException(e);
      }
    } else {
      fullConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(rawInput.getConnectionConfiguration());
    }

    final StandardCheckConnectionInput input = new StandardCheckConnectionInput()
        .withActorId(rawInput.getActorId())
        .withActorType(rawInput.getActorType())
        .withConnectionConfiguration(fullConfig)
        .withActorContext(rawInput.getActorContext());

    final ActivityExecutionContext context = Activity.getExecutionContext();
    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return HeartbeatUtils.withBackgroundHeartbeat(cancellationCallback,
        () -> {
          final var worker = getWorkerFactory(args.getLauncherConfig(), rawInput.getResourceRequirements()).get();
          cancellationCallback.set(worker::cancel);
          final TemporalAttemptExecution<StandardCheckConnectionInput, ConnectorJobOutput> temporalAttemptExecution =
              new TemporalAttemptExecution<>(
                  workspaceRoot,
                  workerEnvironment,
                  logConfigs,
                  args.getJobRunConfig(),
                  worker,
                  input,
                  airbyteApiClient,
                  airbyteVersion,
                  () -> context);
          return temporalAttemptExecution.get();
        },
        context);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public StandardCheckConnectionOutput run(final CheckConnectionInput args) {
    ApmTraceUtils
        .addTagsToTrace(Map.of(JOB_ID_KEY, args.getJobRunConfig().getJobId(), DOCKER_IMAGE_KEY, args.getLauncherConfig().getDockerImage()));
    final ConnectorJobOutput output = runWithJobOutput(args);
    if (output.getFailureReason() != null) {
      return new StandardCheckConnectionOutput().withStatus(Status.FAILED).withMessage("Error checking connection");
    }

    return output.getCheckConnection();
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<StandardCheckConnectionInput, ConnectorJobOutput>, Exception> getWorkerFactory(
                                                                                                                final IntegrationLauncherConfig launcherConfig,
                                                                                                                final ResourceRequirements actorDefinitionResourceRequirements) {
    return () -> {
      final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.CHECK);
      final ResourceRequirements defaultWorkerConfigResourceRequirements = workerConfigs.getResourceRequirements();

      final IntegrationLauncher integrationLauncher = new AirbyteIntegrationLauncher(
          launcherConfig.getJobId(),
          Math.toIntExact(launcherConfig.getAttemptId()),
          launcherConfig.getConnectionId(),
          launcherConfig.getWorkspaceId(),
          launcherConfig.getDockerImage(),
          processFactory,
          ResourceRequirementsUtils.getResourceRequirements(actorDefinitionResourceRequirements, defaultWorkerConfigResourceRequirements),
          null,
          launcherConfig.getAllowedHosts(),
          launcherConfig.getIsCustomConnector(),
          featureFlags,
          Collections.emptyMap(),
          Collections.emptyMap());

      final ConnectorConfigUpdater connectorConfigUpdater = new ConnectorConfigUpdater(
          airbyteApiClient.getSourceApi(),
          airbyteApiClient.getDestinationApi());

      final var protocolVersion =
          launcherConfig.getProtocolVersion() != null ? launcherConfig.getProtocolVersion() : AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;
      final AirbyteStreamFactory streamFactory =
          new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, protocolVersion, Optional.empty(), Optional.empty(), false);

      return new DefaultCheckConnectionWorker(integrationLauncher, connectorConfigUpdater, streamFactory);
    };
  }

}
