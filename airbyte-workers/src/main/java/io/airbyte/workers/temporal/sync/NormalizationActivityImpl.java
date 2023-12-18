/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ScopeType;
import io.airbyte.api.client.model.generated.SecretPersistenceConfig;
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.migrations.v1.CatalogMigrationV1Helper;
import io.airbyte.commons.temporal.HeartbeatUtils;
import io.airbyte.commons.version.Version;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.general.DefaultNormalizationWorker;
import io.airbyte.workers.helper.SecretPersistenceConfigHelper;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.normalization.DefaultNormalizationRunner;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.NormalizationLauncherWorker;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Normalization temporal activity impl.
 */
@Singleton
@Slf4j
public class NormalizationActivityImpl implements NormalizationActivity {

  private final Optional<ContainerOrchestratorConfig> containerOrchestratorConfig;
  private final WorkerConfigsProvider workerConfigsProvider;
  private final ProcessFactory processFactory;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final String airbyteVersion;
  private final Integer serverPort;
  private final AirbyteConfigValidator airbyteConfigValidator;
  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;
  private final WorkloadIdGenerator workloadIdGenerator;

  private static final String V1_NORMALIZATION_MINOR_VERSION = "3";

  public NormalizationActivityImpl(@Named("containerOrchestratorConfig") final Optional<ContainerOrchestratorConfig> containerOrchestratorConfig,
                                   final WorkerConfigsProvider workerConfigsProvider,
                                   final ProcessFactory processFactory,
                                   final SecretsRepositoryReader secretsRepositoryReader,
                                   @Named("workspaceRoot") final Path workspaceRoot,
                                   final WorkerEnvironment workerEnvironment,
                                   final LogConfigs logConfigs,
                                   @Value("${airbyte.version}") final String airbyteVersion,
                                   @Value("${micronaut.server.port}") final Integer serverPort,
                                   final AirbyteConfigValidator airbyteConfigValidator,
                                   final AirbyteApiClient airbyteApiClient,
                                   final FeatureFlagClient featureFlagClient,
                                   final MetricClient metricClient,
                                   final WorkloadIdGenerator workloadIdGenerator) {
    this.containerOrchestratorConfig = containerOrchestratorConfig;
    this.workerConfigsProvider = workerConfigsProvider;
    this.processFactory = processFactory;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteVersion = airbyteVersion;
    this.serverPort = serverPort;
    this.airbyteConfigValidator = airbyteConfigValidator;
    this.airbyteApiClient = airbyteApiClient;
    this.featureFlagClient = featureFlagClient;
    this.metricClient = metricClient;
    this.workloadIdGenerator = workloadIdGenerator;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public NormalizationSummary normalize(final JobRunConfig jobRunConfig,
                                        final IntegrationLauncherConfig destinationLauncherConfig,
                                        final NormalizationInput input) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_NORMALIZATION, 1);

    ApmTraceUtils.addTagsToTrace(
        Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), JOB_ID_KEY, jobRunConfig.getJobId(), DESTINATION_DOCKER_IMAGE_KEY,
            destinationLauncherConfig.getDockerImage()));
    final ActivityExecutionContext context = Activity.getExecutionContext();
    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);
    return HeartbeatUtils.withBackgroundHeartbeat(
        cancellationCallback,
        () -> {
          final NormalizationInput fullInput = hydrateNormalizationInput(input);

          // Check the version of normalization
          // We require at least version 0.3.0 to support data types v1. Using an older version would lead to
          // all columns being typed as JSONB. If normalization is using an older version, fallback to using
          // v0 data types.
          if (!normalizationSupportsV1DataTypes(destinationLauncherConfig)) {
            log.info("Using protocol v0");
            CatalogMigrationV1Helper.downgradeSchemaIfNeeded(fullInput.getCatalog());
          } else {

            // This should only be useful for syncs that started before the release that contained v1 migration.
            // However, we lack the effective way to detect those syncs so this code should remain until we
            // phase v0 out.
            // Performance impact should be low considering the nature of the check compared to the time to run
            // normalization.
            log.info("Using protocol v1");
            CatalogMigrationV1Helper.upgradeSchemaIfNeeded(fullInput.getCatalog());
          }

          final Supplier<NormalizationInput> inputSupplier = () -> {
            airbyteConfigValidator.ensureAsRuntime(ConfigSchema.NORMALIZATION_INPUT, Jsons.jsonNode(fullInput));
            return fullInput;
          };

          final CheckedSupplier<Worker<NormalizationInput, NormalizationSummary>, Exception> workerFactory;

          log.info("Using normalization: " + destinationLauncherConfig.getNormalizationDockerImage());
          if (containerOrchestratorConfig.isPresent()) {
            final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.DEFAULT);
            workerFactory = getContainerLauncherWorkerFactory(workerConfigs, destinationLauncherConfig, jobRunConfig,
                input.getConnectionId(), input.getWorkspaceId());
          } else {
            workerFactory = getLegacyWorkerFactory(destinationLauncherConfig, jobRunConfig);
          }
          final var worker = workerFactory.get();
          cancellationCallback.set(worker::cancel);

          final TemporalAttemptExecution<NormalizationInput, NormalizationSummary> temporalAttemptExecution = new TemporalAttemptExecution<>(
              workspaceRoot, workerEnvironment, logConfigs,
              jobRunConfig,
              worker,
              inputSupplier.get(),
              airbyteApiClient,
              airbyteVersion,
              () -> context);

          return temporalAttemptExecution.get();
        },
        context);
  }

  private NormalizationInput hydrateNormalizationInput(final NormalizationInput input) throws Exception {
    // Hydrate the destination config.
    final JsonNode fullDestinationConfig;
    final UUID organizationId = input.getConnectionContext().getOrganizationId();
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      try {
        final SecretPersistenceConfig secretPersistenceConfig = airbyteApiClient.getSecretPersistenceConfigApi().getSecretsPersistenceConfig(
            new SecretPersistenceConfigGetRequestBody().scopeType(ScopeType.ORGANIZATION).scopeId(organizationId));
        final RuntimeSecretPersistence runtimeSecretPersistence =
            SecretPersistenceConfigHelper.fromApiSecretPersistenceConfig(secretPersistenceConfig);
        fullDestinationConfig =
            secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(input.getDestinationConfiguration(), runtimeSecretPersistence);
      } catch (final ApiException e) {
        throw new RuntimeException(e);
      }
    } else {
      fullDestinationConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(input.getDestinationConfiguration());
    }
    // Retrieve the catalog.
    final ConfiguredAirbyteCatalog catalog = retrieveCatalog(input.getConnectionId());
    return input.withDestinationConfiguration(fullDestinationConfig).withCatalog(catalog);
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
        .withResourceRequirements(getNormalizationResourceRequirements())
        .withWorkspaceId(syncInput.getWorkspaceId())
        .withConnectionContext(syncInput.getConnectionContext());
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public NormalizationInput generateNormalizationInputWithMinimumPayload(final JsonNode destinationConfiguration,
                                                                         final ConfiguredAirbyteCatalog airbyteCatalog,
                                                                         final UUID workspaceId) {
    return new NormalizationInput()
        .withDestinationConfiguration(destinationConfiguration)
        .withCatalog(airbyteCatalog)
        .withResourceRequirements(getNormalizationResourceRequirements())
        .withWorkspaceId(workspaceId);
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public NormalizationInput generateNormalizationInputWithMinimumPayloadWithConnectionId(final JsonNode destinationConfiguration,
                                                                                         final ConfiguredAirbyteCatalog airbyteCatalog,
                                                                                         final UUID workspaceId,
                                                                                         final UUID connectionId,
                                                                                         final UUID organizationId) {
    return new NormalizationInput()
        .withConnectionId(connectionId)
        .withDestinationConfiguration(destinationConfiguration)
        .withCatalog(airbyteCatalog)
        .withResourceRequirements(getNormalizationResourceRequirements())
        .withWorkspaceId(workspaceId)
        // As much info as we can give.
        .withConnectionContext(
            new ConnectionContext()
                .withOrganizationId(organizationId)
                .withConnectionId(connectionId)
                .withWorkspaceId(workspaceId));
  }

  private ResourceRequirements getNormalizationResourceRequirements() {
    return workerConfigsProvider.getConfig(ResourceType.NORMALIZATION).getResourceRequirements();
  }

  @VisibleForTesting
  static boolean normalizationSupportsV1DataTypes(final IntegrationLauncherConfig destinationLauncherConfig) {
    try {
      final Version normalizationVersion = new Version(getNormalizationImageTag(destinationLauncherConfig));
      return V1_NORMALIZATION_MINOR_VERSION.equals(normalizationVersion.getMinorVersion());
    } catch (final IllegalArgumentException e) {
      // IllegalArgument here means that the version isn't in a semver format.
      // The current behavior is to assume it supports v0 data types for dev purposes.
      return false;
    }
  }

  private static String getNormalizationImageTag(final IntegrationLauncherConfig destinationLauncherConfig) {
    return destinationLauncherConfig.getNormalizationDockerImage().split(":", 2)[1];
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
        workerEnvironment, () -> {});
  }

  @SuppressWarnings("LineLength")
  private CheckedSupplier<Worker<NormalizationInput, NormalizationSummary>, Exception> getContainerLauncherWorkerFactory(
                                                                                                                         final WorkerConfigs workerConfigs,
                                                                                                                         final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                                                         final JobRunConfig jobRunConfig,
                                                                                                                         final UUID connectionId,
                                                                                                                         final UUID workspaceId) {
    return () -> new NormalizationLauncherWorker(
        connectionId,
        workspaceId,
        destinationLauncherConfig,
        jobRunConfig,
        workerConfigs,
        containerOrchestratorConfig.get(),
        serverPort,
        featureFlagClient,
        metricClient,
        workloadIdGenerator);
  }

  private ConfiguredAirbyteCatalog retrieveCatalog(final UUID connectionId) throws Exception {
    final ConnectionRead connectionInfo =
        AirbyteApiClient
            .retryWithJitterThrows(
                () -> airbyteApiClient.getConnectionApi()
                    .getConnection(new ConnectionIdRequestBody().connectionId(connectionId)),
                "retrieve the connection");
    if (connectionInfo.getSyncCatalog() == null) {
      throw new IllegalArgumentException("Connection is missing catalog, which is required");
    }
    final ConfiguredAirbyteCatalog catalog = CatalogClientConverters.toConfiguredAirbyteProtocol(connectionInfo.getSyncCatalog());

    // NOTE: when we passed the catalog through the activity input, this mapping was previously done
    // during replication.
    return new NamespacingMapper(
        Enums.convertTo(connectionInfo.getNamespaceDefinition(), JobSyncConfig.NamespaceDefinitionType.class),
        connectionInfo.getNamespaceFormat(),
        connectionInfo.getPrefix()).mapCatalog(catalog);
  }

}
