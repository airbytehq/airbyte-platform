/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import static io.airbyte.config.helpers.LogClientSingleton.fullLogPath;
import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.temporal.HeartbeatUtils;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.version.Version;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseWorkloadApiForSpec;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.DefaultGetSpecWorker;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.models.SpecInput;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.Metadata;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadPriority;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * SpecActivityImpl.
 */
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class SpecActivityImpl implements SpecActivity {

  private final WorkerConfigsProvider workerConfigsProvider;
  private final ProcessFactory processFactory;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final AirbyteApiClient airbyteApiClient;
  private final String airbyteVersion;
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final FeatureFlags featureFlags;
  private final GsonPksExtractor gsonPksExtractor;
  private final FeatureFlagClient featureFlagClient;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final MetricClient metricClient;

  public SpecActivityImpl(final WorkerConfigsProvider workerConfigsProvider,
                          final ProcessFactory processFactory,
                          @Named("workspaceRoot") final Path workspaceRoot,
                          final WorkerEnvironment workerEnvironment,
                          final LogConfigs logConfigs,
                          final AirbyteApiClient airbyteApiClient,
                          @Value("${airbyte.version}") final String airbyteVersion,
                          final AirbyteMessageSerDeProvider serDeProvider,
                          final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                          final FeatureFlags featureFlags,
                          final GsonPksExtractor gsonPksExtractor,
                          final FeatureFlagClient featureFlagClient,
                          final WorkloadClient workloadClient,
                          final WorkloadIdGenerator workloadIdGenerator,
                          final MetricClient metricClient) {
    this.workerConfigsProvider = workerConfigsProvider;
    this.processFactory = processFactory;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.airbyteApiClient = airbyteApiClient;
    this.airbyteVersion = airbyteVersion;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
    this.gsonPksExtractor = gsonPksExtractor;
    this.featureFlagClient = featureFlagClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.metricClient = metricClient;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig, final IntegrationLauncherConfig launcherConfig) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ACTIVITY_SPEC, 1);

    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(), DOCKER_IMAGE_KEY, launcherConfig.getDockerImage(),
        JOB_ID_KEY, jobRunConfig.getJobId()));

    final Supplier<JobGetSpecConfig> inputSupplier =
        () -> new JobGetSpecConfig().withDockerImage(launcherConfig.getDockerImage()).withIsCustomConnector(launcherConfig.getIsCustomConnector());

    final ActivityExecutionContext context = Activity.getExecutionContext();
    final AtomicReference<Runnable> cancellationCallback = new AtomicReference<>(null);

    return HeartbeatUtils.withBackgroundHeartbeat(
        cancellationCallback,
        () -> {
          final var worker = getWorkerFactory(launcherConfig).get();
          cancellationCallback.set(worker::cancel);
          final TemporalAttemptExecution<JobGetSpecConfig, ConnectorJobOutput> temporalAttemptExecution = new TemporalAttemptExecution<>(
              workspaceRoot,
              workerEnvironment,
              logConfigs,
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

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(SpecInput input) throws WorkerException {
    final String jobId = input.getJobRunConfig().getJobId();
    final String workloadId =
        workloadIdGenerator.generateSpecWorkloadId(jobId);
    final String serializedInput = Jsons.serialize(input);

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(new WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId.toString())),
        serializedInput,
        fullLogPath(Path.of(workloadId)),
        Geography.AUTO.getValue(),
        WorkloadType.SPEC,
        WorkloadPriority.HIGH,
        null,
        null);

    workloadClient.createWorkload(workloadCreateRequest);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(ANONYMOUS));
    workloadClient.waitForWorkload(workloadId, checkFrequencyInSeconds);

    return workloadClient.getConnectorJobOutput(
        workloadId,
        failureReason -> new ConnectorJobOutput()
            .withOutputType(OutputType.SPEC)
            .withSpec(null)
            .withFailureReason(failureReason));
  }

  @Override
  public boolean shouldUseWorkload(UUID workspaceId) {
    return featureFlagClient.boolVariation(UseWorkloadApiForSpec.INSTANCE, new Workspace(workspaceId));
  }

  @Override
  public void reportSuccess() {
    metricClient.count(OssMetricsRegistry.SPEC, 1, new MetricAttribute(MetricTags.STATUS, "success"));
  }

  @Override
  public void reportFailure() {
    metricClient.count(OssMetricsRegistry.SPEC, 1, new MetricAttribute(MetricTags.STATUS, "failed"));
  }

  private CheckedSupplier<Worker<JobGetSpecConfig, ConnectorJobOutput>, Exception> getWorkerFactory(
                                                                                                    final IntegrationLauncherConfig launcherConfig) {
    return () -> {
      final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.SPEC);
      final AirbyteStreamFactory streamFactory = getStreamFactory(launcherConfig);
      final IntegrationLauncher integrationLauncher = new AirbyteIntegrationLauncher(
          launcherConfig.getJobId(),
          launcherConfig.getAttemptId().intValue(),
          launcherConfig.getConnectionId(),
          launcherConfig.getWorkspaceId(),
          launcherConfig.getDockerImage(),
          processFactory,
          workerConfigs.getResourceRequirements(),
          null,
          launcherConfig.getAllowedHosts(),
          launcherConfig.getIsCustomConnector(),
          featureFlags,
          Collections.emptyMap(),
          Collections.emptyMap());

      return new DefaultGetSpecWorker(integrationLauncher, streamFactory);
    };
  }

  private AirbyteStreamFactory getStreamFactory(final IntegrationLauncherConfig launcherConfig) {
    final Version protocolVersion =
        launcherConfig.getProtocolVersion() != null ? launcherConfig.getProtocolVersion() : migratorFactory.getMostRecentVersion();
    // Try to detect version from the stream
    return new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, protocolVersion, Optional.empty(),
        Optional.empty(), new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false), gsonPksExtractor)
            .withDetectVersion(true);
  }

}
