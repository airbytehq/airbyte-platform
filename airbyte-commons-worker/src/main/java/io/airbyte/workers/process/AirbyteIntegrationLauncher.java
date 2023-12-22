/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ROOT_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;
import static io.airbyte.workers.process.Metadata.CHECK_JOB;
import static io.airbyte.workers.process.Metadata.CHECK_STEP_KEY;
import static io.airbyte.workers.process.Metadata.CONNECTOR_STEP;
import static io.airbyte.workers.process.Metadata.DISCOVER_JOB;
import static io.airbyte.workers.process.Metadata.JOB_TYPE_KEY;
import static io.airbyte.workers.process.Metadata.READ_STEP;
import static io.airbyte.workers.process.Metadata.SPEC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;
import static io.airbyte.workers.process.Metadata.WRITE_STEP;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datadog.trace.api.Trace;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.WorkerEnvConstants;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.exception.WorkerException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Launcher creates process for each protocol method.
 */
public class AirbyteIntegrationLauncher implements IntegrationLauncher {

  private static final Configs configs = new EnvConfigs();

  private static final ResourceRequirements DEFAULT_SIDECAR_RESOURCES = new ResourceRequirements()
      .withMemoryLimit(configs.getSidecarKubeMemoryLimit()).withMemoryRequest(configs.getSidecarMemoryRequest())
      .withCpuLimit(configs.getSidecarKubeCpuLimit()).withCpuRequest(configs.getSidecarKubeCpuRequest());
  private static final ResourceRequirements DEFAULT_SOCAT_RESOURCES = new ResourceRequirements()
      .withMemoryLimit(configs.getSidecarKubeMemoryLimit()).withMemoryRequest(configs.getSidecarMemoryRequest())
      .withCpuLimit(configs.getSocatSidecarKubeCpuLimit()).withCpuRequest(configs.getSocatSidecarKubeCpuRequest());

  // Stderr channel usually does not require much CPU resources. It's not on critical path nor it will
  // impact performance.
  // Thus, we will allocate much less CPU for these containers.
  private static final ResourceRequirements LOW_SOCAT_RESOURCES = new ResourceRequirements()
      .withMemoryLimit(configs.getSidecarKubeMemoryLimit()).withMemoryRequest(configs.getSidecarMemoryRequest())
      .withCpuLimit("2").withCpuRequest("0.25");

  private static final String CONFIG = "--config";

  private final String jobId;
  private final int attempt;
  private final UUID connectionId;
  private final UUID workspaceId;
  private final String imageName;
  private final ProcessFactory processFactory;
  private final ResourceRequirements resourceRequirement;
  private final SyncResourceRequirements syncResourceRequirements;
  private final FeatureFlags featureFlags;

  private final Map<String, String> additionalEnvironmentVariables;
  private final Map<String, String> additionalLabels;

  /**
   * If true, launcher will use a separated isolated pool to run the job.
   * <p>
   * At this moment, we put custom connector jobs into an isolated pool.
   */
  private final boolean useIsolatedPool;
  private final AllowedHosts allowedHosts;

  public AirbyteIntegrationLauncher(final String jobId,
                                    final int attempt,
                                    final UUID connectionId,
                                    final UUID workspaceId,
                                    final String imageName,
                                    final ProcessFactory processFactory,
                                    final ResourceRequirements resourceRequirement,
                                    final SyncResourceRequirements syncResourceRequirements,
                                    final AllowedHosts allowedHosts,
                                    final boolean useIsolatedPool,
                                    final FeatureFlags featureFlags,
                                    final Map<String, String> additionalEnvironmentVariables,
                                    final Map<String, String> additionalLabels) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.connectionId = connectionId;
    this.workspaceId = workspaceId;
    this.imageName = imageName;
    this.processFactory = processFactory;
    this.resourceRequirement = resourceRequirement;
    this.syncResourceRequirements = syncResourceRequirements;
    this.allowedHosts = allowedHosts;
    this.featureFlags = featureFlags;
    this.useIsolatedPool = useIsolatedPool;
    this.additionalEnvironmentVariables = additionalEnvironmentVariables;
    this.additionalLabels = additionalLabels;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process spec(final Path jobRoot) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    return processFactory.create(
        ResourceType.SPEC,
        SPEC_JOB,
        jobId,
        attempt,
        connectionId,
        workspaceId,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        Collections.emptyMap(),
        null,
        buildGenericConnectorResourceRequirements(resourceRequirement),
        allowedHosts,
        Map.of(JOB_TYPE_KEY, SPEC_JOB),
        getWorkerMetadata(),
        Collections.emptyMap(),
        additionalEnvironmentVariables,
        "spec");
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process check(final Path jobRoot, final String configFilename, final String configContents) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    return processFactory.create(
        ResourceType.CHECK,
        CHECK_JOB,
        jobId,
        attempt,
        connectionId,
        workspaceId,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        ImmutableMap.of(configFilename, configContents),
        null,
        buildGenericConnectorResourceRequirements(resourceRequirement),
        allowedHosts,
        Map.of(JOB_TYPE_KEY, CHECK_JOB, CHECK_STEP_KEY, CONNECTOR_STEP),
        getWorkerMetadata(),
        Collections.emptyMap(),
        additionalEnvironmentVariables,
        "check",
        CONFIG, configFilename);
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process discover(final Path jobRoot, final String configFilename, final String configContents) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    return processFactory.create(
        ResourceType.DISCOVER,
        DISCOVER_JOB,
        jobId,
        attempt,
        connectionId,
        workspaceId,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        ImmutableMap.of(configFilename, configContents),
        null,
        buildGenericConnectorResourceRequirements(resourceRequirement),
        allowedHosts,
        Map.of(JOB_TYPE_KEY, DISCOVER_JOB),
        getWorkerMetadata(),
        Collections.emptyMap(),
        additionalEnvironmentVariables,
        "discover",
        CONFIG, configFilename);
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process read(final Path jobRoot,
                      final String configFilename,
                      final String configContents,
                      final String catalogFilename,
                      final String catalogContents,
                      final String stateFilename,
                      final String stateContents)
      throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    final List<String> arguments = Lists.newArrayList(
        "read",
        CONFIG, configFilename,
        "--catalog", catalogFilename);

    final Map<String, String> files = new HashMap<>();
    files.put(configFilename, configContents);
    files.put(catalogFilename, catalogContents);

    if (stateFilename != null) {
      arguments.add("--state");
      arguments.add(stateFilename);

      Preconditions.checkNotNull(stateContents);
      files.put(stateFilename, stateContents);
    }

    return processFactory.create(
        ResourceType.REPLICATION,
        READ_STEP,
        jobId,
        attempt,
        connectionId,
        workspaceId,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        files,
        null,
        buildSourceConnectorResourceRequirements(resourceRequirement, syncResourceRequirements),
        allowedHosts,
        getLabels(Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, READ_STEP)),
        getWorkerMetadata(),
        Collections.emptyMap(),
        additionalEnvironmentVariables,
        arguments.toArray(new String[arguments.size()]));
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process write(final Path jobRoot,
                       final String configFilename,
                       final String configContents,
                       final String catalogFilename,
                       final String catalogContents)
      throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    final Map<String, String> files = ImmutableMap.of(
        configFilename, configContents,
        catalogFilename, catalogContents);

    return processFactory.create(
        ResourceType.REPLICATION,
        WRITE_STEP,
        jobId,
        attempt,
        connectionId,
        workspaceId,
        jobRoot,
        imageName,
        useIsolatedPool,
        true,
        files,
        null,
        buildDestinationConnectorResourceRequirements(resourceRequirement, syncResourceRequirements),
        allowedHosts,
        getLabels(Map.of(JOB_TYPE_KEY, SYNC_JOB, SYNC_STEP_KEY, WRITE_STEP)),
        getWorkerMetadata(),
        Collections.emptyMap(),
        additionalEnvironmentVariables,
        "write",
        CONFIG, configFilename,
        "--catalog", catalogFilename);
  }

  private Map<String, String> getLabels(final Map<String, String> labels) {
    final var mergedLabels = new HashMap<String, String>();
    mergedLabels.putAll(additionalLabels);
    mergedLabels.putAll(labels);
    return mergedLabels;
  }

  private Map<String, String> getWorkerMetadata() {
    final Configs configs = new EnvConfigs();
    // We've managed to exceed the maximum number of parameters for Map.of(), so use a builder + convert
    // back to hashmap
    return Maps.newHashMap(
        ImmutableMap.<String, String>builder()
            .put(WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, imageName)
            .put(WorkerEnvConstants.WORKER_JOB_ID, jobId)
            .put(WorkerEnvConstants.WORKER_JOB_ATTEMPT, String.valueOf(attempt))
            .put(EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA, String.valueOf(featureFlags.autoDetectSchema()))
            .put(EnvVariableFeatureFlags.APPLY_FIELD_SELECTION, String.valueOf(featureFlags.applyFieldSelection()))
            .put(EnvVariableFeatureFlags.FIELD_SELECTION_WORKSPACES, featureFlags.fieldSelectionWorkspaces())
            // The platform doesn't support this env-var anymore, however the connectors still depend on it,
            // defaulting to false if not supplied.
            .put("USE_STREAM_CAPABLE_STATE", "true")
            .put(EnvConfigs.SOCAT_KUBE_CPU_LIMIT, configs.getSocatSidecarKubeCpuLimit())
            .put(EnvConfigs.SOCAT_KUBE_CPU_REQUEST, configs.getSocatSidecarKubeCpuRequest())
            .put(EnvConfigs.LAUNCHDARKLY_KEY, configs.getLaunchDarklyKey())
            .put(EnvConfigs.FEATURE_FLAG_CLIENT, configs.getFeatureFlagClient())
            .put(EnvConfigs.OTEL_COLLECTOR_ENDPOINT, configs.getOtelCollectorEndpoint())
            .build());
  }

  private static ConnectorResourceRequirements buildDestinationConnectorResourceRequirements(final ResourceRequirements resourceRequirements,
                                                                                             final SyncResourceRequirements syncResourceReqs) {
    if (syncResourceReqs != null) {
      return new ConnectorResourceRequirements(
          syncResourceReqs.getDestination(),
          syncResourceReqs.getHeartbeat(),
          syncResourceReqs.getDestinationStdErr(),
          syncResourceReqs.getDestinationStdIn(),
          syncResourceReqs.getDestinationStdOut());
    } else {
      return new ConnectorResourceRequirements(
          resourceRequirements,
          DEFAULT_SIDECAR_RESOURCES,
          LOW_SOCAT_RESOURCES,
          DEFAULT_SOCAT_RESOURCES,
          DEFAULT_SOCAT_RESOURCES);
    }
  }

  /**
   * Generate ConnectorResourceRequirements with filled default for side containers.
   */
  @VisibleForTesting
  public static ConnectorResourceRequirements buildGenericConnectorResourceRequirements(final ResourceRequirements resourceRequirements) {
    return new ConnectorResourceRequirements(
        resourceRequirements,
        DEFAULT_SIDECAR_RESOURCES,
        LOW_SOCAT_RESOURCES,
        DEFAULT_SOCAT_RESOURCES,
        DEFAULT_SOCAT_RESOURCES);
  }

  private static ConnectorResourceRequirements buildSourceConnectorResourceRequirements(final ResourceRequirements resourceRequirements,
                                                                                        final SyncResourceRequirements syncResourceRequirements) {
    if (syncResourceRequirements != null) {
      return new ConnectorResourceRequirements(
          syncResourceRequirements.getSource(),
          syncResourceRequirements.getHeartbeat(),
          syncResourceRequirements.getSourceStdErr(),
          // Req is null because we do create a StdIn container for sources.
          // This needs to be updated if we were to add one.
          null,
          syncResourceRequirements.getSourceStdOut());
    } else {
      return new ConnectorResourceRequirements(
          resourceRequirements,
          DEFAULT_SIDECAR_RESOURCES,
          LOW_SOCAT_RESOURCES,
          DEFAULT_SOCAT_RESOURCES,
          DEFAULT_SOCAT_RESOURCES);
    }
  }

}
