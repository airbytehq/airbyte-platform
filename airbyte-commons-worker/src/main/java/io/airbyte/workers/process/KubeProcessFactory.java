/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import autovalue.shaded.org.jetbrains.annotations.NotNull;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.helper.DockerImageNameHelper;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.ImageName;
import io.airbyte.featureflag.ImageVersion;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.RunSocatInConnectorContainer;
import io.airbyte.featureflag.UseCustomK8sScheduler;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.ConnectorApmSupportHelper;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kube process factory.
 */
public class KubeProcessFactory implements ProcessFactory {

  @VisibleForTesting
  public static final int KUBE_NAME_LEN_LIMIT = 63;

  private static final Logger LOGGER = LoggerFactory.getLogger(KubeProcessFactory.class);
  private static final UUID UUID_EMPTY = UUID.fromString("00000000-0000-0000-0000-000000000000");

  private final WorkerConfigsProvider workerConfigsProvider;
  private final FeatureFlagClient featureFlagClient;
  private final String namespace;
  private final String serviceAccount;
  private final KubernetesClient fabricClient;
  private final String kubeHeartbeatUrl;
  private final String processRunnerHost;

  /**
   * Sets up a process factory with the default processRunnerHost.
   */
  public KubeProcessFactory(final WorkerConfigsProvider workerConfigsProvider,
                            final FeatureFlagClient featureFlagClient,
                            final String namespace,
                            final String serviceAccount,
                            final KubernetesClient fabricClient,
                            final String kubeHeartbeatUrl) {
    this(
        workerConfigsProvider,
        featureFlagClient,
        namespace,
        serviceAccount,
        fabricClient,
        kubeHeartbeatUrl,
        Exceptions.toRuntime(() -> InetAddress.getLocalHost().getHostAddress()));
  }

  /**
   * Create kube pod process factory.
   *
   * @param namespace kubernetes namespace where spawned pods will live
   * @param fabricClient fabric8 kubernetes client
   * @param kubeHeartbeatUrl a url where if the response is not 200 the spawned process will fail
   *        itself
   * @param processRunnerHost is the local host or ip of the machine running the process factory.
   *        injectable for testing.
   */
  @VisibleForTesting
  public KubeProcessFactory(final WorkerConfigsProvider workerConfigsProvider,
                            final FeatureFlagClient featureFlagClient,
                            final String namespace,
                            final String serviceAccount,
                            final KubernetesClient fabricClient,
                            final String kubeHeartbeatUrl,
                            final String processRunnerHost) {
    this.workerConfigsProvider = workerConfigsProvider;
    this.featureFlagClient = featureFlagClient;
    this.namespace = namespace;
    this.serviceAccount = serviceAccount;
    this.fabricClient = fabricClient;
    this.kubeHeartbeatUrl = kubeHeartbeatUrl;
    this.processRunnerHost = processRunnerHost;
  }

  @Override
  public Process create(
                        final ResourceType resourceType,
                        final String jobType,
                        final String jobId,
                        final int attempt,
                        final UUID connectionId,
                        final UUID workspaceId,
                        final Path jobRoot,
                        final String imageName,
                        final boolean isCustomConnector,
                        final boolean usesStdin,
                        final Map<String, String> files,
                        final String entrypoint,
                        final ConnectorResourceRequirements resourceRequirements,
                        final AllowedHosts allowedHosts,
                        final Map<String, String> customLabels,
                        final Map<String, String> jobMetadata,
                        final Map<Integer, Integer> internalToExternalPorts,
                        @NotNull final Map<String, String> additionalEnvironmentVariables,
                        final String... args)
      throws WorkerException {
    try {
      // used to differentiate source and destination processes with the same id and attempt
      final String podName = ProcessFactory.createProcessName(imageName, jobType, jobId, attempt, KUBE_NAME_LEN_LIMIT);
      LOGGER.info("Attempting to start pod = {} for {} with resources {} and allowedHosts {}", podName, imageName, resourceRequirements,
          allowedHosts);

      final int stdoutLocalPort = KubePortManagerSingleton.getInstance().take();
      LOGGER.info("{} stdoutLocalPort = {}", podName, stdoutLocalPort);

      final int stderrLocalPort = KubePortManagerSingleton.getInstance().take();
      LOGGER.info("{} stderrLocalPort = {}", podName, stderrLocalPort);

      final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(resourceType);

      final var allLabels = getLabels(jobId, attempt, connectionId, workspaceId, imageName, customLabels, workerConfigs.getWorkerKubeLabels());

      // If using isolated pool, check workerConfigs has isolated pool set. If not set, fall back to use
      // regular node pool.
      final var nodeSelectors =
          isCustomConnector ? workerConfigs.getWorkerIsolatedKubeNodeSelectors().orElse(workerConfigs.getworkerKubeNodeSelectors())
              : workerConfigs.getworkerKubeNodeSelectors();

      final String schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler.INSTANCE,
          // Pod may not have a connectionId, yet feature flag client requires a context.
          // If we do not have one, use empty uuid.
          new Connection(connectionId != null ? connectionId : UUID_EMPTY));

      final boolean runSocatInMainContainer = shouldRunSocatInMainContainer(imageName, connectionId, workspaceId);

      final Context featureFlagContext = createFeatureFlagContext(connectionId, workspaceId, imageName);

      return new KubePodProcess(
          processRunnerHost,
          fabricClient,
          featureFlagClient,
          featureFlagContext,
          podName,
          namespace,
          serviceAccount,
          schedulerName.isBlank() ? null : schedulerName,
          imageName,
          workerConfigs.getJobImagePullPolicy(),
          workerConfigs.getSidecarImagePullPolicy(),
          stdoutLocalPort,
          stderrLocalPort,
          kubeHeartbeatUrl,
          usesStdin,
          files,
          entrypoint,
          resourceRequirements,
          workerConfigs.getJobImagePullSecrets(),
          workerConfigs.getWorkerKubeTolerations(),
          nodeSelectors,
          allLabels,
          workerConfigs.getWorkerKubeAnnotations(),
          workerConfigs.getJobSocatImage(),
          workerConfigs.getJobBusyboxImage(),
          workerConfigs.getJobCurlImage(),
          runSocatInMainContainer,
          MoreMaps.merge(jobMetadata, workerConfigs.getEnvMap(), additionalEnvironmentVariables),
          internalToExternalPorts,
          args).toProcess();
    } catch (final Exception e) {
      throw new WorkerException("Failed to create pod for " + jobType + " step", e);
    }
  }

  @org.jetbrains.annotations.NotNull
  private static Multi createFeatureFlagContext(final UUID connectionId, final UUID workspaceId, final String imageName) {
    final List<Context> contexts = new ArrayList<>();

    if (workspaceId != null) {
      contexts.add(new Workspace(workspaceId));
    }
    if (connectionId != null) {
      contexts.add(new Connection(connectionId));
    }
    if (imageName != null) {
      contexts.add(new ImageName(ConnectorApmSupportHelper.getImageName(imageName)));
      contexts.add(new ImageVersion(ConnectorApmSupportHelper.getImageVersion(imageName)));
    }

    return new Multi(contexts);
  }

  /**
   * Returns general labels to be applied to all Kubernetes pods. All general labels should be added
   * here.
   */
  public static Map<String, String> getLabels(final String jobId,
                                              final int attemptId,
                                              final UUID connectionId,
                                              final UUID workspaceId,
                                              final String fullImagePath,
                                              final Map<String, String> customLabels,
                                              final Map<String, String> envLabels) {
    final Map<String, String> allLabels = new HashMap<>();
    if (envLabels != null) {
      allLabels.putAll(envLabels);
    }
    allLabels.putAll(customLabels);

    final String shortImageName = ProcessFactory.getShortImageName(fullImagePath);
    final String imageVersion = ProcessFactory.getImageVersion(fullImagePath);

    final var generalKubeLabels = Map.of(
        Metadata.JOB_LABEL_KEY, jobId,
        Metadata.ATTEMPT_LABEL_KEY, String.valueOf(attemptId),
        Metadata.CONNECTION_ID_LABEL_KEY, String.valueOf(connectionId),
        Metadata.WORKSPACE_LABEL_KEY, String.valueOf(workspaceId),
        Metadata.WORKER_POD_LABEL_KEY, Metadata.WORKER_POD_LABEL_VALUE,
        Metadata.IMAGE_NAME, shortImageName,
        Metadata.IMAGE_VERSION, imageVersion);

    allLabels.putAll(generalKubeLabels);

    return allLabels;
  }

  private boolean shouldRunSocatInMainContainer(final String imageName, final UUID connectionId, final UUID workspaceId) {
    final String imageNameWithoutVersion;
    final String imageVersion;
    if (imageName == null) {
      imageNameWithoutVersion = "";
      imageVersion = "";
    } else {
      imageNameWithoutVersion = DockerImageNameHelper.extractImageNameWithoutVersion(imageName);
      imageVersion = DockerImageNameHelper.extractImageVersionString(imageName);
    }

    final var imageNameContext = imageNameWithoutVersion != null ? imageNameWithoutVersion : "";
    final var imageVersionContext = imageVersion != null ? imageVersion : "";
    final var connectionContext = connectionId != null ? connectionId : UUID_EMPTY;
    final var workspaceContext = workspaceId != null ? workspaceId : UUID_EMPTY;

    return featureFlagClient.boolVariation(RunSocatInConnectorContainer.INSTANCE,
        new Multi(List.of(
            new ImageName(imageNameContext),
            new ImageVersion(imageVersionContext),
            new Connection(connectionContext),
            new Workspace(workspaceContext))));
  }

}
