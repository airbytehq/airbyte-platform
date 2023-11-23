/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static io.airbyte.commons.constants.WorkerConstants.KubeConstants.INIT_CONTAINER_STARTUP_TIMEOUT;
import static io.airbyte.commons.constants.WorkerConstants.KubeConstants.INIT_CONTAINER_TERMINATION_TIMEOUT;
import static io.airbyte.commons.constants.WorkerConstants.KubeConstants.POD_READY_TIMEOUT;

import io.airbyte.commons.io.IOs;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.list.Lists;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.TolerationPOJO;
import io.airbyte.featureflag.ConnectorApmEnabled;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.helper.ConnectorApmSupportHelper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodFluent;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.readiness.Readiness;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessHandle.Info;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A Process abstraction backed by a Kube Pod running in a Kubernetes cluster 'somewhere'. The
 * parent process starting a Kube Pod Process needs to exist within the Kube networking space. This
 * is so the parent process can forward data into the child's stdin and read the child's stdout and
 * stderr streams and copy configuration files over.
 * <p>
 * This is made possible by:
 * <ul>
 * <li>1) An init container that creates 3 named pipes corresponding to stdin, stdout and std err on
 * a shared volume.</li>
 * <li>2) Config files (e.g. config.json, catalog.json etc) are copied from the parent process into
 * a shared volume.</li>
 * <li>3) Redirecting the stdin named pipe to the original image's entrypoint and it's output into
 * the respective named pipes for stdout and stderr.</li>
 * <li>4) Each named pipe has a corresponding side car. Each side car forwards its stream
 * accordingly using socat. e.g. stderr/stdout is forwarded to parent process while input from the
 * parent process is forwarded into stdin.</li>
 * <li>5) The parent process listens on the stdout and stederr sockets for an incoming TCP
 * connection. It also initiates a TCP connection to the child process aka the Kube pod on the
 * specified stdin socket.</li>
 * <li>6) The child process is able to access configuration data via the shared volume. It's inputs
 * and outputs - stdin, stdout and stderr - are forwarded the parent process via the sidecars.</li>
 * <li>7) The main process has its entrypoint wrapped to perform IO redirection and better error
 * handling.</li>
 * <li>8) A heartbeat sidecar checks if the worker that launched the pod is still alive. If not, the
 * pod will fail.</li>
 * </ul>
 * The docker image used for this pod process must expose a AIRBYTE_ENTRYPOINT which contains the
 * entrypoint we will wrap when creating the main container in the pod.
 * <p>
 * See the constructor for more information.
 */

// Suppressing AvoidPrintStackTrace PMD warnings because
// it is required for the connectors
@SuppressWarnings("PMD.AvoidPrintStackTrace")
// TODO(Davin): Better test for this. See https://github.com/airbytehq/airbyte/issues/3700.
@Slf4j
public class KubePodProcess implements KubePod {

  private static final Configs configs = new EnvConfigs();

  private static final Logger LOGGER = LoggerFactory.getLogger(KubePodProcess.class);

  public static final String MAIN_CONTAINER_NAME = "main";
  public static final String INIT_CONTAINER_NAME = "init";

  private static final String PIPES_DIR = "/pipes";
  private static final String STDIN_PIPE_FILE = PIPES_DIR + "/stdin";
  private static final String STDOUT_PIPE_FILE = PIPES_DIR + "/stdout";
  private static final String STDERR_PIPE_FILE = PIPES_DIR + "/stderr";
  public static final String CONFIG_DIR = "/config";
  public static final String TMP_DIR = "/tmp";
  private static final String TERMINATION_DIR = "/termination";
  private static final String TERMINATION_FILE_MAIN = TERMINATION_DIR + "/main";
  private static final String TERMINATION_FILE_CHECK = TERMINATION_DIR + "/check";
  public static final String SUCCESS_FILE_NAME = "FINISHED_UPLOADING";

  private static final int STDIN_REMOTE_PORT = 9001;

  // 143 is the typical SIGTERM exit code.
  // Used when the process is destroyed and the exit code can't be retrieved.
  private static final int KILLED_EXIT_CODE = 143;

  // init container should fail if no new data copied into the init container within
  // INIT_RETRY_TIMEOUT_MINUTES
  private static final double INIT_SLEEP_PERIOD_SECONDS = 0.1;

  // This timeout was initially 1 minute, but sync pods scheduled on newly-provisioned nodes
  // are occasionally not able to start the copy within 1 minute, hence the increase to 5 as default.
  // Can be set in env
  private static final Duration INIT_RETRY_TIMEOUT_MINUTES = Duration.ofMinutes(configs.getJobInitRetryTimeoutMinutes());

  private static final int INIT_RETRY_MAX_ITERATIONS = (int) (INIT_RETRY_TIMEOUT_MINUTES.toSeconds() / INIT_SLEEP_PERIOD_SECONDS);

  private static final ConnectorApmSupportHelper CONNECTOR_DATADOG_SUPPORT_HELPER = new ConnectorApmSupportHelper();
  private final KubernetesClient fabricClient;
  private final Pod podDefinition;

  private final AtomicBoolean wasClosed = new AtomicBoolean(false);

  private final OutputStream stdin;
  private InputStream stdout;
  private InputStream stderr;

  private final ServerSocket stdoutServerSocket;
  private final int stdoutLocalPort;
  private final ServerSocket stderrServerSocket;
  private final int stderrLocalPort;
  private final ExecutorService executorService;
  private final CompletableFuture<Integer> exitCodeFuture;
  private final SharedIndexInformer<Pod> podInformer;

  /**
   * Get pod IP.
   *
   * @param client kube client
   * @param podName pod name
   * @param podNamespace pod namespace
   * @return pod's ip address
   */
  public static String getPodIP(final KubernetesClient client, final String podName, final String podNamespace) {
    final var pod = client.pods().inNamespace(podNamespace).withName(podName).get();
    if (pod == null) {
      throw new RuntimeException(prependPodInfo("Error: unable to find pod!", podNamespace, podName));
    }
    return pod.getStatus().getPodIP();
  }

  private static Container getInit(final boolean usesStdin,
                                   final List<VolumeMount> mainVolumeMounts,
                                   final String busyboxImage,
                                   final ResourceRequirements resourceRequirements)
      throws IOException {

    final var initCommand = MoreResources.readResource("entrypoints/sync/init.sh")
        .replaceAll("USES_STDIN_VALUE", String.valueOf(usesStdin))
        .replaceAll("STDOUT_PIPE_FILE_VALUE", STDOUT_PIPE_FILE)
        .replaceAll("STDERR_PIPE_FILE_VALUE", STDERR_PIPE_FILE)
        .replaceAll("STDIN_PIPE_FILE_VALUE", STDIN_PIPE_FILE)
        .replaceAll("MAX_ITERATION_VALUE", String.valueOf(INIT_RETRY_MAX_ITERATIONS))
        .replaceAll("SUCCESS_FILE_NAME_VALUE", SUCCESS_FILE_NAME)
        .replaceAll("SLEEP_PERIOD_VALUE", String.valueOf(INIT_SLEEP_PERIOD_SECONDS));

    return new ContainerBuilder()
        .withName(INIT_CONTAINER_NAME)
        .withImage(busyboxImage)
        .withWorkingDir(CONFIG_DIR)
        .withCommand("sh", "-c", initCommand)
        .withResources(getResourceRequirementsBuilder(resourceRequirements).build())
        .withVolumeMounts(mainVolumeMounts)
        .build();
  }

  private static Container getMain(final FeatureFlagClient featureFlagClient,
                                   final Context featureFlagContext,
                                   final String image,
                                   final String imagePullPolicy,
                                   final boolean usesStdin,
                                   final String entrypointOverride,
                                   final List<VolumeMount> mainVolumeMounts,
                                   final ResourceRequirements resourceRequirements,
                                   final Map<Integer, Integer> internalToExternalPorts,
                                   final String socatCommands,
                                   final Map<String, String> envMap,
                                   final String... args)
      throws IOException {
    final var argsStr = String.join(" ", args);
    final var optionalStdin = usesStdin ? String.format("< %s", STDIN_PIPE_FILE) : "";
    final var entrypointOverrideValue = entrypointOverride == null ? "" : StringEscapeUtils.escapeXSI(entrypointOverride);

    // communicates its completion to the heartbeat check via a file and closes itself if the heartbeat
    // fails
    final var mainCommand = MoreResources.readResource("entrypoints/sync/main.sh")
        .replaceAll("TERMINATION_FILE_CHECK", TERMINATION_FILE_CHECK)
        .replaceAll("TERMINATION_FILE_MAIN", TERMINATION_FILE_MAIN)
        .replaceAll("OPTIONAL_STDIN", optionalStdin)
        .replace("ENTRYPOINT_OVERRIDE_VALUE", entrypointOverrideValue) // use replace and not replaceAll to preserve escaping and quoting
        .replaceAll("ARGS", argsStr)
        .replaceAll("STDERR_PIPE_FILE", STDERR_PIPE_FILE)
        .replaceAll("STDOUT_PIPE_FILE", STDOUT_PIPE_FILE)
        .replaceAll("SOCAT_COMMANDS", socatCommands);

    final List<ContainerPort> containerPorts = createContainerPortList(internalToExternalPorts);

    final List<EnvVar> envVars = envMap.entrySet().stream()
        .map(entry -> new EnvVar(entry.getKey(), entry.getValue(), null))
        .collect(Collectors.toList());

    if (featureFlagClient.boolVariation(ConnectorApmEnabled.INSTANCE, featureFlagContext)) {
      CONNECTOR_DATADOG_SUPPORT_HELPER.addApmEnvVars(envVars);
      CONNECTOR_DATADOG_SUPPORT_HELPER.addServerNameAndVersionToEnvVars(image, envVars);
    }

    final ContainerBuilder containerBuilder = new ContainerBuilder()
        .withName(MAIN_CONTAINER_NAME)
        .withPorts(containerPorts)
        .withImage(image)
        .withImagePullPolicy(imagePullPolicy)
        .withCommand("sh", "-c", mainCommand)
        .withEnv(envVars)
        .withWorkingDir(CONFIG_DIR)
        .withVolumeMounts(mainVolumeMounts);

    final ResourceRequirementsBuilder resourceRequirementsBuilder = getResourceRequirementsBuilder(resourceRequirements);
    if (resourceRequirementsBuilder != null) {
      containerBuilder.withResources(resourceRequirementsBuilder.build());
    }
    return containerBuilder.build();
  }

  /**
   * Create port list for pod.
   *
   * @param internalToExternalPorts internal to external pots
   * @return container ports
   */
  public static List<ContainerPort> createContainerPortList(final Map<Integer, Integer> internalToExternalPorts) {
    return internalToExternalPorts.keySet().stream()
        .map(integer -> new ContainerPortBuilder()
            .withContainerPort(integer)
            .build())
        .collect(Collectors.toList());
  }

  /**
   * Copy files to kube pod.
   *
   * @param client kube client
   * @param podDefinition pod to copy to
   * @param files files to copy
   */
  public static void copyFilesToKubeConfigVolume(final KubernetesClient client,
                                                 final Pod podDefinition,
                                                 final Map<String, String> files) {
    final List<Map.Entry<String, String>> fileEntries = new ArrayList<>(files.entrySet());

    // copy this file last to indicate that the copy has completed
    fileEntries.add(new AbstractMap.SimpleEntry<>(SUCCESS_FILE_NAME, ""));

    Path tmpFile = null;
    Process proc = null;
    for (final Map.Entry<String, String> file : fileEntries) {
      try {
        tmpFile = Path.of(IOs.writeFileToRandomTmpDir(file.getKey(), file.getValue()));

        LOGGER.info("Uploading file: " + file.getKey());
        final var containerPath = Path.of(CONFIG_DIR + "/" + file.getKey());

        // using kubectl cp directly here, because both fabric and the official kube client APIs have
        // several issues with copying files. See https://github.com/airbytehq/airbyte/issues/8643 for
        // details.
        final String command = String.format("kubectl cp %s %s/%s:%s -c %s", tmpFile, podDefinition.getMetadata().getNamespace(),
            podDefinition.getMetadata().getName(), containerPath, INIT_CONTAINER_NAME);
        LOGGER.info(command);

        proc = Runtime.getRuntime().exec(command);
        LOGGER.info("Waiting for kubectl cp to complete");
        final int exitCode = proc.waitFor();

        if (exitCode != 0) {
          // Copying the success indicator file to the init container causes the container to immediately
          // exit, causing the `kubectl cp` command to exit with code 137. This check ensures that an error is
          // not thrown in this case if the init container exits successfully.
          if (SUCCESS_FILE_NAME.equals(file.getKey())
              && waitForInitPodToTerminate(client, podDefinition, INIT_CONTAINER_TERMINATION_TIMEOUT.toMinutes(), TimeUnit.MINUTES) == 0) {
            LOGGER.info("Init was successful; ignoring non-zero kubectl cp exit code for success indicator file.");
          } else {
            throw new IOException("kubectl cp failed with exit code " + exitCode);
          }
        }

        LOGGER.info("kubectl cp complete, closing process");
      } catch (final IOException | InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        if (tmpFile != null) {
          try {
            tmpFile.toFile().delete();
          } catch (final Exception e) {
            LOGGER.info("Caught exception when deleting temp file but continuing to allow process deletion.", e);
          }
        }
        if (proc != null) {
          proc.destroy();
        }
      }
    }
  }

  /**
   * The calls in this function aren't straight-forward due to api limitations. There is no proper way
   * to directly look for containers within a pod or query if a container is in a running state beside
   * checking if the getRunning field is set. We could put this behind an interface, but that seems
   * heavy-handed compared to the 10 lines here.
   */
  private static void waitForInitPodToRun(final KubernetesClient client, final Pod podDefinition) throws InterruptedException {
    // todo: this could use the watcher instead of waitUntilConditions
    LOGGER.info("Waiting for init container to be ready before copying files...");
    client.pods().inNamespace(podDefinition.getMetadata().getNamespace()).withName(podDefinition.getMetadata().getName())
        .waitUntilCondition(p -> !p.getStatus().getInitContainerStatuses().isEmpty()
            && p.getStatus().getInitContainerStatuses().get(0).getState().getRunning() != null, INIT_CONTAINER_STARTUP_TIMEOUT.toMinutes(),
            TimeUnit.MINUTES);
    LOGGER.info("Init container ready..");
  }

  /**
   * Waits for the init container to terminate, and returns its exit code.
   */
  private static int waitForInitPodToTerminate(final KubernetesClient client,
                                               final Pod podDefinition,
                                               final long timeUnitsToWait,
                                               final TimeUnit timeUnit)
      throws InterruptedException {
    LOGGER.info("Waiting for init container to terminate before checking exit value...");
    client.pods().inNamespace(podDefinition.getMetadata().getNamespace()).withName(podDefinition.getMetadata().getName())
        .waitUntilCondition(p -> p.getStatus().getInitContainerStatuses().get(0).getState().getTerminated() != null, timeUnitsToWait, timeUnit);
    final int exitValue = client.pods().inNamespace(podDefinition.getMetadata().getNamespace()).withName(podDefinition.getMetadata().getName()).get()
        .getStatus().getInitContainerStatuses().get(0).getState().getTerminated().getExitCode();
    LOGGER.info("Init container terminated with exit value {}.", exitValue);
    return exitValue;
  }

  private Toleration[] buildPodTolerations(final List<TolerationPOJO> tolerations) {
    if (tolerations == null || tolerations.isEmpty()) {
      return null;
    }
    return tolerations.stream().map(workerPodToleration -> new TolerationBuilder()
        .withKey(workerPodToleration.getKey())
        .withEffect(workerPodToleration.getEffect())
        .withOperator(workerPodToleration.getOperator())
        .withValue(workerPodToleration.getValue())
        .build())
        .toArray(Toleration[]::new);
  }

  @SuppressWarnings({"PMD.InvalidLogMessageFormat", "VariableDeclarationUsageDistance"})
  public KubePodProcess(final String processRunnerHost,
                        final KubernetesClient fabricClient,
                        final FeatureFlagClient featureFlagClient,
                        final Context featureFlagContext,
                        final String podName,
                        final String namespace,
                        final String serviceAccount,
                        final String schedulerName,
                        final String image,
                        final String imagePullPolicy,
                        final String sidecarImagePullPolicy,
                        final int stdoutLocalPort,
                        final int stderrLocalPort,
                        final String kubeHeartbeatUrl,
                        final boolean usesStdin,
                        final Map<String, String> files,
                        final String entrypointOverride,
                        final ConnectorResourceRequirements podResourceRequirements,
                        final List<String> imagePullSecrets,
                        final List<TolerationPOJO> tolerations,
                        final Map<String, String> nodeSelectors,
                        final Map<String, String> labels,
                        final Map<String, String> annotations,
                        final String socatImage,
                        final String busyboxImage,
                        final String curlImage,
                        final boolean runSocatInMainContainer,
                        final Map<String, String> envMap,
                        final Map<Integer, Integer> internalToExternalPorts,
                        final String... args)
      throws IOException, InterruptedException {
    try {
      this.fabricClient = fabricClient;
      this.stdoutLocalPort = stdoutLocalPort;
      this.stderrLocalPort = stderrLocalPort;
      this.stdoutServerSocket = new ServerSocket(stdoutLocalPort);
      this.stderrServerSocket = new ServerSocket(stderrLocalPort);
      this.executorService = Executors.newFixedThreadPool(2);
      setupStdOutAndStdErrListeners();

      if (entrypointOverride != null) {
        LOGGER.info("Found entrypoint override: {}", entrypointOverride);
      }

      final Volume pipeVolume = new VolumeBuilder()
          .withName("airbyte-pipes")
          .withNewEmptyDir()
          .endEmptyDir()
          .build();

      final VolumeMount pipeVolumeMount = new VolumeMountBuilder()
          .withName("airbyte-pipes")
          .withMountPath(PIPES_DIR)
          .build();

      final Volume configVolume = new VolumeBuilder()
          .withName("airbyte-config")
          .withNewEmptyDir()
          .withMedium("Memory")
          .endEmptyDir()
          .build();

      final VolumeMount configVolumeMount = new VolumeMountBuilder()
          .withName("airbyte-config")
          .withMountPath(CONFIG_DIR)
          .build();

      final Volume terminationVolume = new VolumeBuilder()
          .withName("airbyte-termination")
          .withNewEmptyDir()
          .endEmptyDir()
          .build();

      final VolumeMount terminationVolumeMount = new VolumeMountBuilder()
          .withName("airbyte-termination")
          .withMountPath(TERMINATION_DIR)
          .build();

      final Volume tmpVolume = new VolumeBuilder()
          .withName("tmp")
          .withNewEmptyDir()
          .endEmptyDir()
          .build();

      final VolumeMount tmpVolumeMount = new VolumeMountBuilder()
          .withName("tmp")
          .withMountPath(TMP_DIR)
          .build();

      // Printing socat notice logs with socat -d -d
      // To print info logs as well use socat -d -d -d
      // more info: https://linux.die.net/man/1/socat
      final Container remoteStdin = new ContainerBuilder()
          .withName("remote-stdin")
          .withImage(socatImage)
          .withCommand("sh", "-c", "socat -d -d TCP-L:9001 STDOUT > " + STDIN_PIPE_FILE)
          .withVolumeMounts(pipeVolumeMount, terminationVolumeMount)
          .withResources(getResourceRequirementsBuilder(podResourceRequirements.stdIn()).build())
          .withImagePullPolicy(sidecarImagePullPolicy)
          .build();

      final Container relayStdout = new ContainerBuilder()
          .withName("relay-stdout")
          .withImage(socatImage)
          .withCommand("sh", "-c", String.format("cat %s | socat -d -d -t 60 - TCP:%s:%s", STDOUT_PIPE_FILE, processRunnerHost, stdoutLocalPort))
          .withVolumeMounts(pipeVolumeMount, terminationVolumeMount)
          .withResources(getResourceRequirementsBuilder(podResourceRequirements.stdOut()).build())
          .withImagePullPolicy(sidecarImagePullPolicy)
          .build();

      final Container relayStderr = new ContainerBuilder()
          .withName("relay-stderr")
          .withImage(socatImage)
          .withCommand("sh", "-c", String.format("cat %s | socat -d -d -t 60 - TCP:%s:%s", STDERR_PIPE_FILE, processRunnerHost, stderrLocalPort))
          .withVolumeMounts(pipeVolumeMount, terminationVolumeMount)
          .withResources(getResourceRequirementsBuilder(podResourceRequirements.stdErr()).build())
          .withImagePullPolicy(sidecarImagePullPolicy)
          .build();

      final List<Container> socatContainers;
      final String socatCommands;

      if (runSocatInMainContainer) {
        socatContainers = List.of();

        final var socatStdinCmd = usesStdin ? String.format("socat -d -d TCP-L:9001 STDOUT > %s &", STDIN_PIPE_FILE) : "";
        final var socatStdoutCmd = String.format("(cat %s | socat -d -d -t 60 - TCP:%s:%s &)", STDOUT_PIPE_FILE, processRunnerHost, stdoutLocalPort);
        final var socatStderrCmd = String.format("(cat %s | socat -d -d -t 60 - TCP:%s:%s &)", STDERR_PIPE_FILE, processRunnerHost, stderrLocalPort);

        socatCommands = String.join(System.lineSeparator(), socatStdinCmd, socatStdoutCmd, socatStderrCmd);

      } else {
        socatContainers = usesStdin ? List.of(remoteStdin, relayStdout, relayStderr) : List.of(relayStdout, relayStderr);
        socatCommands = "";
      }

      // Note that we pass the resource requirements of the main container for the init.
      // The pod's effective resource requirements is going to be the sum of all final running pods. We
      // could pass that for the init, but it's
      // probably not worth the extra logic to sum everything. Passing what should be the largest
      // requirements should do.
      final Container init = getInit(usesStdin, List.of(pipeVolumeMount, configVolumeMount), busyboxImage, podResourceRequirements.main());
      final Container main = getMain(
          featureFlagClient,
          featureFlagContext,
          image,
          imagePullPolicy,
          usesStdin,
          entrypointOverride,
          List.of(pipeVolumeMount, configVolumeMount, terminationVolumeMount, tmpVolumeMount),
          podResourceRequirements.main(),
          internalToExternalPorts,
          socatCommands,
          envMap,
          args);

      // communicates via a file if it isn't able to reach the heartbeating server and succeeds if the
      // main container completes
      final String heartbeatCommand = MoreResources.readResource("entrypoints/sync/check.sh")
          .replaceAll("TERMINATION_FILE_CHECK", TERMINATION_FILE_CHECK)
          .replaceAll("TERMINATION_FILE_MAIN", TERMINATION_FILE_MAIN)
          .replaceAll("HEARTBEAT_URL", kubeHeartbeatUrl);

      final Container callHeartbeatServer = new ContainerBuilder()
          .withName("call-heartbeat-server")
          .withImage(curlImage)
          .withCommand("sh")
          .withArgs("-c", heartbeatCommand)
          .withVolumeMounts(terminationVolumeMount)
          .withResources(getResourceRequirementsBuilder(podResourceRequirements.heartbeat()).build())
          .withImagePullPolicy(sidecarImagePullPolicy)
          .build();

      final List<Container> containers = Lists.concat(List.of(main, callHeartbeatServer), socatContainers);

      final PodFluent.SpecNested<PodBuilder> podBuilder = new PodBuilder()
          .withApiVersion("v1")
          .withNewMetadata()
          .withName(podName)
          .withLabels(labels)
          .withAnnotations(annotations)
          .endMetadata()
          .withNewSpec()
          .withSchedulerName(schedulerName)
          .withServiceAccount(serviceAccount)
          .withAutomountServiceAccountToken(true);

      final List<LocalObjectReference> pullSecrets = imagePullSecrets
          .stream()
          .map(imagePullSecret -> new LocalObjectReference(imagePullSecret))
          .collect(Collectors.toList());

      final Pod pod = podBuilder.withTolerations(buildPodTolerations(tolerations))
          .withImagePullSecrets(pullSecrets) // An empty list or an empty LocalObjectReference turns this into a no-op setting.
          .withNodeSelector(nodeSelectors)
          .withRestartPolicy("Never")
          .withInitContainers(init)
          .withContainers(containers)
          .withVolumes(pipeVolume, configVolume, terminationVolume, tmpVolume)
          .endSpec()
          .build();

      LOGGER.info("Creating pod {}...", pod.getMetadata().getName());
      val start = System.currentTimeMillis();

      this.podDefinition = fabricClient.pods().inNamespace(namespace).createOrReplace(pod);

      // We want to create a watch before the init container runs. Then we can guarantee
      // that we're checking for updates across the full lifecycle of the main container.
      // This is safe only because we are blocking the init pod until we copy files onto it.
      // See the ExitCodeWatcher comments for more info.
      exitCodeFuture = new CompletableFuture<>();
      podInformer = fabricClient.pods()
          .inNamespace(namespace)
          .withName(pod.getMetadata().getName())
          .inform();
      podInformer.addEventHandler(new ExitCodeWatcher(
          pod.getMetadata().getName(),
          namespace,
          exitCodeFuture::complete,
          () -> {
            LOGGER.info(prependPodInfo(
                String.format(
                    "Exit code watcher failed to retrieve the exit code. Defaulting to %s. This is expected if the job was cancelled.",
                    KILLED_EXIT_CODE),
                namespace,
                podName));

            exitCodeFuture.complete(KILLED_EXIT_CODE);
          }));

      waitForInitPodToRun(fabricClient, podDefinition);

      LOGGER.info("Copying files...");
      copyFilesToKubeConfigVolume(fabricClient, podDefinition, files);

      LOGGER.info("Waiting until pod is ready...");
      // If a pod gets into a non-terminal error state it should be automatically killed by our
      // heartbeating mechanism.
      // This also handles the case where a very short pod already completes before this check completes
      // the first time.
      // This doesn't manage things like pods that are blocked from running for some cluster reason or if
      // the init
      // container got stuck somehow.
      fabricClient.resource(podDefinition).waitUntilCondition(p -> {
        final boolean isReady = Objects.nonNull(p) && Readiness.getInstance().isReady(p);
        final boolean isTerminal = Objects.nonNull(p) && KubePodResourceHelper.isTerminal(p);
        return isReady || isTerminal;
      }, POD_READY_TIMEOUT.toMinutes(), TimeUnit.MINUTES);
      MetricClientFactory.getMetricClient().distribution(OssMetricsRegistry.KUBE_POD_PROCESS_CREATE_TIME_MILLISECS,
          System.currentTimeMillis() - start);

      // allow writing stdin to pod
      LOGGER.info("Reading pod IP...");
      final var podIp = getPodIP(fabricClient, podName, namespace);
      LOGGER.info("Pod IP: {}", podIp);

      if (usesStdin) {
        LOGGER.info("Creating stdin socket...");
        final var socketToDestStdIo = new Socket(podIp, STDIN_REMOTE_PORT);
        this.stdin = socketToDestStdIo.getOutputStream();
      } else {
        LOGGER.info("Using null stdin output stream...");
        this.stdin = NullOutputStream.NULL_OUTPUT_STREAM;
      }
    } catch (final Exception e) {
      // We need to make sure the ports are offered back
      cleanup();
      throw e; // Throw the exception again to inform the caller
    }
  }

  private void setupStdOutAndStdErrListeners() {
    final var context = MDC.getCopyOfContextMap();
    executorService.submit(() -> {
      MDC.setContextMap(context);
      try {
        LOGGER.info("Creating stdout socket server...");
        final var socket = stdoutServerSocket.accept(); // blocks until connected
        // cat /proc/sys/net/ipv4/tcp_keepalive_time
        // 300
        // cat /proc/sys/net/ipv4/tcp_keepalive_probes
        // 5
        // cat /proc/sys/net/ipv4/tcp_keepalive_intvl
        // 60
        socket.setKeepAlive(true);
        LOGGER.info("Setting stdout...");
        this.stdout = socket.getInputStream();
      } catch (final IOException e) {
        e.printStackTrace(); // todo: propagate exception / join at the end of constructor
      }
    });
    executorService.submit(() -> {
      MDC.setContextMap(context);
      try {
        LOGGER.info("Creating stderr socket server...");
        final var socket = stderrServerSocket.accept(); // blocks until connected
        socket.setKeepAlive(true);
        LOGGER.info("Setting stderr...");
        this.stderr = socket.getInputStream();
      } catch (final IOException e) {
        e.printStackTrace(); // todo: propagate exception / join at the end of constructor
      }
    });
  }

  /**
   * Waits for the Kube Pod backing this process and returns the exit value after closing resources.
   */
  @Override
  public int waitFor() throws InterruptedException {
    try {
      exitCodeFuture.get();
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }

    return exitValue();
  }

  /**
   * Immediately terminates the Kube Pod backing this process and cleans up IO resources.
   */
  @Override
  public void destroy() {
    cleanup();
  }

  /**
   * cleanup destroys and returns any resources it has consumed this is a private method so that the
   * constructor may call it.
   */
  private void cleanup() {
    if (this.podDefinition == null) {
      // no pod to destroy; just close resources
      close();
      return;
    }
    final String podName = podDefinition.getMetadata().getName();
    final String podNamespace = podDefinition.getMetadata().getNamespace();

    LOGGER.info(prependPodInfo("Destroying Kube process.", podNamespace, podName));
    try {
      fabricClient.resource(podDefinition).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
      exitCodeFuture.complete(KILLED_EXIT_CODE);
    } finally {
      close();
      LOGGER.info(prependPodInfo("Destroyed Kube process.", podNamespace, podName));
    }
  }

  private Container getMainContainerFromPodDefinition() {
    final Optional<Container> containerOptional = podDefinition.getSpec()
        .getContainers()
        .stream()
        .filter(c -> MAIN_CONTAINER_NAME.equals(c.getName()))
        .findFirst();
    if (containerOptional.isEmpty()) {
      LOGGER.warn(String.format("Could not find main container definition for pod: %s", podDefinition.getMetadata().getName()));
      return null;
    } else {
      return containerOptional.get();
    }
  }

  @Override
  public KubePodInfo getInfo() {
    final Container mainContainer = getMainContainerFromPodDefinition();
    final KubeContainerInfo mainContainerInfo = new KubeContainerInfo(mainContainer.getImage(), mainContainer.getImagePullPolicy());
    return new KubePodInfo(podDefinition.getMetadata().getNamespace(),
        podDefinition.getMetadata().getName(),
        mainContainerInfo);
  }

  /**
   * Close all open resource in the opposite order of resource creation.
   * <p>
   * Null checks exist because certain local Kube clusters (e.g. Docker for Desktop) back this
   * implementation with OS processes and resources, which are automatically reaped by the OS.
   */
  private void close() {
    final boolean previouslyClosed = wasClosed.getAndSet(true);

    // short-circuit if close was already called, so we don't re-offer ports multiple times
    // since the offer call is non-atomic
    if (previouslyClosed) {
      return;
    }

    if (this.stdin != null) {
      Exceptions.swallow(this.stdin::close);
    }

    if (this.stdout != null) {
      Exceptions.swallow(this.stdout::close);
    }

    if (this.stderr != null) {
      Exceptions.swallow(this.stderr::close);
    }

    if (this.stdoutServerSocket != null) {
      Exceptions.swallow(this.stdoutServerSocket::close);
    }
    if (this.stderrServerSocket != null) {
      Exceptions.swallow(this.stderrServerSocket::close);
    }
    if (this.podInformer != null) {
      Exceptions.swallow(this.podInformer::close);
    }
    if (this.executorService != null) {
      Exceptions.swallow(this.executorService::shutdownNow);
    }

    KubePortManagerSingleton.getInstance().offer(stdoutLocalPort);
    KubePortManagerSingleton.getInstance().offer(stderrLocalPort);

    if (podDefinition != null) {
      LOGGER.info(prependPodInfo("Closed all resources for pod", podDefinition.getMetadata().getNamespace(), podDefinition.getMetadata().getName()));
    } else {
      LOGGER.error("Unexpected: PodDefinition is null when closing in KubePodProcess. Log an error to help debugging but not blocking the process.");
    }
  }

  private int getReturnCode() {
    if (exitCodeFuture.isDone()) {
      try {
        return exitCodeFuture.get();
      } catch (final InterruptedException | ExecutionException e) {
        throw new RuntimeException(
            prependPodInfo("Cannot find pod %s : %s while trying to retrieve exit code. This probably means the pod was not correctly created.",
                podDefinition.getMetadata().getNamespace(),
                podDefinition.getMetadata().getName()),
            e);
      }
    } else {
      throw new IllegalThreadStateException(prependPodInfo("Main container in kube pod has not terminated yet.",
          podDefinition.getMetadata().getNamespace(),
          podDefinition.getMetadata().getName()));
    }
  }

  @Override
  public int exitValue() {
    // getReturnCode throws IllegalThreadException if the Kube pod has not exited;
    // close() is only called if the Kube pod has terminated.
    final var returnCode = getReturnCode();
    // The OS traditionally handles process resource clean up. Therefore an exit code of 0, also
    // indicates that all kernel resources were shut down.
    // Because this is a custom implementation, manually close all the resources.
    // Further, since the local resources are used to talk to Kubernetes resources, shut local resources
    // down after Kubernetes resources are shut down, regardless of Kube termination status.
    close();

    return returnCode;
  }

  @Override
  public Process toProcess() {
    return new Process() {

      @Override
      public OutputStream getOutputStream() {
        return KubePodProcess.this.stdin;
      }

      @Override
      public InputStream getInputStream() {
        return KubePodProcess.this.stdout;
      }

      @Override
      public InputStream getErrorStream() {
        return KubePodProcess.this.stderr;
      }

      @Override
      public int waitFor() throws InterruptedException {
        return KubePodProcess.this.waitFor();
      }

      @Override
      public int exitValue() {
        return KubePodProcess.this.exitValue();
      }

      @Override
      public void destroy() {
        KubePodProcess.this.destroy();
      }

      @Override
      public Info info() {
        return new KubePodProcessInfo(podDefinition.getMetadata().getName());
      }

    };
  }

  /**
   * Get resource requirements builder.
   *
   * @param resourceRequirements resource requirements
   * @return builder
   */
  public static ResourceRequirementsBuilder getResourceRequirementsBuilder(final ResourceRequirements resourceRequirements) {
    if (resourceRequirements != null) {
      Quantity cpuLimit = null;
      Quantity memoryLimit = null;
      final Map<String, Quantity> limitMap = new HashMap<>();
      if (!com.google.common.base.Strings.isNullOrEmpty(resourceRequirements.getCpuLimit())) {
        cpuLimit = Quantity.parse(resourceRequirements.getCpuLimit());
        limitMap.put("cpu", cpuLimit);
      }
      if (!com.google.common.base.Strings.isNullOrEmpty(resourceRequirements.getMemoryLimit())) {
        memoryLimit = Quantity.parse(resourceRequirements.getMemoryLimit());
        limitMap.put("memory", memoryLimit);
      }
      final Map<String, Quantity> requestMap = new HashMap<>();
      // if null then use unbounded resource allocation
      if (!com.google.common.base.Strings.isNullOrEmpty(resourceRequirements.getCpuRequest())) {
        final Quantity cpuRequest = Quantity.parse(resourceRequirements.getCpuRequest());
        requestMap.put("cpu", min(cpuRequest, cpuLimit));
      }
      if (!com.google.common.base.Strings.isNullOrEmpty(resourceRequirements.getMemoryRequest())) {
        final Quantity memoryRequest = Quantity.parse(resourceRequirements.getMemoryRequest());
        requestMap.put("memory", min(memoryRequest, memoryLimit));
      }
      return new ResourceRequirementsBuilder()
          .withRequests(requestMap)
          .withLimits(limitMap);
    }
    return new ResourceRequirementsBuilder();
  }

  private static Quantity min(final Quantity request, final Quantity limit) {
    if (limit == null) {
      return request;
    }
    if (request == null) {
      return limit;
    }
    if (request.getNumericalAmount().compareTo(limit.getNumericalAmount()) <= 0) {
      return request;
    } else {
      log.info("Invalid resource requirements detected, requested {} while limit is {}, falling back to requesting {}.", request, limit, limit);
      return limit;
    }
  }

  private static String prependPodInfo(final String message, final String podNamespace, final String podName) {
    return String.format("(pod: %s / %s) - %s", podNamespace, podName, message);
  }

}
