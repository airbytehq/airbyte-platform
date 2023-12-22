package io.airbyte.workload.launcher.pods

import io.airbyte.commons.io.IOs
import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseCustomK8sScheduler
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubePodProcess
import io.airbyte.workers.process.KubePodResourceHelper
import io.airbyte.workload.launcher.pods.OrchestratorPodLauncher.Constants.KUBECTL_COMPLETED_VALUE
import io.airbyte.workload.launcher.pods.OrchestratorPodLauncher.Constants.KUBECTL_PHASE_FIELD_NAME
import io.airbyte.workload.launcher.pods.OrchestratorPodLauncher.Constants.MAX_DELETION_TIMEOUT
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.ContainerPort
import io.fabric8.kubernetes.api.model.DeletionPropagation
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodList
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder
import io.fabric8.kubernetes.api.model.StatusDetails
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeBuilder
import io.fabric8.kubernetes.api.model.VolumeMount
import io.fabric8.kubernetes.api.model.VolumeMountBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.readiness.Readiness
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.nio.file.Path
import java.time.Duration
import java.util.Objects
import java.util.concurrent.TimeUnit
import java.util.function.Predicate

private val logger = KotlinLogging.logger {}

/**
 * Atomic operations on the raw Kube api. Domain level information should be opaque to this layer.
 */
@Singleton
class OrchestratorPodLauncher(
  private val kubernetesClient: KubernetesClient,
  private val featureFlagClient: FeatureFlagClient,
  @Value("\${airbyte.worker.job.kube.namespace}") private val namespace: String?,
  @Value("\${google.application.credentials}") private val googleApplicationCredentials: String?,
  @Value("\${airbyte.container.orchestrator.secret-name}") private val secretName: String?,
  @Value("\${airbyte.container.orchestrator.secret-mount-path}") private val secretMountPath: String?,
  @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-name}") private val dataPlaneCredsSecretName: String?,
  @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-mount-path}") private val dataPlaneCredsSecretMountPath: String?,
  @Value("\${airbyte.worker.job.kube.serviceAccount}") private val serviceAccount: String?,
  @Named("orchestratorEnvVars") private val envVars: List<EnvVar>,
  @Named("orchestratorContainerPorts") private val containerPorts: List<ContainerPort>,
  private val metricClient: MetricClient,
) {
  fun create(
    allLabels: Map<String, String>,
    resourceRequirements: ResourceRequirements?,
    nodeSelectors: Map<String, String>,
    kubePodInfo: KubePodInfo,
    annotations: Map<String, String>,
  ): Pod {
    val volumes: MutableList<Volume> = ArrayList()
    val volumeMounts: MutableList<VolumeMount> = ArrayList()

    volumes.add(
      VolumeBuilder()
        .withName("airbyte-config")
        .withNewEmptyDir()
        .withMedium("Memory")
        .endEmptyDir()
        .build(),
    )

    volumeMounts.add(
      VolumeMountBuilder()
        .withName("airbyte-config")
        .withMountPath(KubePodProcess.CONFIG_DIR)
        .build(),
    )

    if (StringUtils.isNotEmpty(secretName) && StringUtils.isNotEmpty(secretMountPath) && StringUtils.isNotEmpty(googleApplicationCredentials)) {
      volumes.add(
        VolumeBuilder()
          .withName("airbyte-secret")
          .withSecret(
            SecretVolumeSourceBuilder()
              .withSecretName(secretName)
              .withDefaultMode(420)
              .build(),
          )
          .build(),
      )
      volumeMounts.add(
        VolumeMountBuilder()
          .withName("airbyte-secret")
          .withMountPath(secretMountPath)
          .build(),
      )
    }

    if (StringUtils.isNotEmpty(dataPlaneCredsSecretName) && StringUtils.isNotEmpty(dataPlaneCredsSecretMountPath)) {
      volumes.add(
        VolumeBuilder()
          .withName("airbyte-dataplane-creds")
          .withSecret(
            SecretVolumeSourceBuilder()
              .withSecretName(dataPlaneCredsSecretName)
              .withDefaultMode(420)
              .build(),
          )
          .build(),
      )
      volumeMounts.add(
        VolumeMountBuilder()
          .withName("airbyte-dataplane-creds")
          .withMountPath(dataPlaneCredsSecretMountPath)
          .build(),
      )
    }

    val initContainer =
      ContainerBuilder()
        .withName(KubePodProcess.INIT_CONTAINER_NAME)
        .withImage("busybox:1.35")
        .withVolumeMounts(volumeMounts)
        .withCommand(
          listOf(
            "sh",
            "-c",
            String.format(
              """
              i=0
              until [ ${'$'}i -gt 60 ]
              do
                echo "${'$'}i - waiting for config file transfer to complete..."
                # check if the upload-complete file exists, if so exit without error
                if [ -f "%s/%s" ]; then
                  exit 0
                fi
                i=${'$'}((i+1))
                sleep 1
              done
              echo "config files did not transfer in time"
              # no upload-complete file was created in time, exit with error
              exit 1
              
              """.trimIndent(),
              KubePodProcess.CONFIG_DIR,
              KubePodProcess.SUCCESS_FILE_NAME,
            ),
          ),
        )
        .build()

    val mainContainer =
      ContainerBuilder()
        .withName(KubePodProcess.MAIN_CONTAINER_NAME)
        .withImage(kubePodInfo.mainContainerInfo.image)
        .withImagePullPolicy(kubePodInfo.mainContainerInfo.pullPolicy)
        .withResources(KubePodProcess.getResourceRequirementsBuilder(resourceRequirements).build())
        .withEnv(envVars)
        .withPorts(containerPorts)
        .withVolumeMounts(volumeMounts)
        .build()

    // TODO: We should inject the scheduler from the ENV and use this just for overrides
    val schedulerName = featureFlagClient.stringVariation(UseCustomK8sScheduler, Connection(ANONYMOUS))

    val podToCreate =
      PodBuilder()
        .withApiVersion("v1")
        .withNewMetadata()
        .withName(kubePodInfo.name)
        .withNamespace(kubePodInfo.namespace)
        .withLabels<Any, Any>(allLabels)
        .withAnnotations<Any, Any>(annotations)
        .endMetadata()
        .withNewSpec()
        .withSchedulerName(schedulerName)
        .withServiceAccount(serviceAccount)
        .withAutomountServiceAccountToken(true)
        .withRestartPolicy("Never")
        .withContainers(mainContainer)
        .withInitContainers(initContainer)
        .withVolumes(volumes)
        .withNodeSelector<Any, Any>(nodeSelectors)
        .endSpec()
        .build()

    // should only create after the kubernetes API creates the pod
    return runKubeCommand(
      {
        kubernetesClient.pods()
          .inNamespace(kubePodInfo.namespace)
          .resource(podToCreate)
          .serverSideApply()
      },
      "pod_create",
    )
  }

  fun waitForPodInit(
    labels: Map<String, String>,
    waitDuration: Duration,
  ) {
    runKubeCommand(
      {
        kubernetesClient.pods()
          .inNamespace(namespace)
          .withLabels(labels)
          .waitUntilCondition(
            { p: Pod ->
              (
                p.status.initContainerStatuses.isNotEmpty() &&
                  p.status.initContainerStatuses[0].state.waiting == null
              )
            },
            waitDuration.toMinutes(),
            TimeUnit.MINUTES,
          )
      },
      "wait",
    )

    val pods =
      runKubeCommand(
        {
          kubernetesClient.pods()
            .inNamespace(namespace)
            .withLabels(labels)
            .list()
            .items
        },
        "list",
      )

    if (pods.isEmpty()) {
      throw RuntimeException("No pods found for labels: $labels. Nothing to wait for.")
    }

    val containerState =
      pods[0]
        .status
        .initContainerStatuses[0]
        .state

    if (containerState.running == null) {
      throw RuntimeException(
        "Init container for Pod with labels: $labels was not in a running state after: ${waitDuration.toMinutes()} ${TimeUnit.MINUTES}. " +
          "Actual container state: $containerState.",
      )
    }
  }

  fun waitForPodReadyOrTerminal(
    labels: Map<String, String>,
    waitDuration: Duration,
  ) {
    runKubeCommand(
      {
        kubernetesClient.pods()
          .inNamespace(namespace)
          .withLabels(labels)
          .waitUntilCondition(
            { p: Pod? ->
              Objects.nonNull(p) &&
                (Readiness.getInstance().isReady(p) || KubePodResourceHelper.isTerminal(p))
            },
            waitDuration.toMinutes(),
            TimeUnit.MINUTES,
          )
      },
      "wait",
    )
  }

  fun copyFilesToKubeConfigVolumeMain(
    podDefinition: Pod,
    files: Map<String, String>,
  ) {
    val fileEntries: MutableList<Map.Entry<String, String>> = ArrayList(files.entries)

    // copy this file last to indicate that the copy has completed
    fileEntries.add(java.util.AbstractMap.SimpleEntry(KubePodProcess.SUCCESS_FILE_NAME, ""))
    var tmpFile: Path? = null
    var proc: Process? = null
    for ((key, value) in fileEntries) {
      try {
        tmpFile = Path.of(IOs.writeFileToRandomTmpDir(key, value))
        val containerPath = Path.of(KubePodProcess.CONFIG_DIR + "/" + key)

        // using kubectl cp directly here, because both fabric and the official kube client APIs have
        // several issues with copying files. See https://github.com/airbytehq/airbyte/issues/8643 for
        // details.
        val command =
          String.format(
            "kubectl cp %s %s/%s:%s -c %s",
            tmpFile,
            podDefinition.metadata.namespace,
            podDefinition.metadata.name,
            containerPath,
            KubePodProcess.INIT_CONTAINER_NAME,
          )
        proc =
          runKubeCommand(
            {
              Runtime.getRuntime().exec(command)
            },
            "kubectl_cp",
          )
        val exitCode = proc.waitFor()
        if (exitCode != 0) {
          throw IOException("kubectl cp failed with exit code $exitCode")
        }
      } catch (e: IOException) {
        throw RuntimeException(e)
      } catch (e: InterruptedException) {
        throw RuntimeException(e)
      } finally {
        tmpFile?.toFile()?.delete()
        proc?.destroy()
      }
    }
  }

  fun podsExist(labels: Map<String, String>): Boolean {
    try {
      return runKubeCommand(
        {
          kubernetesClient.pods()
            .inNamespace(namespace)
            .withLabels(labels)
            .list()
            .items
            .stream()
            .filter(
              Predicate<Pod> { kubePod: Pod? ->
                !KubePodResourceHelper.isTerminal(
                  kubePod,
                )
              },
            )
            .findAny()
            .isPresent
        },
        "list",
      )
    } catch (e: Exception) {
      logger.warn(e) { "Could not find pods running for $labels, presuming no pods are running" }
      return false
    }
  }

  fun deleteActivePods(labels: Map<String, String>): List<StatusDetails> {
    return runKubeCommand(
      {
        val statuses =
          listActivePods(labels)
            .list()
            .items
            .flatMap { p ->
              kubernetesClient.pods()
                .inNamespace(namespace)
                .resource(p)
                .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                .delete()
            }

        if (statuses.isEmpty()) {
          return@runKubeCommand statuses
        }

        listActivePods(labels)
          .waitUntilCondition(Objects::isNull, MAX_DELETION_TIMEOUT, TimeUnit.SECONDS)

        statuses
      },
      "delete",
    )
  }

  private fun listActivePods(labels: Map<String, String>): FilterWatchListDeletable<Pod, PodList, PodResource> {
    return kubernetesClient.pods()
      .inNamespace(namespace)
      .withLabels(labels)
      .withoutField(KUBECTL_PHASE_FIELD_NAME, KUBECTL_COMPLETED_VALUE) // filters out completed pods
  }

  private fun <T> runKubeCommand(
    kubeCommand: () -> T,
    commandName: String,
  ): T {
    try {
      return kubeCommand()
    } catch (e: Exception) {
      val attributes: List<MetricAttribute> = listOf(MetricAttribute("operation", commandName))
      val attributesArray = attributes.toTypedArray<MetricAttribute>()
      metricClient.count(OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_ERROR, 1, *attributesArray)

      throw e
    }
  }

  object Constants {
    // Wait why is this named like this?
    // Explanation: Kubectl displays "Completed" but the selector expects "Succeeded"
    const val KUBECTL_COMPLETED_VALUE = "Succeeded"
    const val KUBECTL_PHASE_FIELD_NAME = "status.phase"
    const val MAX_DELETION_TIMEOUT = 45L
  }
}
