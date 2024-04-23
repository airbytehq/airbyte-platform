package io.airbyte.workload.launcher.pods

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.commons.constants.WorkerConstants.KubeConstants.FULL_POD_TIMEOUT
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_REPLICATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WAIT_DESTINATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WAIT_ORCHESTRATOR_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WAIT_SOURCE_OPERATION_NAME
import io.airbyte.workload.launcher.model.setDestinationLabels
import io.airbyte.workload.launcher.model.setSourceLabels
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
import io.airbyte.workload.launcher.pods.factories.OrchestratorPodFactory
import io.fabric8.kubernetes.api.model.Pod
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.UUID
import kotlin.time.TimeSource

/**
 * Interface layer between domain and Kube layers.
 * Composes raw Kube layer atomic operations to perform business operations.
 */
@Singleton
@Requires(env = [Environment.KUBERNETES])
class KubePodClient(
  private val kubePodLauncher: KubePodLauncher,
  private val labeler: PodLabeler,
  private val mapper: PayloadKubeInputMapper,
  private val featureFlagClient: FeatureFlagClient,
  private val orchestratorPodFactory: OrchestratorPodFactory,
  @Named("checkPodFactory") private val checkPodFactory: ConnectorPodFactory,
  @Named("discoverPodFactory") private val discoverPodFactory: ConnectorPodFactory,
  @Named("specPodFactory") private val specPodFactory: ConnectorPodFactory,
) : PodClient {
  override fun podsExistForAutoId(autoId: UUID): Boolean {
    return kubePodLauncher.podsExist(labeler.getAutoIdLabels(autoId))
  }

  @Trace(operationName = LAUNCH_REPLICATION_OPERATION_NAME)
  override fun launchReplication(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  ) {
    val sharedLabels = labeler.getSharedLabels(launcherInput.workloadId, launcherInput.mutexKey, launcherInput.labels, launcherInput.autoId)

    val inputWithLabels =
      replicationInput
        .setSourceLabels(sharedLabels)
        .setDestinationLabels(sharedLabels)

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, inputWithLabels, sharedLabels)

    var pod =
      orchestratorPodFactory.create(
        replicationInput.connectionId,
        kubeInput.orchestratorLabels,
        kubeInput.resourceReqs,
        kubeInput.nodeSelectors,
        kubeInput.kubePodInfo,
        kubeInput.annotations,
        mapOf(),
      )
    try {
      pod =
        kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Failed to create pod ${kubeInput.kubePodInfo.name}.",
        e,
        KubeCommandType.CREATE,
        PodType.ORCHESTRATOR,
      )
    }

    waitOrchestratorPodInit(pod)

    copyFileToOrchestrator(kubeInput, pod)

    waitForOrchestratorStart(pod)

    // We wait for the destination first because orchestrator starts destinations first.
    waitDestinationReadyOrTerminalInit(kubeInput)

    if (!replicationInput.isReset) {
      waitSourceReadyOrTerminalInit(kubeInput)
    }
  }

  @Trace(operationName = WAIT_ORCHESTRATOR_OPERATION_NAME)
  fun waitOrchestratorPodInit(orchestratorPod: Pod) {
    try {
      kubePodLauncher.waitForPodInit(orchestratorPod, POD_INIT_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Init container of orchestrator pod failed to start within allotted timeout of ${POD_INIT_TIMEOUT_VALUE.seconds} seconds. " +
          "(${e.message})",
        e,
        KubeCommandType.WAIT_INIT,
        PodType.ORCHESTRATOR,
      )
    }
  }

  @Trace(operationName = WAIT_ORCHESTRATOR_OPERATION_NAME)
  fun copyFileToOrchestrator(
    kubeInput: OrchestratorKubeInput,
    pod: Pod,
  ) {
    try {
      kubePodLauncher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Failed to copy files to orchestrator pod ${kubeInput.kubePodInfo.name}. (${e.message})",
        e,
        KubeCommandType.COPY,
        PodType.ORCHESTRATOR,
      )
    }
  }

  @Trace(operationName = WAIT_ORCHESTRATOR_OPERATION_NAME)
  fun waitForOrchestratorStart(pod: Pod) {
    try {
      kubePodLauncher.waitForPodReadyOrTerminalByPod(pod, ORCHESTRATOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Main container of orchestrator pod failed to start within allotted timeout of ${ORCHESTRATOR_STARTUP_TIMEOUT_VALUE.seconds} seconds. " +
          "(${e.message})",
        e,
        KubeCommandType.WAIT_MAIN,
        PodType.ORCHESTRATOR,
      )
    }
  }

  @Trace(operationName = WAIT_SOURCE_OPERATION_NAME)
  fun waitSourceReadyOrTerminalInit(kubeInput: OrchestratorKubeInput) {
    try {
      kubePodLauncher.waitForPodReadyOrTerminal(kubeInput.sourceLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Source pod failed to start within allotted timeout of ${REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE.seconds} seconds. (${e.message})",
        e,
        KubeCommandType.WAIT_MAIN,
        PodType.SOURCE,
      )
    }
  }

  @Trace(operationName = WAIT_DESTINATION_OPERATION_NAME)
  fun waitDestinationReadyOrTerminalInit(kubeInput: OrchestratorKubeInput) {
    try {
      kubePodLauncher.waitForPodReadyOrTerminal(kubeInput.destinationLabels, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Destination pod failed to start within allotted timeout of ${REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE.seconds} seconds. (${e.message})",
        e,
        KubeCommandType.WAIT_MAIN,
        PodType.DESTINATION,
      )
    }
  }

  override fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  ) {
    // For check the workload id is too long to be store as a kube label thus it is not added
    val sharedLabels =
      labeler.getSharedLabels(
        workloadId = null,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
      )

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, checkInput, sharedLabels)

    launchConnectorWithSidecar(kubeInput, checkPodFactory, launcherInput.workloadType.toOperationName())
  }

  override fun launchDiscover(
    discoverCatalogInput: DiscoverCatalogInput,
    launcherInput: LauncherInput,
  ) {
    // For discover the workload id is too long to be store as a kube label thus it is not added
    val sharedLabels =
      labeler.getSharedLabels(
        workloadId = null,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
      )

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, discoverCatalogInput, sharedLabels)

    launchConnectorWithSidecar(kubeInput, discoverPodFactory, launcherInput.workloadType.toOperationName())
  }

  override fun launchSpec(
    specInput: SpecInput,
    launcherInput: LauncherInput,
  ) {
    // For spec the workload id is too long to be store as a kube label thus it is not added
    val sharedLabels =
      labeler.getSharedLabels(
        workloadId = null,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
      )

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, specInput, sharedLabels)

    launchConnectorWithSidecar(kubeInput, specPodFactory, launcherInput.workloadType.toOperationName())
  }

  @VisibleForTesting
  fun launchConnectorWithSidecar(
    kubeInput: ConnectorKubeInput,
    factory: ConnectorPodFactory,
    podLogLabel: String,
  ) {
    val start = TimeSource.Monotonic.markNow()

    var pod =
      factory.create(
        kubeInput.connectorLabels,
        kubeInput.nodeSelectors,
        kubeInput.kubePodInfo,
        kubeInput.annotations,
        kubeInput.extraEnv,
      )
    try {
      pod = kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Failed to create pod ${kubeInput.kubePodInfo.name}.",
        e,
        KubeCommandType.CREATE,
      )
    }

    try {
      kubePodLauncher.waitForPodInit(pod, POD_INIT_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "$podLogLabel pod failed to init within allotted timeout.",
        e,
        KubeCommandType.WAIT_INIT,
      )
    }

    try {
      kubePodLauncher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Failed to copy files to $podLogLabel pod ${kubeInput.kubePodInfo.name}.",
        e,
        KubeCommandType.COPY,
      )
    }

    try {
      kubePodLauncher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "$podLogLabel pod failed to start within allotted timeout.",
        e,
        KubeCommandType.WAIT_MAIN,
      )
    }

    println("ELAPSED TIME (SIDECAR): ${start.elapsedNow()}")
  }

  override fun deleteMutexPods(mutexKey: String): Boolean {
    val labels = labeler.getMutexLabels(mutexKey)

    try {
      val deleted = kubePodLauncher.deleteActivePods(labels)

      return deleted.isNotEmpty()
    } catch (e: RuntimeException) {
      throw KubeClientException(
        "Failed to delete pods for mutex key: $mutexKey.",
        e,
        KubeCommandType.DELETE,
      )
    }
  }

  companion object {
    private val TIMEOUT_SLACK: Duration = Duration.ofSeconds(5)
    val ORCHESTRATOR_STARTUP_TIMEOUT_VALUE: Duration = Duration.ofMinutes(1)
    val POD_INIT_TIMEOUT_VALUE: Duration = Duration.ofMinutes(15)
    val REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE: Duration = FULL_POD_TIMEOUT.plus(TIMEOUT_SLACK)
  }
}
