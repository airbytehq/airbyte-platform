package io.airbyte.workload.launcher.pods

import datadog.trace.api.Trace
import io.airbyte.commons.constants.WorkerConstants.KubeConstants.FULL_POD_TIMEOUT
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.process.KubePodInfo
import io.airbyte.workers.process.KubeProcessFactory.KUBE_NAME_LEN_LIMIT
import io.airbyte.workers.process.ProcessFactory
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_REPLICATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WAIT_DESTINATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WAIT_ORCHESTRATOR_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.WAIT_SOURCE_OPERATION_NAME
import io.airbyte.workload.launcher.model.setConnectorLabels
import io.airbyte.workload.launcher.model.setDestinationLabels
import io.airbyte.workload.launcher.model.setSourceLabels
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.fabric8.kubernetes.api.model.Pod
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
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
  private val orchestratorLauncher: OrchestratorPodLauncher,
  private val connectorLauncher: ConnectorPodLauncher,
  private val labeler: PodLabeler,
  private val mapper: PayloadKubeInputMapper,
) : PodClient {
  override fun podsExistForAutoId(autoId: UUID): Boolean {
    return orchestratorLauncher.podsExist(labeler.getAutoIdLabels(autoId))
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

    val pod: Pod
    try {
      pod =
        orchestratorLauncher.create(
          kubeInput.orchestratorLabels,
          kubeInput.resourceReqs,
          kubeInput.nodeSelectors,
          kubeInput.kubePodInfo,
          kubeInput.annotations,
          mapOf(),
        )
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to create pod ${kubeInput.kubePodInfo.name}.",
        e,
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
      orchestratorLauncher.waitForPodInit(orchestratorPod, ORCHESTRATOR_INIT_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Init container of orchestrator pod failed to start within allotted timeout of ${ORCHESTRATOR_INIT_TIMEOUT_VALUE.seconds} seconds. " +
          "(${e.message})",
        e,
      )
    }
  }

  @Trace(operationName = WAIT_ORCHESTRATOR_OPERATION_NAME)
  fun copyFileToOrchestrator(
    kubeInput: ReplicationOrchestratorKubeInput,
    pod: Pod,
  ) {
    try {
      orchestratorLauncher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to copy files to orchestrator pod ${kubeInput.kubePodInfo.name}. (${e.message})",
        e,
      )
    }
  }

  @Trace(operationName = WAIT_ORCHESTRATOR_OPERATION_NAME)
  fun waitForOrchestratorStart(pod: Pod) {
    try {
      orchestratorLauncher.waitForPodReadyOrTerminalByPod(pod, ORCHESTRATOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Main container of orchestrator pod failed to start within allotted timeout of ${ORCHESTRATOR_STARTUP_TIMEOUT_VALUE.seconds} seconds. " +
          "(${e.message})",
        e,
      )
    }
  }

  @Trace(operationName = WAIT_SOURCE_OPERATION_NAME)
  fun waitSourceReadyOrTerminalInit(kubeInput: ReplicationOrchestratorKubeInput) {
    try {
      orchestratorLauncher.waitForPodReadyOrTerminal(kubeInput.sourceLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Source pod failed to start within allotted timeout of ${CONNECTOR_STARTUP_TIMEOUT_VALUE.seconds} seconds. (${e.message})",
        e,
      )
    }
  }

  @Trace(operationName = WAIT_DESTINATION_OPERATION_NAME)
  fun waitDestinationReadyOrTerminalInit(kubeInput: ReplicationOrchestratorKubeInput) {
    try {
      orchestratorLauncher.waitForPodReadyOrTerminal(kubeInput.destinationLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Destination pod failed to start within allotted timeout of ${CONNECTOR_STARTUP_TIMEOUT_VALUE.seconds} seconds. (${e.message})",
        e,
      )
    }
  }

  override fun launchCheck(
    checkInput: CheckConnectionInput,
    launcherInput: LauncherInput,
  ) {
    // TODO: feature flag?
    if (true) {
      return launchCheckWithSidecar(checkInput, launcherInput)
    }

    // For check the workload id is too long to be store as a kube label thus it is not added
    val sharedLabels =
      labeler.getSharedLabels(
        workloadId = null,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
      )

    val inputWithLabels = checkInput.setConnectorLabels(sharedLabels)

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, inputWithLabels, sharedLabels)

    val start = TimeSource.Monotonic.markNow()

    val pod: Pod
    try {
      pod =
        orchestratorLauncher.create(
          kubeInput.orchestratorLabels,
          kubeInput.resourceReqs,
          kubeInput.nodeSelectors,
          kubeInput.kubePodInfo,
          kubeInput.annotations,
          mapOf(),
        )
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to create pod ${kubeInput.kubePodInfo.name}.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodInit(pod, ORCHESTRATOR_INIT_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Orchestrator pod failed to start within allotted timeout.",
        e,
      )
    }

    try {
      orchestratorLauncher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to copy files to orchestrator pod ${kubeInput.kubePodInfo.name}.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodReadyOrTerminal(kubeInput.connectorLabels, CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Connector pod failed to start within allotted timeout.",
        e,
      )
    }
    println("ELAPSED TIME (ORCHESTRATOR): ${start.elapsedNow()}")
  }

  fun launchCheckWithSidecar(
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

    val podName =
      ProcessFactory.createProcessName(
        checkInput.launcherConfig.dockerImage,
        "check",
        checkInput.jobRunConfig.jobId,
        checkInput.jobRunConfig.attemptId.toInt(),
        KUBE_NAME_LEN_LIMIT,
      )

    val connectorKubePodInfo =
      KubePodInfo(
        kubeInput.kubePodInfo.namespace,
        podName,
        KubeContainerInfo(
          checkInput.launcherConfig.dockerImage,
          "",
        ),
      )

    val start = TimeSource.Monotonic.markNow()

    val pod: Pod
    try {
      pod =
        connectorLauncher.create(
          kubeInput.connectorLabels,
          kubeInput.nodeSelectors,
          connectorKubePodInfo,
          kubeInput.annotations,
        )
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to create pod ${connectorKubePodInfo.name}.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodInit(pod, ORCHESTRATOR_INIT_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Check pod failed to init within allotted timeout.",
        e,
      )
    }

    try {
      orchestratorLauncher.copyFilesToKubeConfigVolumeMain(pod, kubeInput.fileMap)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Failed to copy files to check pod ${kubeInput.kubePodInfo.name}.",
        e,
      )
    }

    try {
      orchestratorLauncher.waitForPodReadyOrTerminalByPod(pod, CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubePodInitException(
        "Check pod failed to start within allotted timeout.",
        e,
      )
    }

    println("ELAPSED TIME (SIDECAR): ${start.elapsedNow()}")
  }

  override fun deleteMutexPods(mutexKey: String): Boolean {
    val labels = labeler.getMutexLabels(mutexKey)
    val deleted = orchestratorLauncher.deleteActivePods(labels)

    return deleted.isNotEmpty()
  }

  companion object {
    private val TIMEOUT_SLACK: Duration = Duration.ofSeconds(5)
    val CONNECTOR_STARTUP_TIMEOUT_VALUE: Duration = FULL_POD_TIMEOUT.plus(TIMEOUT_SLACK)
    val ORCHESTRATOR_INIT_TIMEOUT_VALUE: Duration = Duration.ofMinutes(15)
    val ORCHESTRATOR_STARTUP_TIMEOUT_VALUE: Duration = Duration.ofMinutes(1)
  }
}
