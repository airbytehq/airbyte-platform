package io.airbyte.workload.launcher.pods

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.commons.constants.WorkerConstants.KubeConstants.FULL_POD_TIMEOUT
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.KubeClientException
import io.airbyte.workers.exception.KubeCommandType
import io.airbyte.workers.exception.PodType
import io.airbyte.workers.exception.ResourceConstraintException
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.pod.PodLabeler
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_REPLICATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_RESET_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
import io.airbyte.workload.launcher.pods.factories.ReplicationPodFactory
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger {}

/**
 * Interface layer between domain and Kube layers.
 * Composes raw Kube layer atomic operations to perform business operations.
 */
@Singleton
class KubePodClient(
  private val kubePodLauncher: KubePodLauncher,
  private val labeler: PodLabeler,
  private val mapper: PayloadKubeInputMapper,
  private val replicationPodFactory: ReplicationPodFactory,
  @Named("checkPodFactory") private val checkPodFactory: ConnectorPodFactory,
  @Named("discoverPodFactory") private val discoverPodFactory: ConnectorPodFactory,
  @Named("specPodFactory") private val specPodFactory: ConnectorPodFactory,
) {
  fun podsExistForAutoId(autoId: UUID): Boolean {
    return kubePodLauncher.podsRunning(labeler.getAutoIdLabels(autoId))
  }

  @Trace(operationName = LAUNCH_REPLICATION_OPERATION_NAME)
  fun launchReplication(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  ) {
    val sharedLabels =
      labeler.getSharedLabels(
        launcherInput.workloadId,
        launcherInput.mutexKey,
        launcherInput.labels,
        launcherInput.autoId,
        replicationInput.workspaceId,
        replicationInput.networkSecurityTokens,
      )

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, replicationInput, sharedLabels)
    var pod =
      replicationPodFactory.create(
        kubeInput.podName,
        kubeInput.labels,
        kubeInput.annotations,
        kubeInput.nodeSelectors,
        kubeInput.orchestratorImage,
        kubeInput.sourceImage,
        kubeInput.destinationImage,
        kubeInput.orchestratorReqs,
        kubeInput.sourceReqs,
        kubeInput.destinationReqs,
        kubeInput.orchestratorRuntimeEnvVars,
        kubeInput.sourceRuntimeEnvVars,
        kubeInput.destinationRuntimeEnvVars,
        replicationInput.useFileTransfer,
        replicationInput.workspaceId,
      )

    logger.info { "Launching replication pod: ${kubeInput.podName} with containers:" }
    logger.info { "[source] image: ${kubeInput.sourceImage} resources: ${kubeInput.sourceReqs}" }
    logger.info { "[destination] image: ${kubeInput.destinationImage} resources: ${kubeInput.destinationReqs}" }
    logger.info { "[orchestrator] image: ${kubeInput.orchestratorImage} resources: ${kubeInput.orchestratorReqs}" }

    try {
      pod =
        kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Failed to create pod ${kubeInput.podName}.",
        e,
        KubeCommandType.CREATE,
        PodType.REPLICATION,
      )
    }

    // NOTE: might not be necessary depending on when `serversideApply` returns.
    // If it blocks until it moves from PENDING, then we are good. Otherwise, we
    // need this or something similar to wait for the pod to be running on the node.
    waitForPodInitComplete(pod, PodType.REPLICATION.toString())
  }

  @Trace(operationName = LAUNCH_RESET_OPERATION_NAME)
  fun launchReset(
    replicationInput: ReplicationInput,
    launcherInput: LauncherInput,
  ) {
    val sharedLabels =
      labeler.getSharedLabels(
        launcherInput.workloadId,
        launcherInput.mutexKey,
        launcherInput.labels,
        launcherInput.autoId,
        replicationInput.workspaceId,
        replicationInput.networkSecurityTokens,
      )
    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, replicationInput, sharedLabels)

    var pod =
      replicationPodFactory.createReset(
        kubeInput.podName,
        kubeInput.labels,
        kubeInput.annotations,
        kubeInput.nodeSelectors,
        kubeInput.orchestratorImage,
        kubeInput.destinationImage,
        kubeInput.orchestratorReqs,
        kubeInput.destinationReqs,
        kubeInput.orchestratorRuntimeEnvVars,
        kubeInput.destinationRuntimeEnvVars,
        replicationInput.useFileTransfer,
        replicationInput.workspaceId,
      )

    logger.info { "Launching reset pod: ${kubeInput.podName} with containers:" }
    logger.info { "[destination] image: ${kubeInput.destinationImage} resources: ${kubeInput.destinationReqs}" }
    logger.info { "[orchestrator] image: ${kubeInput.orchestratorImage} resources: ${kubeInput.orchestratorReqs}" }

    try {
      pod =
        kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        "Failed to create pod ${kubeInput.podName}.",
        e,
        KubeCommandType.CREATE,
        PodType.RESET,
      )
    }

    // NOTE: might not be necessary depending on when `serversideApply` returns.
    // If it blocks until it moves from PENDING, then we are good. Otherwise, we
    // need this or something similar to wait for the pod to be running on the node.
    waitForPodInitComplete(pod, PodType.REPLICATION.toString())
  }

  fun launchCheck(
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
        workspaceId = checkInput.launcherConfig.workspaceId,
        networkSecurityTokens = checkInput.checkConnectionInput.networkSecurityTokens,
      )
    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, checkInput, sharedLabels)

    launchConnectorWithSidecar(kubeInput, checkPodFactory, launcherInput.workloadType.toOperationName())
  }

  fun launchDiscover(
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
        workspaceId = discoverCatalogInput.launcherConfig.workspaceId,
        networkSecurityTokens = discoverCatalogInput.discoverCatalogInput.networkSecurityTokens,
      )

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, discoverCatalogInput, sharedLabels)

    launchConnectorWithSidecar(kubeInput, discoverPodFactory, launcherInput.workloadType.toOperationName())
  }

  fun launchSpec(
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
        null,
        emptyList(),
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
    var pod =
      factory.create(
        kubeInput.connectorLabels,
        kubeInput.nodeSelectors,
        kubeInput.kubePodInfo,
        kubeInput.annotations,
        kubeInput.connectorReqs,
        kubeInput.initReqs,
        kubeInput.runtimeEnvVars,
        kubeInput.workspaceId,
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

    waitForPodInitComplete(pod, podLogLabel)

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
  }

  fun deleteMutexPods(mutexKey: String): Boolean {
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

  @VisibleForTesting
  fun waitForPodInitComplete(
    pod: Pod,
    podLogLabel: String,
  ) {
    try {
      kubePodLauncher.waitForPodInitComplete(pod, POD_INIT_TIMEOUT_VALUE)
    } catch (e: Exception) {
      when (e) {
        is TimeoutException, is KubernetesClientTimeoutException -> {
          ApmTraceUtils.addExceptionToTrace(e)
          throw ResourceConstraintException(
            "Unable to start the $podLogLabel pod. This may be due to insufficient system resources. Please check available resources and try again.",
            e,
            KubeCommandType.WAIT_INIT,
          )
        } else -> throw e
      }
    }
  }

  companion object {
    private val TIMEOUT_SLACK: Duration = Duration.ofSeconds(5)
    val POD_INIT_TIMEOUT_VALUE: Duration = Duration.ofMinutes(15)
    val REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE: Duration = FULL_POD_TIMEOUT.plus(TIMEOUT_SLACK)
  }
}
