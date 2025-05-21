/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.constants.WorkerConstants.KubeConstants.FULL_POD_TIMEOUT
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.EnableAsyncProfiler
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.exception.KubeClientException
import io.airbyte.workers.exception.KubeCommandType
import io.airbyte.workers.exception.PodType
import io.airbyte.workers.exception.ResourceConstraintException
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.ArchitectureDecider
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_REPLICATION_OPERATION_NAME
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_RESET_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
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
  private val featureFlagClient: FeatureFlagClient,
  @Named("checkPodFactory") private val checkPodFactory: ConnectorPodFactory,
  @Named("discoverPodFactory") private val discoverPodFactory: ConnectorPodFactory,
  @Named("specPodFactory") private val specPodFactory: ConnectorPodFactory,
) {
  fun podsExistForAutoId(autoId: UUID): Boolean = kubePodLauncher.podsRunning(labeler.getAutoIdLabels(autoId))

  @Trace(operationName = LAUNCH_REPLICATION_OPERATION_NAME)
  fun launchReplication(
    payload: SyncPayload,
    launcherInput: LauncherInput,
  ) {
    val replicationInput = payload.input
    val sharedLabels =
      labeler.getSharedLabels(
        workloadId = launcherInput.workloadId,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
        workspaceId = replicationInput.workspaceId,
        networkSecurityTokens = replicationInput.networkSecurityTokens,
      )

    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, payload, sharedLabels)
    val enableAsyncProfiler = featureFlagClient.boolVariation(EnableAsyncProfiler, Connection(replicationInput.connectionId))
    var pod =
      replicationPodFactory.create(
        podName = kubeInput.podName,
        allLabels = kubeInput.labels,
        annotations = kubeInput.annotations,
        nodeSelectors = kubeInput.nodeSelectors,
        orchImage = kubeInput.orchestratorImage,
        sourceImage = kubeInput.sourceImage,
        destImage = kubeInput.destinationImage,
        orchResourceReqs = kubeInput.orchestratorReqs,
        sourceResourceReqs = kubeInput.sourceReqs,
        destResourceReqs = kubeInput.destinationReqs,
        orchRuntimeEnvVars = kubeInput.orchestratorRuntimeEnvVars,
        sourceRuntimeEnvVars = kubeInput.sourceRuntimeEnvVars,
        destRuntimeEnvVars = kubeInput.destinationRuntimeEnvVars,
        isFileTransfer = replicationInput.useFileTransfer,
        workspaceId = replicationInput.workspaceId,
        enableAsyncProfiler = enableAsyncProfiler,
        architectureEnvironmentVariables = payload.architectureEnvironmentVariables ?: ArchitectureDecider.buildLegacyEnvironment(),
      )

    logger.info { "Launching replication pod: ${kubeInput.podName} (selectors = ${kubeInput.nodeSelectors}) with containers:" }
    logger.info { "[source] image: ${kubeInput.sourceImage} resources: ${kubeInput.sourceReqs}" }
    logger.info { "[destination] image: ${kubeInput.destinationImage} resources: ${kubeInput.destinationReqs}" }
    logger.info { "[orchestrator] image: ${kubeInput.orchestratorImage} resources: ${kubeInput.orchestratorReqs}" }

    try {
      pod = kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        message = "Failed to create pod ${kubeInput.podName}.",
        cause = e,
        commandType = KubeCommandType.CREATE,
        podType = PodType.REPLICATION,
      )
    }

    // NOTE: might not be necessary depending on when `serversideApply` returns.
    // If it blocks until it moves from PENDING, then we are good. Otherwise, we
    // need this or something similar to wait for the pod to be running on the node.
    waitForPodInitComplete(pod, PodType.REPLICATION.toString())
  }

  @Trace(operationName = LAUNCH_RESET_OPERATION_NAME)
  fun launchReset(
    payload: SyncPayload,
    launcherInput: LauncherInput,
  ) {
    val replicationInput = payload.input
    val sharedLabels =
      labeler.getSharedLabels(
        workloadId = launcherInput.workloadId,
        mutexKey = launcherInput.mutexKey,
        passThroughLabels = launcherInput.labels,
        autoId = launcherInput.autoId,
        workspaceId = replicationInput.workspaceId,
        networkSecurityTokens = replicationInput.networkSecurityTokens,
      )
    val kubeInput = mapper.toKubeInput(launcherInput.workloadId, payload, sharedLabels)

    var pod =
      replicationPodFactory.createReset(
        podName = kubeInput.podName,
        allLabels = kubeInput.labels,
        annotations = kubeInput.annotations,
        nodeSelectors = kubeInput.nodeSelectors,
        orchImage = kubeInput.orchestratorImage,
        destImage = kubeInput.destinationImage,
        orchResourceReqs = kubeInput.orchestratorReqs,
        destResourceReqs = kubeInput.destinationReqs,
        orchRuntimeEnvVars = kubeInput.orchestratorRuntimeEnvVars,
        destRuntimeEnvVars = kubeInput.destinationRuntimeEnvVars,
        isFileTransfer = replicationInput.useFileTransfer,
        workspaceId = replicationInput.workspaceId,
        architectureEnvironmentVariables = payload.architectureEnvironmentVariables ?: ArchitectureDecider.buildLegacyEnvironment(),
      )

    logger.info { "Launching reset pod: ${kubeInput.podName} (selectors = ${kubeInput.nodeSelectors}) with containers:" }
    logger.info { "[destination] image: ${kubeInput.destinationImage} resources: ${kubeInput.destinationReqs}" }
    logger.info { "[orchestrator] image: ${kubeInput.orchestratorImage} resources: ${kubeInput.orchestratorReqs}" }

    try {
      pod = kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        message = "Failed to create pod ${kubeInput.podName}.",
        cause = e,
        commandType = KubeCommandType.CREATE,
        podType = PodType.RESET,
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

  @InternalForTesting
  internal fun launchConnectorWithSidecar(
    kubeInput: ConnectorKubeInput,
    factory: ConnectorPodFactory,
    podLogLabel: String,
  ) {
    var pod =
      factory.create(
        allLabels = kubeInput.connectorLabels,
        nodeSelectors = kubeInput.nodeSelectors,
        kubePodInfo = kubeInput.kubePodInfo,
        annotations = kubeInput.annotations,
        connectorContainerReqs = kubeInput.connectorReqs,
        initContainerReqs = kubeInput.initReqs,
        runtimeEnvVars = kubeInput.runtimeEnvVars,
        workspaceId = kubeInput.workspaceId,
      )
    try {
      pod = kubePodLauncher.create(pod)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        message = "Failed to create pod ${kubeInput.kubePodInfo.name}.",
        cause = e,
        commandType = KubeCommandType.CREATE,
      )
    }

    waitForPodInitComplete(pod, podLogLabel)

    try {
      kubePodLauncher.waitForPodReadyOrTerminalByPod(pod, REPL_CONNECTOR_STARTUP_TIMEOUT_VALUE)
    } catch (e: RuntimeException) {
      ApmTraceUtils.addExceptionToTrace(e)
      throw KubeClientException(
        message = "$podLogLabel pod failed to start within allotted timeout.",
        cause = e,
        commandType = KubeCommandType.WAIT_MAIN,
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
