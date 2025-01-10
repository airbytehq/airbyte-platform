/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.airbyte.config.ResourceRequirements
import io.airbyte.workers.pod.KubeContainerInfo
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.TolerationBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.utils.KubernetesSerialization
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.ExecutorService

/**
 * Micronaut bean factory for container configuration-related singletons.
 */
@Factory
class ContainerConfigBeanFactory {
  @Singleton
  fun kubeSerialization(objectMapper: ObjectMapper): KubernetesSerialization {
    return KubernetesSerialization(objectMapper, true)
  }

  @Singleton
  fun kubeClient(
    customOkHttpClientFactory: CustomOkHttpClientFactory,
    kubernetesSerialization: KubernetesSerialization,
    // Configured in application.yml under micronaut.executors.kube-client
    @Named("kube-client") executorService: ExecutorService,
  ): KubernetesClient {
    return KubernetesClientBuilder()
      .withHttpClientFactory(customOkHttpClientFactory)
      .withKubernetesSerialization(kubernetesSerialization)
      .withTaskExecutor(executorService)
      .build()
  }

  @Singleton
  @Named("containerOrchestratorImage")
  fun containerOrchestratorImage(
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.worker.job.kube.main.container.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    return "airbyte/container-orchestrator:$airbyteVersion"
  }

  @Singleton
  @Named("connectorSidecarImage")
  fun connectorSidecarImage(
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.worker.job.kube.sidecar.container.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    return if (airbyteVersion.endsWith("-cloud")) {
      "airbyte/connector-sidecar:${airbyteVersion.dropLast(6)}"
    } else {
      "airbyte/connector-sidecar:$airbyteVersion"
    }
  }

  @Singleton
  @Named("initContainerImage")
  fun initContainerImage(
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.worker.job.kube.init.container.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    return if (airbyteVersion.endsWith("-cloud")) {
      "airbyte/workload-init-container:${airbyteVersion.dropLast(6)}"
    } else {
      "airbyte/workload-init-container:$airbyteVersion"
    }
  }

  @Singleton
  @Named("orchestratorKubeContainerInfo")
  fun orchestratorKubeContainerInfo(
    @Named("containerOrchestratorImage") containerOrchestratorImage: String,
    @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") containerOrchestratorImagePullPolicy: String,
  ): KubeContainerInfo {
    return KubeContainerInfo(
      containerOrchestratorImage,
      containerOrchestratorImagePullPolicy,
    )
  }

  @Singleton
  @Named("sidecarKubeContainerInfo")
  fun sidecarKubeContainerInfo(
    @Named("connectorSidecarImage") connectorSidecarImage: String,
    @Value("\${airbyte.worker.job.kube.sidecar.container.image-pull-policy}") connectorSidecarImagePullPolicy: String,
  ): KubeContainerInfo {
    return KubeContainerInfo(connectorSidecarImage, connectorSidecarImagePullPolicy)
  }

  @Singleton
  @Named("initContainerInfo")
  fun initContainerInfo(
    @Named("initContainerImage") initContainerImage: String,
    @Value("\${airbyte.worker.job.kube.sidecar.container.image-pull-policy}") initContainerImagePullPolicy: String,
  ): KubeContainerInfo {
    return KubeContainerInfo(initContainerImage, initContainerImagePullPolicy)
  }

  @Singleton
  @Named("replicationWorkerConfigs")
  fun replicationWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.REPLICATION)
  }

  @Singleton
  @Named("checkWorkerConfigs")
  fun checkWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.CHECK)
  }

  @Singleton
  @Named("discoverWorkerConfigs")
  fun discoverWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.DISCOVER)
  }

  @Singleton
  @Named("specWorkerConfigs")
  fun specWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.SPEC)
  }

  @Singleton
  @Named("checkConnectorReqs")
  fun checkConnectorReqs(
    @Value("\${airbyte.worker.kube-job-configs.check.cpu-limit}") cpuLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.check.cpu-request}") cpuRequest: String,
    @Value("\${airbyte.worker.kube-job-configs.check.memory-limit}") memoryLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.check.memory-request}") memoryRequest: String,
  ): ResourceRequirements {
    return ResourceRequirements()
      .withCpuLimit(cpuLimit)
      .withCpuRequest(cpuRequest)
      .withMemoryLimit(memoryLimit)
      .withMemoryRequest(memoryRequest)
  }

  @Singleton
  @Named("discoverConnectorReqs")
  fun discoverConnectorReqs(
    @Value("\${airbyte.worker.kube-job-configs.discover.cpu-limit}") cpuLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.discover.cpu-request}") cpuRequest: String,
    @Value("\${airbyte.worker.kube-job-configs.discover.memory-limit}") memoryLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.discover.memory-request}") memoryRequest: String,
  ): ResourceRequirements {
    return ResourceRequirements()
      .withCpuLimit(cpuLimit)
      .withCpuRequest(cpuRequest)
      .withMemoryLimit(memoryLimit)
      .withMemoryRequest(memoryRequest)
  }

  @Singleton
  @Named("specConnectorReqs")
  fun specConnectorReqs(
    @Value("\${airbyte.worker.kube-job-configs.spec.cpu-limit}") cpuLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.spec.cpu-request}") cpuRequest: String,
    @Value("\${airbyte.worker.kube-job-configs.spec.memory-limit}") memoryLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.spec.memory-request}") memoryRequest: String,
  ): ResourceRequirements {
    return ResourceRequirements()
      .withCpuLimit(cpuLimit)
      .withCpuRequest(cpuRequest)
      .withMemoryLimit(memoryLimit)
      .withMemoryRequest(memoryRequest)
  }

  @Singleton
  @Named("sidecarReqs")
  fun sidecarReqs(
    @Value("\${airbyte.worker.connector-sidecar.resources.cpu-limit}") cpuLimit: String,
    @Value("\${airbyte.worker.connector-sidecar.resources.cpu-request}") cpuRequest: String,
    @Value("\${airbyte.worker.connector-sidecar.resources.memory-limit}") memoryLimit: String,
    @Value("\${airbyte.worker.connector-sidecar.resources.memory-request}") memoryRequest: String,
  ): ResourceRequirements {
    return ResourceRequirements()
      .withCpuLimit(cpuLimit)
      .withCpuRequest(cpuRequest)
      .withMemoryLimit(memoryLimit)
      .withMemoryRequest(memoryRequest)
  }

  @Singleton
  @Named("fileTransferReqs")
  fun fileTransferReqs(
    @Value("\${airbyte.worker.file-transfer.resources.ephemeral-storage-limit}") ephemeralStorageLimit: String,
    @Value("\${airbyte.worker.file-transfer.resources.ephemeral-storage-request}") ephemeralStorageRequest: String,
  ): ResourceRequirements {
    return ResourceRequirements()
      .withEphemeralStorageLimit(ephemeralStorageLimit)
      .withEphemeralStorageRequest(ephemeralStorageRequest)
  }

  @Singleton
  @Named("replicationPodTolerations")
  fun replicationPodTolerations(
    @Named("replicationWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<Toleration> {
    if (workerConfigs.workerKubeTolerations.isNullOrEmpty()) {
      return listOf()
    }
    return workerConfigs.workerKubeTolerations
      .map { t ->
        TolerationBuilder()
          .withKey(t.key)
          .withEffect(t.effect)
          .withOperator(t.operator)
          .withValue(t.value)
          .build()
      }
  }

  @Singleton
  @Named("checkPodTolerations")
  fun checkPodTolerations(
    @Named("checkWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<Toleration> {
    if (workerConfigs.workerKubeTolerations.isNullOrEmpty()) {
      return listOf()
    }
    return workerConfigs.workerKubeTolerations
      .map { t ->
        TolerationBuilder()
          .withKey(t.key)
          .withEffect(t.effect)
          .withOperator(t.operator)
          .withValue(t.value)
          .build()
      }
  }

  @Singleton
  @Named("discoverPodTolerations")
  fun discoverPodTolerations(
    @Named("discoverWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<Toleration> {
    if (workerConfigs.workerKubeTolerations.isNullOrEmpty()) {
      return listOf()
    }
    return workerConfigs.workerKubeTolerations
      .map { t ->
        TolerationBuilder()
          .withKey(t.key)
          .withEffect(t.effect)
          .withOperator(t.operator)
          .withValue(t.value)
          .build()
      }
  }

  @Singleton
  @Named("specPodTolerations")
  fun specPodTolerations(
    @Named("specWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<Toleration> {
    if (workerConfigs.workerKubeTolerations.isNullOrEmpty()) {
      return listOf()
    }
    return workerConfigs.workerKubeTolerations
      .map { t ->
        TolerationBuilder()
          .withKey(t.key)
          .withEffect(t.effect)
          .withOperator(t.operator)
          .withValue(t.value)
          .build()
      }
  }

  @Singleton
  @Named("spotToleration")
  fun spotTolerations(): Toleration =
    Toleration().apply {
      key = "airbyte/spot"
      value = "true"
      operator = "Equal"
      effect = "NoSchedule"
    }

  @Singleton
  @Named("replicationImagePullSecrets")
  fun replicationImagePullSecrets(
    @Named("replicationWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> {
    return workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
  }

  @Singleton
  @Named("checkImagePullSecrets")
  fun checkImagePullSecrets(
    @Named("checkWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> {
    return workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
  }

  @Singleton
  @Named("discoverImagePullSecrets")
  fun discoverImagePullSecrets(
    @Named("discoverWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> {
    return workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
  }

  @Singleton
  @Named("specImagePullSecrets")
  fun specImagePullSecrets(
    @Named("specWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> {
    return workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
  }
}
