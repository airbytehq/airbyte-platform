/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.workers.config.ResourceType
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.airbyte.config.ResourceRequirements
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.workload.launcher.pods.KubeContainerInfo
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.TolerationBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.fabric8.kubernetes.client.utils.KubernetesSerialization
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.concurrent.ExecutorService

/**
 * Micronaut bean factory for container configuration-related singletons.
 */
@Factory
class ContainerConfigBeanFactory {
  @Singleton
  fun kubeSerialization(objectMapper: ObjectMapper): KubernetesSerialization = KubernetesSerialization(objectMapper, true)

  @Singleton
  fun kubeClient(
    customOkHttpClientFactory: CustomOkHttpClientFactory,
    kubernetesSerialization: KubernetesSerialization,
    // Configured in application.yml under micronaut.executors.kube-client
    @Named("kube-client") executorService: ExecutorService,
  ): KubernetesClient =
    KubernetesClientBuilder()
      .withHttpClientFactory(customOkHttpClientFactory)
      .withKubernetesSerialization(kubernetesSerialization)
      .withTaskExecutor(executorService)
      .build()

  @Singleton
  @Named("containerOrchestratorImage")
  fun containerOrchestratorImage(
    airbyteConfig: AirbyteConfig,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): String =
    airbyteWorkerConfig.job.kubernetes.main.container.image.ifBlank {
      "airbyte/container-orchestrator:${airbyteConfig.version}"
    }

  @Singleton
  @Named("connectorSidecarImage")
  fun connectorSidecarImage(
    airbyteConfig: AirbyteConfig,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): String =
    airbyteWorkerConfig.job.kubernetes.sidecar.container.image.ifBlank {
      if (airbyteConfig.version.endsWith("-cloud")) {
        "airbyte/connector-sidecar:${airbyteConfig.version.dropLast(6)}"
      } else {
        "airbyte/connector-sidecar:${airbyteConfig.version}"
      }
    }

  @Singleton
  @Named("initContainerImage")
  fun initContainerImage(
    airbyteConfig: AirbyteConfig,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): String =
    airbyteWorkerConfig.job.kubernetes.init.container.image.ifBlank {
      if (airbyteConfig.version.endsWith("-cloud")) {
        "airbyte/workload-init-container:${airbyteConfig.version.dropLast(6)}"
      } else {
        "airbyte/workload-init-container:${airbyteConfig.version}"
      }
    }

  @Singleton
  @Named("profilerContainerImage")
  fun profilerContainerImage(
    airbyteConfig: AirbyteConfig,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): String =
    airbyteWorkerConfig.job.kubernetes.profiler.container.image.ifBlank {
      if (airbyteConfig.version.endsWith("-cloud")) {
        "airbyte/async-profiler:${airbyteConfig.version.dropLast(6)}"
      } else {
        "airbyte/async-profiler:${airbyteConfig.version}"
      }
    }

  @Singleton
  @Named("orchestratorKubeContainerInfo")
  fun orchestratorKubeContainerInfo(
    @Named("containerOrchestratorImage") containerOrchestratorImage: String,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): KubeContainerInfo =
    KubeContainerInfo(
      containerOrchestratorImage,
      airbyteWorkerConfig.job.kubernetes.main.container.imagePullPolicy,
    )

  @Singleton
  @Named("sidecarKubeContainerInfo")
  fun sidecarKubeContainerInfo(
    @Named("connectorSidecarImage") connectorSidecarImage: String,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): KubeContainerInfo = KubeContainerInfo(connectorSidecarImage, airbyteWorkerConfig.job.kubernetes.sidecar.container.imagePullPolicy)

  @Singleton
  @Named("profilerKubeContainerInfo")
  fun profilerKubeContainerInfo(
    @Named("profilerContainerImage") profilerContainerImage: String,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): KubeContainerInfo = KubeContainerInfo(profilerContainerImage, airbyteWorkerConfig.job.kubernetes.profiler.container.imagePullPolicy)

  @Singleton
  @Named("initContainerInfo")
  fun initContainerInfo(
    @Named("initContainerImage") initContainerImage: String,
    airbyteWorkerConfig: AirbyteWorkerConfig,
  ): KubeContainerInfo = KubeContainerInfo(initContainerImage, airbyteWorkerConfig.job.kubernetes.init.container.imagePullPolicy)

  @Singleton
  @Named("replicationWorkerConfigs")
  fun replicationWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs =
    workerConfigsProvider.getConfig(ResourceType.REPLICATION)

  @Singleton
  @Named("checkWorkerConfigs")
  fun checkWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs = workerConfigsProvider.getConfig(ResourceType.CHECK)

  @Singleton
  @Named("discoverWorkerConfigs")
  fun discoverWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs = workerConfigsProvider.getConfig(ResourceType.DISCOVER)

  @Singleton
  @Named("specWorkerConfigs")
  fun specWorkerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs = workerConfigsProvider.getConfig(ResourceType.SPEC)

  @Singleton
  @Named("checkConnectorReqs")
  fun checkConnectorReqs(airbyteWorkerConfig: AirbyteWorkerConfig): ResourceRequirements =
    ResourceRequirements()
      .withCpuLimit(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "check" }?.cpuLimit)
      .withCpuRequest(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "check" }?.cpuRequest)
      .withMemoryLimit(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "check" }?.memoryLimit)
      .withMemoryRequest(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "check" }?.memoryRequest)

  @Singleton
  @Named("profilerReqs")
  fun profilerReqs(airbyteWorkerConfig: AirbyteWorkerConfig): ResourceRequirements =
    ResourceRequirements()
      .withCpuLimit(airbyteWorkerConfig.job.kubernetes.profiler.container.cpuLimit)
      .withCpuRequest(airbyteWorkerConfig.job.kubernetes.profiler.container.cpuRequest)
      .withMemoryLimit(airbyteWorkerConfig.job.kubernetes.profiler.container.memoryLimit)
      .withMemoryRequest(airbyteWorkerConfig.job.kubernetes.profiler.container.memoryRequest)

  @Singleton
  @Named("discoverConnectorReqs")
  fun discoverConnectorReqs(airbyteWorkerConfig: AirbyteWorkerConfig): ResourceRequirements =
    ResourceRequirements()
      .withCpuLimit(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "discover" }?.cpuLimit)
      .withCpuRequest(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "discover" }?.cpuRequest)
      .withMemoryLimit(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "discover" }?.memoryLimit)
      .withMemoryRequest(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "discover" }?.memoryRequest)

  @Singleton
  @Named("specConnectorReqs")
  fun specConnectorReqs(airbyteWorkerConfig: AirbyteWorkerConfig): ResourceRequirements =
    ResourceRequirements()
      .withCpuLimit(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "spec" }?.cpuLimit)
      .withCpuRequest(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "spec" }?.cpuRequest)
      .withMemoryLimit(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "spec" }?.memoryLimit)
      .withMemoryRequest(airbyteWorkerConfig.kubeJobConfigs.find { it.name == "spec" }?.memoryRequest)

  @Singleton
  @Named("sidecarReqs")
  fun sidecarReqs(airbyteWorkerConfig: AirbyteWorkerConfig): ResourceRequirements =
    ResourceRequirements()
      .withCpuLimit(airbyteWorkerConfig.connectorSidecar.resources.cpuLimit)
      .withCpuRequest(airbyteWorkerConfig.connectorSidecar.resources.cpuRequest)
      .withMemoryLimit(airbyteWorkerConfig.connectorSidecar.resources.memoryLimit)
      .withMemoryRequest(airbyteWorkerConfig.connectorSidecar.resources.memoryRequest)

  @Singleton
  @Named("fileTransferReqs")
  fun fileTransferReqs(airbyteWorkerConfig: AirbyteWorkerConfig): ResourceRequirements =
    ResourceRequirements()
      .withEphemeralStorageLimit(airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageLimit)
      .withEphemeralStorageRequest(airbyteWorkerConfig.fileTransfer.resources.ephemeralStorageRequest)

  @Singleton
  @Named("replicationPodTolerations")
  fun replicationPodTolerations(
    @Named("replicationWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<Toleration> {
    if (workerConfigs.workerKubeTolerations.isEmpty()) {
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
    if (workerConfigs.workerKubeTolerations.isEmpty()) {
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
  ): List<LocalObjectReference> =
    workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }

  @Singleton
  @Named("checkImagePullSecrets")
  fun checkImagePullSecrets(
    @Named("checkWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> =
    workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }

  @Singleton
  @Named("discoverImagePullSecrets")
  fun discoverImagePullSecrets(
    @Named("discoverWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> =
    workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }

  @Singleton
  @Named("specImagePullSecrets")
  fun specImagePullSecrets(
    @Named("specWorkerConfigs") workerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> =
    workerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
}
