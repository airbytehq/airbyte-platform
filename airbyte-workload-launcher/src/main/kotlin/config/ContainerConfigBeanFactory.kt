/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.airbyte.config.ResourceRequirements
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workers.sync.OrchestratorConstants
import io.fabric8.kubernetes.api.model.ContainerPort
import io.fabric8.kubernetes.api.model.ContainerPortBuilder
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Toleration
import io.fabric8.kubernetes.api.model.TolerationBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for container configuration-related singletons.
 */
@Factory
class ContainerConfigBeanFactory {
  @Singleton
  fun kubeClient(customOkHttpClientFactory: CustomOkHttpClientFactory): KubernetesClient {
    return KubernetesClientBuilder()
      .withHttpClientFactory(customOkHttpClientFactory)
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
  @Named("orchestratorContainerPorts")
  fun orchestratorContainerPorts(): List<ContainerPort> {
    return listOf(
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.SERVER_PORT).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT1).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT2).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT3).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT4).build(),
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
    @Value("\${airbyte.worker.kube-job-configs.check.cpu-limit:2}") cpuLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.check.cpu-request:2}") cpuRequest: String,
    @Value("\${airbyte.worker.kube-job-configs.check.memory-limit:500Mi}") memoryLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.check.memory-request:500Mi}") memoryRequest: String,
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
    @Value("\${airbyte.worker.kube-job-configs.discover.cpu-limit:2}") cpuLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.discover.cpu-request:2}") cpuRequest: String,
    @Value("\${airbyte.worker.kube-job-configs.discover.memory-limit:500Mi}") memoryLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.discover.memory-request:500Mi}") memoryRequest: String,
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
    @Value("\${airbyte.worker.kube-job-configs.spec.cpu-limit:1}") cpuLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.spec.cpu-request:1}") cpuRequest: String,
    @Value("\${airbyte.worker.kube-job-configs.spec.memory-limit:200Mi}") memoryLimit: String,
    @Value("\${airbyte.worker.kube-job-configs.spec.memory-request:200Mi}") memoryRequest: String,
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
    @Value("\${airbyte.worker.connector-sidecar.resources.cpu-limit:2}") cpuLimit: String,
    @Value("\${airbyte.worker.connector-sidecar.resources.cpu-request:2}") cpuRequest: String,
    @Value("\${airbyte.worker.connector-sidecar.resources.memory-limit:500Mi}") memoryLimit: String,
    @Value("\${airbyte.worker.connector-sidecar.resources.memory-request:500Mi}") memoryRequest: String,
  ): ResourceRequirements {
    return ResourceRequirements()
      .withCpuLimit(cpuLimit)
      .withCpuRequest(cpuRequest)
      .withMemoryLimit(memoryLimit)
      .withMemoryRequest(memoryRequest)
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
