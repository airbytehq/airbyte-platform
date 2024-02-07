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
    @Value("\${airbyte.container.orchestrator.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    return "airbyte/container-orchestrator:$airbyteVersion"
  }

  @Singleton
  @Named("containerOrchestratorSidecarImage")
  fun containerOrchestratorSidecarImage(
    @Value("\${airbyte.version}") airbyteVersion: String,
    @Value("\${airbyte.container.orchestrator.sidecar.image}") injectedImage: String?,
  ): String {
    if (injectedImage != null && StringUtils.isNotEmpty(injectedImage)) {
      return injectedImage
    }

    if (airbyteVersion.endsWith("-cloud")) {
      return "airbyte/connector-sidecar:${airbyteVersion.dropLast(6)}"
    } else {
      return "airbyte/connector-sidecar:$airbyteVersion"
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
  fun orchestratorContainerPorts(
    @Value("\${micronaut.server.port}") serverPort: Int,
  ): List<ContainerPort> {
    return listOf(
      ContainerPortBuilder().withContainerPort(serverPort).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT1).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT2).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT3).build(),
      ContainerPortBuilder().withContainerPort(OrchestratorConstants.PORT4).build(),
    )
  }

  @Singleton
  @Named("sidecarKubeContainerInfo")
  fun sidecarKubeContainerInfo(
    @Named("containerOrchestratorSidecarImage") containerOrchestratorImage: String,
    @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") containerOrchestratorImagePullPolicy: String,
  ): KubeContainerInfo {
    return KubeContainerInfo(containerOrchestratorImage, containerOrchestratorImagePullPolicy)
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
  @Named("checkConnectorReqs")
  fun check(checkPodConfig: CheckPodConfig): ResourceRequirements {
    return ResourceRequirements()
      .withMemoryRequest(checkPodConfig.memoryRequest)
      .withMemoryLimit(checkPodConfig.memoryLimit)
      .withCpuRequest(checkPodConfig.cpuRequest)
      .withCpuLimit(checkPodConfig.cpuLimit)
  }

  @Singleton
  @Named("checkSidecarReqs")
  fun sidecar(checkPodConfig: CheckPodConfig): ResourceRequirements {
    return ResourceRequirements()
      .withMemoryRequest(checkPodConfig.memoryRequest)
      .withMemoryLimit(checkPodConfig.memoryLimit)
      .withCpuRequest(checkPodConfig.cpuRequest)
      .withCpuLimit(checkPodConfig.cpuLimit)
  }

  @Singleton
  @Named("checkPodTolerations")
  fun checkPodTolerations(
    @Named("checkWorkerConfigs") checkWorkerConfigs: WorkerConfigs,
  ): List<Toleration> {
    if (checkWorkerConfigs.workerKubeTolerations.isNullOrEmpty()) {
      return listOf()
    }
    return checkWorkerConfigs.workerKubeTolerations
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
  @Named("checkImagePullSecrets")
  fun checkImagePullSecrets(
    @Named("checkWorkerConfigs") checkWorkerConfigs: WorkerConfigs,
  ): List<LocalObjectReference> {
    return checkWorkerConfigs.jobImagePullSecrets
      .map { imagePullSecret -> LocalObjectReference(imagePullSecret) }
  }
}
