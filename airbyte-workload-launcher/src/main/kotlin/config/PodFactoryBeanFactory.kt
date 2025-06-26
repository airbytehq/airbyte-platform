/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workload.launcher.context.WorkloadSecurityContextProvider
import io.airbyte.workload.launcher.pods.KubeContainerInfo
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory.Companion.CHECK_OPERATION_NAME
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory.Companion.DISCOVER_OPERATION_NAME
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory.Companion.SPEC_OPERATION_NAME
import io.airbyte.workload.launcher.pods.factories.InitContainerFactory
import io.airbyte.workload.launcher.pods.factories.ResourceRequirementsFactory
import io.airbyte.workload.launcher.pods.factories.VolumeFactory
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.LocalObjectReference
import io.fabric8.kubernetes.api.model.Toleration
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class PodFactoryBeanFactory {
  @Singleton
  @Named("checkPodFactory")
  fun checkPodFactory(
    featureFlagClient: FeatureFlagClient,
    @Named("checkPodTolerations") tolerations: List<Toleration>,
    @Named("checkImagePullSecrets") imagePullSecrets: List<LocalObjectReference>,
    @Named("checkEnvVars") connectorEnvVars: List<EnvVar>,
    @Named("sideCarEnvVars") sideCarEnvVars: List<EnvVar>,
    @Named("sidecarKubeContainerInfo") sidecarContainerInfo: KubeContainerInfo,
    @Value("\${airbyte.worker.job.kube.serviceAccount}") serviceAccount: String?,
    volumeFactory: VolumeFactory,
    initContainerFactory: InitContainerFactory,
    workloadSecurityContextProvider: WorkloadSecurityContextProvider,
    resourceRequirementsFactory: ResourceRequirementsFactory,
  ): ConnectorPodFactory =
    ConnectorPodFactory(
      CHECK_OPERATION_NAME,
      featureFlagClient,
      tolerations,
      imagePullSecrets,
      connectorEnvVars,
      sideCarEnvVars,
      sidecarContainerInfo,
      serviceAccount,
      volumeFactory,
      initContainerFactory,
      mapOf(
        "config" to "${FileConstants.CONFIG_DIR}/${FileConstants.CONNECTION_CONFIGURATION_FILE}",
      ),
      workloadSecurityContextProvider,
      resourceRequirementsFactory,
    )

  @Singleton
  @Named("discoverPodFactory")
  fun discoverPodFactory(
    featureFlagClient: FeatureFlagClient,
    @Named("discoverPodTolerations") tolerations: List<Toleration>,
    @Named("discoverImagePullSecrets") imagePullSecrets: List<LocalObjectReference>,
    @Named("discoverEnvVars") connectorEnvVars: List<EnvVar>,
    @Named("sideCarEnvVars") sideCarEnvVars: List<EnvVar>,
    @Named("sidecarKubeContainerInfo") sidecarContainerInfo: KubeContainerInfo,
    @Value("\${airbyte.worker.job.kube.serviceAccount}") serviceAccount: String?,
    volumeFactory: VolumeFactory,
    initContainerFactory: InitContainerFactory,
    workloadSecurityContextProvider: WorkloadSecurityContextProvider,
    resourceRequirementsFactory: ResourceRequirementsFactory,
  ): ConnectorPodFactory =
    ConnectorPodFactory(
      DISCOVER_OPERATION_NAME,
      featureFlagClient,
      tolerations,
      imagePullSecrets,
      connectorEnvVars,
      sideCarEnvVars,
      sidecarContainerInfo,
      serviceAccount,
      volumeFactory,
      initContainerFactory,
      mapOf(
        "config" to "${FileConstants.CONFIG_DIR}/${FileConstants.CONNECTION_CONFIGURATION_FILE}",
      ),
      workloadSecurityContextProvider,
      resourceRequirementsFactory,
    )

  @Singleton
  @Named("specPodFactory")
  fun specPodFactory(
    featureFlagClient: FeatureFlagClient,
    @Named("specPodTolerations") tolerations: List<Toleration>,
    @Named("specImagePullSecrets") imagePullSecrets: List<LocalObjectReference>,
    @Named("specEnvVars") connectorEnvVars: List<EnvVar>,
    @Named("sideCarEnvVars") sideCarEnvVars: List<EnvVar>,
    @Named("sidecarKubeContainerInfo") sidecarContainerInfo: KubeContainerInfo,
    @Value("\${airbyte.worker.job.kube.serviceAccount}") serviceAccount: String?,
    volumeFactory: VolumeFactory,
    initContainerFactory: InitContainerFactory,
    workloadSecurityContextProvider: WorkloadSecurityContextProvider,
    resourceRequirementsFactory: ResourceRequirementsFactory,
  ): ConnectorPodFactory =
    ConnectorPodFactory(
      SPEC_OPERATION_NAME,
      featureFlagClient,
      tolerations,
      imagePullSecrets,
      connectorEnvVars,
      sideCarEnvVars,
      sidecarContainerInfo,
      serviceAccount,
      volumeFactory,
      initContainerFactory,
      mapOf(),
      workloadSecurityContextProvider,
      resourceRequirementsFactory,
    )
}
