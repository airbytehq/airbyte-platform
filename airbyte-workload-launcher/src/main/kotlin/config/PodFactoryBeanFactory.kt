package io.airbyte.workload.launcher.config

import io.airbyte.config.ResourceRequirements
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workers.process.KubeContainerInfo
import io.airbyte.workload.launcher.pods.factories.ConnectorPodFactory
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
    @Named("checkConnectorReqs") connectorReqs: ResourceRequirements,
    @Named("sidecarReqs") sidecarReqs: ResourceRequirements,
    @Named("checkPodTolerations") tolerations: List<Toleration>,
    @Named("checkImagePullSecrets") imagePullSecrets: List<LocalObjectReference>,
    @Named("checkEnvVars") connectorEnvVars: List<EnvVar>,
    @Named("sideCarEnvVars") sideCarEnvVars: List<EnvVar>,
    @Named("sidecarKubeContainerInfo") sidecarContainerInfo: KubeContainerInfo,
    @Value("\${airbyte.worker.job.kube.serviceAccount}") serviceAccount: String?,
    volumeFactory: VolumeFactory,
  ): ConnectorPodFactory {
    return ConnectorPodFactory(
      "check",
      featureFlagClient,
      connectorReqs,
      sidecarReqs,
      tolerations,
      imagePullSecrets,
      connectorEnvVars,
      sideCarEnvVars,
      sidecarContainerInfo,
      serviceAccount,
      volumeFactory,
    )
  }

  @Singleton
  @Named("discoverPodFactory")
  fun discoverPodFactory(
    featureFlagClient: FeatureFlagClient,
    @Named("discoverConnectorReqs") connectorReqs: ResourceRequirements,
    @Named("sidecarReqs") sidecarReqs: ResourceRequirements,
    @Named("discoverPodTolerations") tolerations: List<Toleration>,
    @Named("discoverImagePullSecrets") imagePullSecrets: List<LocalObjectReference>,
    @Named("discoverEnvVars") connectorEnvVars: List<EnvVar>,
    @Named("sideCarEnvVars") sideCarEnvVars: List<EnvVar>,
    @Named("sidecarKubeContainerInfo") sidecarContainerInfo: KubeContainerInfo,
    @Value("\${airbyte.worker.job.kube.serviceAccount}") serviceAccount: String?,
    volumeFactory: VolumeFactory,
  ): ConnectorPodFactory {
    return ConnectorPodFactory(
      "discover",
      featureFlagClient,
      connectorReqs,
      sidecarReqs,
      tolerations,
      imagePullSecrets,
      connectorEnvVars,
      sideCarEnvVars,
      sidecarContainerInfo,
      serviceAccount,
      volumeFactory,
    )
  }
}
