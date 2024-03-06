package io.airbyte.workload.launcher.pods.factories

import io.airbyte.workers.process.KubePodProcess
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeBuilder
import io.fabric8.kubernetes.api.model.VolumeMount
import io.fabric8.kubernetes.api.model.VolumeMountBuilder
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Singleton

@Singleton
class VolumeFactory(
  @Value("\${google.application.credentials}") private val googleApplicationCredentials: String?,
  @Value("\${airbyte.container.orchestrator.secret-name}") private val secretName: String?,
  @Value("\${airbyte.container.orchestrator.secret-mount-path}") private val secretMountPath: String?,
  @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-name}") private val dataPlaneCredsSecretName: String?,
  @Value("\${airbyte.container.orchestrator.data-plane-creds.secret-mount-path}") private val dataPlaneCredsSecretMountPath: String?,
) {
  fun config(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName("airbyte-config")
        .withNewEmptyDir()
        .withMedium("Memory")
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName("airbyte-config")
        .withMountPath(KubePodProcess.CONFIG_DIR)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun secret(): VolumeMountPair? {
    val hasSecrets =
      StringUtils.isNotEmpty(secretName) &&
        StringUtils.isNotEmpty(secretMountPath) &&
        StringUtils.isNotEmpty(googleApplicationCredentials)
    if (!hasSecrets) {
      return null
    }

    val volume =
      VolumeBuilder()
        .withName("airbyte-secret")
        .withSecret(
          SecretVolumeSourceBuilder()
            .withSecretName(secretName)
            .withDefaultMode(420)
            .build(),
        )
        .build()

    val mount =
      VolumeMountBuilder()
        .withName("airbyte-secret")
        .withMountPath(secretMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun dataplaneCreds(): VolumeMountPair? {
    val hasDataplaneCreds =
      StringUtils.isNotEmpty(dataPlaneCredsSecretName) &&
        StringUtils.isNotEmpty(dataPlaneCredsSecretMountPath)
    if (!hasDataplaneCreds) {
      return null
    }

    val volume =
      VolumeBuilder()
        .withName("airbyte-dataplane-creds")
        .withSecret(
          SecretVolumeSourceBuilder()
            .withSecretName(dataPlaneCredsSecretName)
            .withDefaultMode(420)
            .build(),
        )
        .build()

    val mount =
      VolumeMountBuilder()
        .withName("airbyte-dataplane-creds")
        .withMountPath(dataPlaneCredsSecretMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }
}

data class VolumeMountPair(
  val volume: Volume,
  val mount: VolumeMount,
)
