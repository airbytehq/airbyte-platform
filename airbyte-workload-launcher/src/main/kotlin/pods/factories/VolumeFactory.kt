package io.airbyte.workload.launcher.pods.factories

import io.airbyte.workers.pod.FileConstants
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
        .withMountPath(FileConstants.CONFIG_DIR)
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

  fun source(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName("airbyte-source")
        .withNewEmptyDir()
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName("airbyte-source")
        .withMountPath(FileConstants.SOURCE_DIR)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun destination(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName("airbyte-destination")
        .withNewEmptyDir()
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName("airbyte-destination")
        .withMountPath(FileConstants.DEST_DIR)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun replication(): ReplicationVolumes {
    val volumes: MutableList<Volume> = ArrayList()
    val orchVolumeMounts: MutableList<VolumeMount> = ArrayList()
    val sourceVolumeMounts: MutableList<VolumeMount> = ArrayList()
    val destVolumeMounts: MutableList<VolumeMount> = ArrayList()

    val config = config()
    volumes.add(config.volume)
    orchVolumeMounts.add(config.mount)

    val source = source()
    volumes.add(source.volume)
    orchVolumeMounts.add(source.mount)
    sourceVolumeMounts.add(source.mount)

    val dest = destination()
    volumes.add(dest.volume)
    orchVolumeMounts.add(dest.mount)
    destVolumeMounts.add(dest.mount)

    val secrets = secret()
    if (secrets != null) {
      volumes.add(secrets.volume)
      orchVolumeMounts.add(secrets.mount)
    }

    val dataPlaneCreds = dataplaneCreds()
    if (dataPlaneCreds != null) {
      volumes.add(dataPlaneCreds.volume)
      orchVolumeMounts.add(dataPlaneCreds.mount)
    }

    return ReplicationVolumes(
      volumes,
      orchVolumeMounts,
      sourceVolumeMounts,
      destVolumeMounts,
    )
  }
}

data class VolumeMountPair(
  val volume: Volume,
  val mount: VolumeMount,
)

data class ReplicationVolumes(
  val allVolumes: List<Volume>,
  val orchVolumeMounts: List<VolumeMount>,
  val sourceVolumeMounts: List<VolumeMount>,
  val destVolumeMounts: List<VolumeMount>,
)
