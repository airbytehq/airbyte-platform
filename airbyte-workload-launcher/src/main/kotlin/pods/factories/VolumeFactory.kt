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
  @Value("\${airbyte.worker.job.kube.volumes.secret.secret-name}") private val secretName: String?,
  @Value("\${airbyte.worker.job.kube.volumes.secret.mount-path}") private val secretMountPath: String?,
  @Value("\${airbyte.worker.job.kube.volumes.data-plane-creds.secret-name}") private val dataPlaneCredsSecretName: String?,
  @Value("\${airbyte.worker.job.kube.volumes.data-plane-creds.mount-path}") private val dataPlaneCredsMountPath: String?,
  @Value("\${airbyte.worker.job.kube.volumes.staging.mount-path}") private val stagingMountPath: String,
) {
  fun config(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(CONFIG_VOLUME_NAME)
        .withNewEmptyDir()
        .withMedium("Memory")
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(CONFIG_VOLUME_NAME)
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
        .withName(SECRET_VOLUME_NAME)
        .withSecret(
          SecretVolumeSourceBuilder()
            .withSecretName(secretName)
            .withDefaultMode(420)
            .build(),
        )
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(SECRET_VOLUME_NAME)
        .withMountPath(secretMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun dataplaneCreds(): VolumeMountPair? {
    val hasDataplaneCreds =
      StringUtils.isNotEmpty(dataPlaneCredsSecretName) &&
        StringUtils.isNotEmpty(dataPlaneCredsMountPath)
    if (!hasDataplaneCreds) {
      return null
    }

    val volume =
      VolumeBuilder()
        .withName(DATA_PLANE_CREDS_VOLUME_NAME)
        .withSecret(
          SecretVolumeSourceBuilder()
            .withSecretName(dataPlaneCredsSecretName)
            .withDefaultMode(420)
            .build(),
        )
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(DATA_PLANE_CREDS_VOLUME_NAME)
        .withMountPath(dataPlaneCredsMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun source(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(SOURCE_VOLUME_NAME)
        .withNewEmptyDir()
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(SOURCE_VOLUME_NAME)
        .withMountPath(FileConstants.SOURCE_DIR)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun destination(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(DESTINATION_VOLUME_NAME)
        .withNewEmptyDir()
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(DESTINATION_VOLUME_NAME)
        .withMountPath(FileConstants.DEST_DIR)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun staging(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(STAGING_VOLUME_NAME)
        .withNewEmptyDir()
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(STAGING_VOLUME_NAME)
        .withMountPath(stagingMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun replication(useStaging: Boolean): ReplicationVolumes {
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

    if (useStaging) {
      val staging = staging()
      volumes.add(staging.volume)
      orchVolumeMounts.add(staging.mount)
      sourceVolumeMounts.add(staging.mount)
      destVolumeMounts.add(staging.mount)
    }

    return ReplicationVolumes(
      volumes,
      orchVolumeMounts,
      sourceVolumeMounts,
      destVolumeMounts,
    )
  }

  companion object {
    const val CONFIG_VOLUME_NAME = "airbyte-config"
    const val DATA_PLANE_CREDS_VOLUME_NAME = "airbyte-data-plane-creds"
    const val DESTINATION_VOLUME_NAME = "airbyte-destination"
    const val SECRET_VOLUME_NAME = "airbyte-secret"
    const val SOURCE_VOLUME_NAME = "airbyte-source"
    const val STAGING_VOLUME_NAME = "airbyte-file-staging"
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
