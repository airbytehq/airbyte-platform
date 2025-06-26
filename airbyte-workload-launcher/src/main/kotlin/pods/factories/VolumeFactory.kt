/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods.factories

import io.airbyte.commons.storage.STORAGE_CLAIM_NAME
import io.airbyte.commons.storage.STORAGE_MOUNT
import io.airbyte.commons.storage.STORAGE_VOLUME_NAME
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workload.launcher.ArchitectureDecider
import io.airbyte.workload.launcher.pipeline.stages.model.ArchitectureEnvironmentVariables
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder
import io.fabric8.kubernetes.api.model.Volume
import io.fabric8.kubernetes.api.model.VolumeBuilder
import io.fabric8.kubernetes.api.model.VolumeMount
import io.fabric8.kubernetes.api.model.VolumeMountBuilder
import io.micronaut.context.annotation.Value
import io.micronaut.core.util.StringUtils
import jakarta.inject.Singleton

@Singleton
data class VolumeFactory(
  @Value("\${google.application.credentials}") private val googleApplicationCredentials: String?,
  @Value("\${airbyte.worker.job.kube.volumes.secret.secret-name}") private val secretName: String?,
  @Value("\${airbyte.worker.job.kube.volumes.secret.mount-path}") private val secretMountPath: String?,
  @Value("\${airbyte.worker.job.kube.volumes.data-plane-creds.secret-name}") private val dataPlaneCredsSecretName: String?,
  @Value("\${airbyte.worker.job.kube.volumes.data-plane-creds.mount-path}") private val dataPlaneCredsMountPath: String?,
  @Value("\${airbyte.worker.job.kube.volumes.staging.mount-path}") private val stagingMountPath: String,
  @Value("\${airbyte.cloud.storage.type}") private val cloudStorageType: String,
  @Value("\${airbyte.worker.job.kube.volumes.local.enabled}") private val localVolumeEnabled: Boolean,
) {
  private fun config(): VolumeMountPair {
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

  private fun sharedTmp(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(SHARED_TMP)
        .withNewEmptyDir()
        .endEmptyDir()
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(SHARED_TMP)
        .withMountPath(FileConstants.TMP)
        .build()

    return VolumeMountPair(volume, mount)
  }

  private fun socket(socketPath: String): VolumeMountPair {
    val unixSocketVolume =
      VolumeBuilder()
        .withName(SOCKET_VOLUME)
        .withNewEmptyDir()
        .withMedium(MEMORY_MEDIUM)
        .endEmptyDir()
        .build()

    val unixSocketVolumeMount =
      VolumeMountBuilder()
        .withName(SOCKET_VOLUME)
        .withMountPath(socketPath) // common mount point for the Unix sockets
        .build()
    return VolumeMountPair(unixSocketVolume, unixSocketVolumeMount)
  }

  private fun secret(): VolumeMountPair? {
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
        ).build()

    val mount =
      VolumeMountBuilder()
        .withName(SECRET_VOLUME_NAME)
        .withMountPath(secretMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }

  private fun dataplaneCreds(): VolumeMountPair? {
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
        ).build()

    val mount =
      VolumeMountBuilder()
        .withName(DATA_PLANE_CREDS_VOLUME_NAME)
        .withMountPath(dataPlaneCredsMountPath)
        .build()

    return VolumeMountPair(volume, mount)
  }

  private fun source(): VolumeMountPair {
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

  private fun destination(): VolumeMountPair {
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

  private fun staging(): VolumeMountPair {
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

  // returns a volume+mount which allows the platform to use the local filesystem for storage (logs, state, etc).
  private fun storage(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(STORAGE_VOLUME_NAME)
        .withPersistentVolumeClaim(PersistentVolumeClaimVolumeSource(STORAGE_CLAIM_NAME, false))
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(STORAGE_VOLUME_NAME)
        .withMountPath(STORAGE_MOUNT)
        .build()

    return VolumeMountPair(volume, mount)
  }

  // returns a volume+mount which allows connectors to access the volume mounted at /local.
  private fun connectorHostFileAccess(): VolumeMountPair {
    val volume =
      VolumeBuilder()
        .withName(LOCAL_VOLUME_NAME)
        .withPersistentVolumeClaim(PersistentVolumeClaimVolumeSource(LOCAL_CLAIM_NAME, false))
        .build()

    val mount =
      VolumeMountBuilder()
        .withName(LOCAL_VOLUME_NAME)
        .withMountPath(LOCAL_VOLUME_MOUNT)
        .build()

    return VolumeMountPair(volume, mount)
  }

  fun connector(): ConnectorVolumes {
    val volumes = mutableListOf<Volume>()
    val initMounts = mutableListOf<VolumeMount>()
    val sidecarMounts = mutableListOf<VolumeMount>()
    val mainMounts = mutableListOf<VolumeMount>()

    val config = config()
    volumes.add(config.volume)
    initMounts.add(config.mount)
    mainMounts.add(config.mount)
    sidecarMounts.add(config.mount)

    val secrets = secret()
    if (secrets != null) {
      volumes.add(secrets.volume)
      initMounts.add(secrets.mount)
      sidecarMounts.add(secrets.mount)
    }

    val dataPlaneCreds = dataplaneCreds()
    if (dataPlaneCreds != null) {
      volumes.add(dataPlaneCreds.volume)
      initMounts.add(dataPlaneCreds.mount)
      sidecarMounts.add(dataPlaneCreds.mount)
    }

    if (cloudStorageType.lowercase() == "local") {
      storage().also {
        volumes.add(it.volume)
        initMounts.add(it.mount)
        sidecarMounts.add(it.mount)
      }
    }

    if (localVolumeEnabled) {
      connectorHostFileAccess().also {
        volumes.add(it.volume)
        mainMounts.add(it.mount)
      }
    }

    return ConnectorVolumes(
      volumes,
      initMounts = initMounts,
      mainMounts = mainMounts,
      sidecarMounts = sidecarMounts,
    )
  }

  fun replication(
    useStaging: Boolean,
    enableAsyncProfiler: Boolean = false,
    architecture: ArchitectureEnvironmentVariables = ArchitectureDecider.buildLegacyEnvironment(),
  ): ReplicationVolumes {
    val volumes = mutableListOf<Volume>()
    val orchVolumeMounts = mutableListOf<VolumeMount>()
    val sourceVolumeMounts = mutableListOf<VolumeMount>()
    val destVolumeMounts = mutableListOf<VolumeMount>()
    val profilerVolumeMounts = mutableListOf<VolumeMount>()

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
      if (enableAsyncProfiler) {
        profilerVolumeMounts.add(secrets.mount)
      }
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

    if (cloudStorageType.lowercase() == "local") {
      storage().also {
        volumes.add(it.volume)
        orchVolumeMounts.add(it.mount)
      }
    }

    if (localVolumeEnabled) {
      connectorHostFileAccess().also {
        volumes.add(it.volume)
        sourceVolumeMounts.add(it.mount)
        destVolumeMounts.add(it.mount)
      }
    }

    if (enableAsyncProfiler) {
      sharedTmp().also {
        volumes.add(it.volume)
        orchVolumeMounts.add(it.mount)
        sourceVolumeMounts.add(it.mount)
        destVolumeMounts.add(it.mount)
        profilerVolumeMounts.add(it.mount)
      }
    }

    if (architecture.isSocketBased()) {
      socket(architecture.getSocketBasePath()).also {
        volumes.add(it.volume)
        orchVolumeMounts.add(it.mount)
        sourceVolumeMounts.add(it.mount)
        destVolumeMounts.add(it.mount)
        profilerVolumeMounts.add(it.mount)
      }
    }

    return ReplicationVolumes(
      volumes,
      orchVolumeMounts,
      sourceVolumeMounts,
      destVolumeMounts,
      profilerVolumeMounts,
    )
  }

  companion object {
    const val CONFIG_VOLUME_NAME = "airbyte-config"
    const val DATA_PLANE_CREDS_VOLUME_NAME = "airbyte-data-plane-creds"
    const val DESTINATION_VOLUME_NAME = "airbyte-destination"
    const val SECRET_VOLUME_NAME = "airbyte-secret"
    const val SOURCE_VOLUME_NAME = "airbyte-source"
    const val STAGING_VOLUME_NAME = "airbyte-file-staging"
    const val SHARED_TMP = "shared-tmp"
    const val SOCKET_VOLUME = "unix-socket-volume"
    const val MEMORY_MEDIUM = "Memory"

    // "local" means that the connector has local file access to this volume.
    const val LOCAL_VOLUME_MOUNT = "/local"
    const val LOCAL_VOLUME_NAME = "airbyte-local"
    const val LOCAL_CLAIM_NAME = "airbyte-local-pvc"
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
  val profilerVolumeMounts: List<VolumeMount>,
)

data class ConnectorVolumes(
  val volumes: List<Volume>,
  val mainMounts: List<VolumeMount>,
  val sidecarMounts: List<VolumeMount>,
  val initMounts: List<VolumeMount>,
)
