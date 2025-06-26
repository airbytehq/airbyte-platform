/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.workers.models.ArchitectureConstants.BOOKKEEPER
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_MEDIUM
import io.airbyte.workers.models.ArchitectureConstants.PLATFORM_MODE
import io.airbyte.workers.models.ArchitectureConstants.SOCKET_PATH
import io.fabric8.kubernetes.api.model.EnvVar

data class ArchitectureEnvironmentVariables(
  val sourceEnvironmentVariables: List<EnvVar>,
  val platformEnvironmentVariables: List<EnvVar>,
  val destinationEnvironmentVariables: List<EnvVar>,
) {
  fun isSocketBased(): Boolean = sourceEnvironmentVariables.any { it.name == DATA_CHANNEL_MEDIUM && it.value == Transport.SOCKET.name }

  fun getSocketBasePath(): String = SOCKET_PATH

  fun isPlatformBookkeeperMode(): Boolean = platformEnvironmentVariables.any { it.name == PLATFORM_MODE && it.value == BOOKKEEPER }

  /** Default tiny footprint for book‑keeping side‑cars (only needed in socket mode). */
  fun bookkeeperResourceRequirements(): io.airbyte.config.ResourceRequirements =
    io.airbyte.config
      .ResourceRequirements()
      .withCpuLimit("1")
      .withCpuRequest("1")
      .withMemoryLimit("1024Mi")
      .withMemoryRequest("1024Mi")
}

enum class Serialization {
  PROTOBUF,
  JSONL,
  ;

  @JsonCreator
  fun fromValue(v: String): Serialization = valueOf(v.trim().uppercase())
}

enum class Transport {
  SOCKET,
  STDIO,
  ;

  @JsonCreator
  fun fromValue(v: String): Transport = valueOf(v.trim().uppercase())
}

data class DataChannel
  @JsonCreator
  constructor(
    @JsonProperty("version") val version: String,
    @JsonProperty("supportedSerialization") val supportedSerialization: List<Serialization>,
    @JsonProperty("supportedTransport") val supportedTransport: List<Transport>,
  )

data class IPCOptions
  @JsonCreator
  constructor(
    @JsonProperty("dataChannel") val dataChannel: DataChannel,
  )
