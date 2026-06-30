/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.ForceRunStdioMode
import io.airbyte.featureflag.SocketCount
import io.airbyte.featureflag.SocketFormat
import io.airbyte.featureflag.SocketTest
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.Jsons
import io.airbyte.workers.input.toFeatureFlagContext
import io.airbyte.workers.models.ArchitectureConstants.BOOKKEEPER
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_FORMAT
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_MEDIUM
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_SOCKET_PATHS
import io.airbyte.workers.models.ArchitectureConstants.ORCHESTRATOR
import io.airbyte.workers.models.ArchitectureConstants.PLATFORM_MODE
import io.airbyte.workers.models.ArchitectureConstants.SOCKET_FILE_POSTFIX
import io.airbyte.workers.models.ArchitectureConstants.SOCKET_FILE_PREFIX
import io.airbyte.workers.models.ArchitectureConstants.SOCKET_PATH
import io.airbyte.workload.launcher.constants.GCS_DATALAKE_DEFINITION_ID
import io.airbyte.workload.launcher.constants.GCS_DATALAKE_DOCKER_IMAGE
import io.airbyte.workload.launcher.constants.PodConstants.CPU_RESOURCE_KEY
import io.airbyte.workload.launcher.constants.S3_DATALAKE_DEFINITION_ID
import io.airbyte.workload.launcher.constants.S3_DATALAKE_DOCKER_IMAGE
import io.airbyte.workload.launcher.pipeline.stages.model.ArchitectureEnvironmentVariables
import io.airbyte.workload.launcher.pipeline.stages.model.IPCOptions
import io.airbyte.workload.launcher.pipeline.stages.model.Serialization
import io.airbyte.workload.launcher.pipeline.stages.model.Transport
import io.airbyte.workload.launcher.pods.ResourceConversionUtils
import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID
import kotlin.math.min

private val logger = KotlinLogging.logger {}

@Singleton
class ArchitectureDecider(
  private val featureFlags: FeatureFlagClient,
  private val airbyteApiClient: AirbyteApiClient,
) {
  /**
   * Decide what environment variables each container needs for the given replication.
   */
  fun computeEnvironmentVariables(input: ReplicationInput): ArchitectureEnvironmentVariables {
    if (input.useFileTransfer) {
      logger.info { "Building STDIO environment: file transfer mode enabled" }
      return buildLegacyEnvironment()
    }

    if (input.isReset) {
      logger.info { "Building STDIO environment: reset mode enabled" }
      return buildLegacyEnvironment()
    }

    val ffContext = input.toFeatureFlagContext()

    if (featureFlags.boolVariation(ForceRunStdioMode, ffContext)) {
      logger.info { "Building STDIO environment: ForceRunStdioMode flag enabled" }
      return buildLegacyEnvironment()
    }

    if (featureFlags.boolVariation(SocketTest, ffContext)) {
      logger.info { "Building socket environment: SocketTest flag enabled" }
      return buildSocketEnvironment(input, Serialization.PROTOBUF.name, Transport.SOCKET.name, BOOKKEEPER, ffContext)
    }
    if (input.sourceIPCOptions == null || input.sourceIPCOptions.isNull) {
      logger.info { "Building STDIO environment: source IPC options missing" }
      return buildLegacyEnvironment()
    }

    if (input.destinationIPCOptions == null || input.destinationIPCOptions.isNull) {
      logger.info { "Building STDIO environment: destination IPC options missing" }
      return buildLegacyEnvironment()
    }

    val connection = airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(input.connectionId))
    if (hasHashedFields(connection)) {
      logger.info { "Building STDIO environment: connection has hashed fields" }
      return buildLegacyEnvironment()
    }

    if (hasMappers(connection)) {
      logger.info { "Building STDIO environment: connection has mappers" }
      return buildLegacyEnvironment()
    }

    val (srcOpts: IPCOptions?, dstOpts: IPCOptions?) =
      try {
        Jsons.`object`(input.sourceIPCOptions, IPCOptions::class.java) to
          Jsons.`object`(input.destinationIPCOptions, IPCOptions::class.java)
      } catch (e: Exception) {
        logger.warn(e) { "Failed to parse IPCOptions, falling back to standard mode" }
        logger.info { "Building STDIO environment: Failed to parse IPCOptions" }
        return buildLegacyEnvironment()
      }

    if (srcOpts == null) {
      logger.info { "Building STDIO environment: parsed source IPCOptions is null" }
      return buildLegacyEnvironment()
    }

    if (dstOpts == null) {
      logger.info { "Building STDIO environment: parsed destination IPCOptions is null" }
      return buildLegacyEnvironment()
    }

    // Version guard
    if (srcOpts.dataChannel.version != dstOpts.dataChannel.version) {
      logger.warn {
        "Data‑channel version mismatch (${srcOpts.dataChannel.version} vs " +
          "${dstOpts.dataChannel.version}); falling back to STDIO mode."
      }
      logger.info { "Building STDIO environment: Data-channel version mismatch" }
      return buildLegacyEnvironment()
    }

    val commonSerialization =
      srcOpts.dataChannel.supportedSerialization
        .intersect(dstOpts.dataChannel.supportedSerialization.toSet())

    val serialization =
      when {
        Serialization.PROTOBUF in commonSerialization -> Serialization.PROTOBUF.name
        Serialization.JSONL in commonSerialization -> Serialization.JSONL.name
        else -> throw IllegalArgumentException("No common data‑serialisation format between source and destination.")
      }

    val commonTransport =
      srcOpts.dataChannel.supportedTransport
        .intersect(dstOpts.dataChannel.supportedTransport.toSet())

    val transport =
      when {
        Transport.SOCKET in commonTransport -> Transport.SOCKET.name
        Transport.STDIO in commonTransport -> Transport.STDIO.name
        else -> throw IllegalArgumentException("No common data‑transport medium between source and destination.")
      }

    return if (transport == Transport.SOCKET.name) {
      logger.info { "Building socket environment: transport=$transport, serialization=$serialization" }
      buildSocketEnvironment(input, serialization, transport, BOOKKEEPER, ffContext)
    } else {
      logger.info { "Building non-socket environment: transport=$transport, serialization=$serialization" }
      buildNonSocketEnvironment(serialization, transport, ORCHESTRATOR)
    }
  }

  private fun hasHashedFields(connection: ConnectionRead): Boolean =
    connection.syncCatalog.streams.any { stream ->
      stream.config?.hashedFields?.isNotEmpty() == true
    }

  private fun hasMappers(connection: ConnectionRead): Boolean =
    connection.syncCatalog.streams.any { stream ->
      stream.config?.mappers?.isNotEmpty() == true
    }

  private fun buildSocketEnvironment(
    input: ReplicationInput,
    serialisation: String,
    transport: String,
    platformMode: String,
    ffContext: Context,
  ): ArchitectureEnvironmentVariables {
    // 0. Decide serialisation format
    val serialisationOverride = featureFlags.stringVariation(SocketFormat, ffContext).trim()
    val finalSerialisation: String =
      runCatching {
        Serialization.valueOf(serialisationOverride)
        serialisationOverride
      }.getOrElse {
        serialisation
      }
    // 1. Decide socket‑count (override flag > destination+dedup check > CPU‑based heuristic)
    val overrideCnt = featureFlags.intVariation(SocketCount, ffContext)
    val socketCount =
      when {
        overrideCnt > 0 -> overrideCnt
        shouldOverrideSocketCountToOne(input) -> 1
        else -> {
          val defaultCnt =
            min(
              extractCpuLimit(input, isSource = true),
              extractCpuLimit(input, isSource = false),
            ) * 2
          defaultCnt
        }
      }

    // 2. Build comma‑separated socket paths
    val socketPaths =
      (1..socketCount)
        .joinToString(separator = ",") { "$SOCKET_PATH/$SOCKET_FILE_PREFIX$it$SOCKET_FILE_POSTFIX" }

    // 3. Assembling ENV lists
    val baseEnv =
      envList(finalSerialisation, transport) +
        EnvVar(DATA_CHANNEL_SOCKET_PATHS, socketPaths, null)

    return ArchitectureEnvironmentVariables(
      sourceEnvironmentVariables = baseEnv,
      platformEnvironmentVariables = listOf(EnvVar(PLATFORM_MODE, platformMode, null)),
      destinationEnvironmentVariables = baseEnv,
    )
  }

  companion object {
    fun buildLegacyEnvironment(): ArchitectureEnvironmentVariables =
      buildNonSocketEnvironment(Serialization.JSONL.name, Transport.STDIO.name, ORCHESTRATOR)

    private fun buildNonSocketEnvironment(
      serialisation: String,
      transport: String,
      platformMode: String,
    ): ArchitectureEnvironmentVariables =
      ArchitectureEnvironmentVariables(
        sourceEnvironmentVariables = envList(serialisation, transport),
        platformEnvironmentVariables = listOf(EnvVar(PLATFORM_MODE, platformMode, null)),
        destinationEnvironmentVariables = envList(serialisation, transport),
      )

    private fun envList(
      serialisation: String,
      transport: String,
    ): List<EnvVar> =
      listOf(
        EnvVar(DATA_CHANNEL_FORMAT, serialisation, null),
        EnvVar(DATA_CHANNEL_MEDIUM, transport, null),
      )
  }

  private fun hasAnyDedupStream(input: ReplicationInput): Boolean =
    input.catalog?.streams?.any { stream ->
      stream.destinationSyncMode == io.airbyte.config.DestinationSyncMode.APPEND_DEDUP ||
        stream.destinationSyncMode == io.airbyte.config.DestinationSyncMode.OVERWRITE_DEDUP
    } ?: false

  private fun shouldOverrideSocketCountToOne(input: ReplicationInput): Boolean {
    val destinationDefinitionId = input.connectionContext?.destinationDefinitionId
    val dockerImage = input.destinationLauncherConfig?.dockerImage

    val dockerImageWithoutTag = dockerImage?.substringBefore(':')

    val isIceberg = isIcebergConnector(destinationDefinitionId, dockerImageWithoutTag)

    return isIceberg && hasAnyDedupStream(input)
  }

  private fun isIcebergConnector(
    actorDefinitionId: UUID?,
    dockerImage: String?,
  ): Boolean =
    actorDefinitionId == GCS_DATALAKE_DEFINITION_ID || actorDefinitionId == S3_DATALAKE_DEFINITION_ID ||
      dockerImage == GCS_DATALAKE_DOCKER_IMAGE || dockerImage == S3_DATALAKE_DOCKER_IMAGE

  private fun extractCpuLimit(
    input: ReplicationInput,
    isSource: Boolean,
  ): Int {
    val res: ResourceRequirements =
      if (isSource) {
        ResourceConversionUtils.domainToApi(input.syncResourceRequirements.source)
      } else {
        ResourceConversionUtils.domainToApi(input.syncResourceRequirements.destination)
      }

    return res.limits
      ?.get(CPU_RESOURCE_KEY)
      ?.numericalAmount
      ?.toInt()
      ?.takeIf { it > 0 } ?: 1
  }
}
