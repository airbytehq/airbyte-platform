/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.workload.launcher.constants.PodConstants.CPU_RESOURCE_KEY
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
    if (input.useFileTransfer ||
      input.isReset
    ) {
      return buildLegacyEnvironment()
    }

    val ffContext = input.toFeatureFlagContext()

    if (featureFlags.boolVariation(ForceRunStdioMode, ffContext)) {
      return buildLegacyEnvironment()
    }

    if (featureFlags.boolVariation(SocketTest, ffContext)) {
      return buildSocketEnvironment(input, Serialization.PROTOBUF.name, Transport.SOCKET.name, BOOKKEEPER, ffContext)
    }
    val ipcInfoMissing =
      input.sourceIPCOptions == null ||
        input.sourceIPCOptions.isNull ||
        input.destinationIPCOptions == null ||
        input.destinationIPCOptions.isNull

    if (ipcInfoMissing ||
      hasHashedFieldsOrMappers(airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(input.connectionId)))
    ) {
      return buildLegacyEnvironment()
    }

    val (srcOpts: IPCOptions?, dstOpts: IPCOptions?) =
      try {
        Jsons.`object`(input.sourceIPCOptions, IPCOptions::class.java) to
          Jsons.`object`(input.destinationIPCOptions, IPCOptions::class.java)
      } catch (e: Exception) {
        logger.warn(e) { "Failed to parse IPCOptions, falling back to legacy mode" }
        return buildLegacyEnvironment()
      }

    if (srcOpts == null || dstOpts == null) {
      return buildLegacyEnvironment()
    }

    // Version guard
    if (srcOpts.dataChannel.version != dstOpts.dataChannel.version) {
      logger.warn {
        "Data‑channel version mismatch (${srcOpts.dataChannel.version} vs " +
          "${dstOpts.dataChannel.version}); falling back to legacy mode."
      }
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
      buildSocketEnvironment(input, serialization, transport, BOOKKEEPER, ffContext)
    } else {
      buildNonSocketEnvironment(serialization, transport, ORCHESTRATOR)
    }
  }

  private fun hasHashedFieldsOrMappers(connection: ConnectionRead): Boolean =
    connection.syncCatalog.streams.any { stream ->
      val config = stream.config
      val hasHashedFields = config?.hashedFields?.isNotEmpty() == true
      val hasMappers = config?.mappers?.isNotEmpty() == true
      hasHashedFields || hasMappers
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
    private val GCS_DATALAKE_DEFINITION_ID: UUID = UUID.fromString("8c8a2d3e-1b4f-4a9c-9e7d-6f5a4b3c2d1e") // GCS Datalake
    private const val GCS_DATALAKE_DOCKER_REPOSITORY = "airbyte/destination-gcs-data-lake"

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

    val dockerImageRepository = dockerImage?.substringBefore(':')

    val matchesDestination =
      (destinationDefinitionId == GCS_DATALAKE_DEFINITION_ID) ||
        (dockerImageRepository == GCS_DATALAKE_DOCKER_REPOSITORY)

    return matchesDestination && hasAnyDedupStream(input)
  }

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
      ?.amount
      ?.toIntOrNull()
      ?.takeIf { it > 0 } ?: 1
  }
}
