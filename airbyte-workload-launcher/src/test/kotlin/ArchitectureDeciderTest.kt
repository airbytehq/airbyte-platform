/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.SocketCount
import io.airbyte.featureflag.SocketTest
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.Jsons
import io.airbyte.workers.models.ArchitectureConstants.BOOKKEEPER
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_FORMAT
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_MEDIUM
import io.airbyte.workers.models.ArchitectureConstants.DATA_CHANNEL_SOCKET_PATHS
import io.airbyte.workers.models.ArchitectureConstants.ORCHESTRATOR
import io.airbyte.workers.models.ArchitectureConstants.PLATFORM_MODE
import io.airbyte.workload.launcher.pipeline.stages.model.DataChannel
import io.airbyte.workload.launcher.pipeline.stages.model.IPCOptions
import io.airbyte.workload.launcher.pipeline.stages.model.Serialization
import io.airbyte.workload.launcher.pipeline.stages.model.Transport
import io.fabric8.kubernetes.api.model.EnvVar
import io.mockk.every
import io.mockk.mockk
import io.mockk.unmockkAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.UUID

class ArchitectureDeciderTest {
  private val featureFlags = mockk<FeatureFlagClient>(relaxed = true)
  private val airbyteApiClient = mockk<AirbyteApiClient>(relaxed = true)

  private val decider = ArchitectureDecider(featureFlags, airbyteApiClient)

  private fun ipcNode(
    version: String = "1.0.0",
    serializations: List<Serialization>,
    transports: List<Transport>,
  ): JsonNode = Jsons.jsonNode(IPCOptions(DataChannel(version, serializations, transports)))

  private fun input(
    connectionId: UUID = UUID.randomUUID(),
    mapperCount: Int = 0,
    srcIpc: JsonNode? = null,
    dstIpc: JsonNode? = null,
    useFileTransfer: Boolean = false,
    isReset: Boolean = false,
    cpuLimit: Int = 1,
  ): ReplicationInput {
    val connectionRead =
      mockk<ConnectionRead> {
        val airbyteStreamAndConfiguration =
          mockk<AirbyteStreamAndConfiguration> {
            val streamConfiguration =
              mockk<AirbyteStreamConfiguration> {
                every { hashedFields } returns emptyList()
                every { mappers } returns if (mapperCount > 0) List(mapperCount) { mockk<ConfiguredStreamMapper>() } else emptyList()
              }

            every { config } returns streamConfiguration
          }
        every { syncCatalog.streams } returns listOf(airbyteStreamAndConfiguration)
      }

    every { airbyteApiClient.connectionApi.getConnection(any()) } returns connectionRead

    val domainRes: io.airbyte.config.ResourceRequirements =
      io.airbyte.config
        .ResourceRequirements()
        .withCpuLimit(cpuLimit.toString())
    val syncRes =
      mockk<io.airbyte.config.SyncResourceRequirements> {
        every { source } returns domainRes
        every { destination } returns domainRes
      }

    val input = mockk<ReplicationInput>()
    every { input.connectionId } returns connectionId
    every { input.sourceIPCOptions } returns srcIpc
    every { input.destinationIPCOptions } returns dstIpc
    every { input.useFileTransfer } returns useFileTransfer
    every { input.isReset } returns isReset
    every { input.syncResourceRequirements } returns syncRes
    return input
  }

  private fun List<EnvVar>.valueOf(name: String) = first { it.name == name }.value

  @AfterEach fun tearDown() = unmockkAll()

  @Test
  fun `SocketTest flag forces socket mode`() {
    val conn = UUID.randomUUID()
    every { featureFlags.boolVariation(SocketTest, Connection(conn)) } returns true

    val env = decider.computeEnvironmentVariables(input(connectionId = conn))

    Assertions.assertEquals(Transport.SOCKET.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_MEDIUM))
    Assertions.assertEquals(BOOKKEEPER, env.platformEnvironmentVariables.valueOf(PLATFORM_MODE))
  }

  @Test
  fun `presence of mappers falls back to legacy`() {
    val env = decider.computeEnvironmentVariables(input(mapperCount = 2))

    Assertions.assertEquals(Transport.STDIO.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_MEDIUM))
    Assertions.assertEquals(ORCHESTRATOR, env.platformEnvironmentVariables.valueOf(PLATFORM_MODE))
  }

  @Test
  fun `missing IPC info falls back to legacy`() {
    val env = decider.computeEnvironmentVariables(input())

    Assertions.assertEquals(Serialization.JSONL.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_FORMAT))
    Assertions.assertEquals(ORCHESTRATOR, env.platformEnvironmentVariables.valueOf(PLATFORM_MODE))
  }

  @Test
  fun `version mismatch in IPC options falls back to legacy`() {
    val env =
      decider.computeEnvironmentVariables(
        input(
          srcIpc = ipcNode(version = "1.0.0", serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.SOCKET)),
          dstIpc = ipcNode(version = "2.0.0", serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.SOCKET)),
        ),
      )

    Assertions.assertEquals(Transport.STDIO.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_MEDIUM))
  }

  @Test
  fun `PROTOBUF plus SOCKET selects socket mode with default socket count`() {
    every { featureFlags.intVariation(SocketCount, any()) } returns 0 // no override

    val env =
      decider.computeEnvironmentVariables(
        input(
          srcIpc =
            ipcNode(
              serializations = listOf(Serialization.PROTOBUF, Serialization.JSONL),
              transports = listOf(Transport.SOCKET, Transport.STDIO),
            ),
          dstIpc =
            ipcNode(
              serializations = listOf(Serialization.JSONL, Serialization.PROTOBUF),
              transports = listOf(Transport.SOCKET, Transport.STDIO),
            ),
          cpuLimit = 1, // → min(cpu) * 4 = 4 sockets
        ),
      )

    Assertions.assertEquals(Transport.SOCKET.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_MEDIUM))
    Assertions.assertEquals(BOOKKEEPER, env.platformEnvironmentVariables.valueOf(PLATFORM_MODE))
    Assertions.assertEquals(
      "/var/run/sockets/airbyte_socket_1.sock,/var/run/sockets/airbyte_socket_2.sock," +
        "/var/run/sockets/airbyte_socket_3.sock,/var/run/sockets/airbyte_socket_4.sock",
      env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_SOCKET_PATHS),
    )
    Assertions.assertEquals(
      "/var/run/sockets/airbyte_socket_1.sock,/var/run/sockets/airbyte_socket_2.sock," +
        "/var/run/sockets/airbyte_socket_3.sock,/var/run/sockets/airbyte_socket_4.sock",
      env.destinationEnvironmentVariables.valueOf(DATA_CHANNEL_SOCKET_PATHS),
    )
    Assertions.assertEquals(
      4,
      env.sourceEnvironmentVariables
        .valueOf(DATA_CHANNEL_SOCKET_PATHS)
        .split(',')
        .size,
    )
    Assertions.assertEquals(
      4,
      env.destinationEnvironmentVariables
        .valueOf(DATA_CHANNEL_SOCKET_PATHS)
        .split(',')
        .size,
    )
  }

  @Test
  fun `SocketCount flag overrides CPU heuristic`() {
    val conn = UUID.randomUUID()
    every { featureFlags.intVariation(SocketCount, Connection(conn)) } returns 3

    val env =
      decider.computeEnvironmentVariables(
        input(
          connectionId = conn,
          srcIpc = ipcNode(serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.SOCKET)),
          dstIpc = ipcNode(serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.SOCKET)),
        ),
      )

    Assertions.assertEquals(
      3,
      env.sourceEnvironmentVariables
        .valueOf(DATA_CHANNEL_SOCKET_PATHS)
        .split(',')
        .size,
    )
  }

  @Test
  fun `JSONL plus STDIO selects non‑socket orchestrator mode`() {
    val env =
      decider.computeEnvironmentVariables(
        input(
          srcIpc = ipcNode(serializations = listOf(Serialization.JSONL), transports = listOf(Transport.STDIO)),
          dstIpc = ipcNode(serializations = listOf(Serialization.JSONL), transports = listOf(Transport.STDIO)),
        ),
      )

    Assertions.assertEquals(Serialization.JSONL.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_FORMAT))
    Assertions.assertEquals(Transport.STDIO.name, env.sourceEnvironmentVariables.valueOf(DATA_CHANNEL_MEDIUM))
    Assertions.assertEquals(ORCHESTRATOR, env.platformEnvironmentVariables.valueOf(PLATFORM_MODE))
  }

  @Test
  fun `no common serialization format throws`() {
    Assertions.assertThrows(IllegalArgumentException::class.java) {
      decider.computeEnvironmentVariables(
        input(
          srcIpc = ipcNode(serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.STDIO)),
          dstIpc = ipcNode(serializations = listOf(Serialization.JSONL), transports = listOf(Transport.STDIO)),
        ),
      )
    }
  }

  @Test
  fun `no common transport medium throws`() {
    Assertions.assertThrows(IllegalArgumentException::class.java) {
      decider.computeEnvironmentVariables(
        input(
          srcIpc = ipcNode(serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.SOCKET)),
          dstIpc = ipcNode(serializations = listOf(Serialization.PROTOBUF), transports = listOf(Transport.STDIO)),
        ),
      )
    }
  }
}
