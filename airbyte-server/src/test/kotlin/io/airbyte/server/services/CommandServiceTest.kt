/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ConnectorJobOutput.OutputType
import io.airbyte.protocol.models.Jsons
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.server.repositories.domain.Command
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.WorkloadService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class CommandServiceTest {
  private lateinit var commandsRepository: CommandsRepository
  private lateinit var workloadService: WorkloadService
  private lateinit var workloadOutputReader: WorkloadOutputDocStoreReader
  private lateinit var service: CommandService

  @BeforeEach
  fun setUp() {
    commandsRepository =
      mockk {
        every { findById(COMMAND_ID) } returns Optional.of(defaultCheckCommand)
        every { findById(UNKNOWN_COMMAND_ID) } returns Optional.empty()
      }

    workloadService = mockk(relaxed = true)
    workloadOutputReader = mockk(relaxed = true)
    service =
      CommandService(
        actorRepository = mockk(),
        commandsRepository = commandsRepository,
        jobInputService = mockk(),
        logClientManager = mockk(),
        organizationService = mockk(),
        workloadService = workloadService,
        workloadQueueService = mockk(),
        workloadOutputReader = workloadOutputReader,
        workspaceService = mockk(),
        workspaceRoot = Path.of("/test-root"),
      )
  }

  @Test
  fun `cancel calls cancelWorkload`() {
    service.cancel(COMMAND_ID)
    verify { workloadService.cancelWorkload(WORKLOAD_ID, any(), any()) }
  }

  @Test
  fun `cancel does nothing for unknown command ids`() {
    service.cancel(UNKNOWN_COMMAND_ID)
    verify(exactly = 0) { workloadService.cancelWorkload(any(), any(), any()) }
  }

  @Test
  fun `getConnectorJobOutput returns the output`() {
    val expectedOutput = ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns expectedOutput

    val output = service.getConnectorJobOutput(COMMAND_ID)
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getConnectorJobOutput does nothing for unknown command ids`() {
    val output = service.getConnectorJobOutput(UNKNOWN_COMMAND_ID)
    verify(exactly = 0) { workloadOutputReader.readConnectorOutput(any()) }
    assertNull(output)
  }

  @Test
  fun `getStatus returns the correct CommandStatus`() {
    val workload: Workload = mockk { every { status } returns WorkloadStatus.SUCCESS }
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload

    val status = service.getStatus(COMMAND_ID)
    assertEquals(CommandStatus.COMPLETED, status)
  }

  @Test
  fun `getStatus returns null for unknown command ids`() {
    val status = service.getStatus(UNKNOWN_COMMAND_ID)
    verify(exactly = 0) { workloadService.getWorkload(any()) }
    assertNull(status)
  }

  companion object {
    const val UNKNOWN_COMMAND_ID = "i-do-not-exist"
    const val COMMAND_ID = "command-123"
    const val WORKLOAD_ID = "workload-12-123"

    val defaultCheckCommand =
      Command(
        id = COMMAND_ID,
        workloadId = WORKLOAD_ID,
        commandType = "Check",
        commandInput = Jsons.deserialize("""{"yo":"lo"}"""),
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
  }
}
