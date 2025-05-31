/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ConnectionContext
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ConnectorJobOutput.OutputType
import io.airbyte.config.FailureReason
import io.airbyte.config.Organization
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.data.services.CatalogService
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.server.helpers.WorkloadIdGenerator
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.server.repositories.domain.Command
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.output.DocStoreAccessException
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.ConflictException
import io.airbyte.workload.services.WorkloadService
import io.micronaut.data.exceptions.DataAccessException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class CommandServiceTest {
  private lateinit var catalogService: CatalogService
  private lateinit var commandsRepository: CommandsRepository
  private lateinit var jobInputService: JobInputService
  private lateinit var workloadService: WorkloadService
  private lateinit var workloadQueueService: WorkloadQueueService
  private lateinit var workloadOutputReader: WorkloadOutputDocStoreReader
  private lateinit var service: CommandService

  @BeforeEach
  fun setUp() {
    commandsRepository =
      mockk {
        every { findById(COMMAND_ID) } returns Optional.of(defaultCheckCommand)
        every { findById(UNKNOWN_COMMAND_ID) } returns Optional.empty()
        every { findById(DISCOVER_COMMAND_ID) } returns Optional.of(defaultDiscoverCommand)
      }

    catalogService = mockk()
    jobInputService = mockk()
    workloadService = mockk(relaxed = true)
    workloadQueueService = mockk(relaxed = true)
    workloadOutputReader = mockk(relaxed = true)
    service =
      CommandService(
        actorRepository = mockk(relaxed = true),
        catalogService = catalogService,
        commandsRepository = commandsRepository,
        jobInputService = jobInputService,
        logClientManager = mockk(relaxed = true),
        organizationService = mockk(relaxed = true) { every { getOrganizationForWorkspaceId(any()) } returns Optional.of(ORGANIZATION) },
        workloadService = workloadService,
        workloadQueueService = workloadQueueService,
        workloadOutputReader = workloadOutputReader,
        workspaceService = mockk(relaxed = true),
        workspaceRoot = Path.of("/test-root"),
        workloadIdGenerator = WorkloadIdGenerator(),
        discoverAutoRefreshWindowMinutes = 0,
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
  fun `createCheck for actorDef returns false if command exists`() {
    every { commandsRepository.existsById(COMMAND_ID) } returns true
    val output =
      service.createCheckCommand(
        commandId = COMMAND_ID,
        actorDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        configuration = Jsons.emptyObject(),
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertFalse(output)
  }

  @Test
  fun `createCheck for actorId returns false if command exists`() {
    every { commandsRepository.existsById(COMMAND_ID) } returns true
    val output =
      service.createCheckCommand(
        commandId = COMMAND_ID,
        actorId = UUID.randomUUID(),
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertFalse(output)
  }

  @Test
  fun `createDiscover returns false if command exists`() {
    every { commandsRepository.existsById(COMMAND_ID) } returns true
    val output =
      service.createDiscoverCommand(
        commandId = COMMAND_ID,
        actorId = UUID.randomUUID(),
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertFalse(output)
  }

  @Test
  fun `createReplicate returns false if command exists`() {
    every { commandsRepository.existsById(COMMAND_ID) } returns true
    val output =
      service.createReplicateCommand(
        commandId = COMMAND_ID,
        connectionId = UUID.randomUUID(),
        jobId = "123",
        attemptNumber = 0,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
        appliedCatalogDiff = null,
      )
    assertFalse(output)
  }

  @Test
  fun `creating a command successfully saves the command and enqueues the workload`() {
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getDiscoverInput(any(), any(), any()) } returns
      DiscoverCommandInput.DiscoverCatalogInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        integrationLauncherConfig = IntegrationLauncherConfig(),
        discoverCatalogInput = StandardDiscoverCatalogInput(),
      )
    every { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns
      mockk()
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    val output =
      service.createDiscoverCommand(
        commandId = COMMAND_ID,
        actorId = UUID.randomUUID(),
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertTrue(output)

    verify { commandsRepository.save(any()) }
    verify { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `creating a command still succeeds if the workload already exists saves the command but doesn't enqueue`() {
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getDiscoverInput(any(), any(), any()) } returns
      DiscoverCommandInput.DiscoverCatalogInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        integrationLauncherConfig = IntegrationLauncherConfig(),
        discoverCatalogInput = StandardDiscoverCatalogInput(),
      )
    every { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } throws
      ConflictException("dupl")
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } throws DataAccessException("dupl")

    // The edge case here is that creating a workload that already exists will throw a DataAccessException
    // on the workloadQueueService because the workload has already been created, enqueued etc.
    // The common case is discover with a snap window. It is expected that different connection generates
    // the same workload if they have the same source.
    val output =
      service.createDiscoverCommand(
        commandId = COMMAND_ID,
        actorId = UUID.randomUUID(),
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertTrue(output)

    verify { commandsRepository.save(any()) }
    verify(exactly = 0) { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `createReplicate sets the mutexKey`() {
    val connectionId = UUID.randomUUID()
    val jobId = "12345"
    val attemptNumber = 0L
    val signalInput = "signal-input"

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns
      mockk()
    every { jobInputService.getReplicationInput(any(), any(), any(), any(), any()) } returns
      ReplicationActivityInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        connectionContext =
          ConnectionContext()
            .withConnectionId(
              connectionId,
            ).withWorkspaceId(WORKSPACE_ID)
            .withOrganizationId(ORGANIZATION.organizationId),
      )

    service.createReplicateCommand(
      commandId = COMMAND_ID,
      connectionId = connectionId,
      jobId = jobId,
      attemptNumber = attemptNumber,
      appliedCatalogDiff = null,
      signalInput = signalInput,
      commandInput = Jsons.emptyObject(),
    )
    verify {
      workloadService.createWorkload(
        workloadId = any(),
        labels = any(),
        input = any(),
        workspaceId = WORKSPACE_ID,
        organizationId = ORGANIZATION.organizationId,
        logPath = any(),
        mutexKey = connectionId.toString(),
        type = WorkloadType.SYNC,
        autoId = any(),
        deadline = any(),
        signalInput = signalInput,
        dataplaneGroup = any(),
        priority = WorkloadPriority.DEFAULT,
      )
    }
  }

  @Test
  fun `getConnectorJobOutput returns the output`() {
    val expectedOutput = ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns expectedOutput

    val output = service.getCheckJobOutput(COMMAND_ID)
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getConnectorJobOutput does nothing for unknown command ids`() {
    val output = service.getCheckJobOutput(UNKNOWN_COMMAND_ID)
    verify(exactly = 0) { workloadOutputReader.readConnectorOutput(any()) }
    assertNull(output)
  }

  @Test
  fun `getDiscoverJobOutput looks up the catalog if there is a catalogId`() {
    val discoverCatalogId = UUID.randomUUID()
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.DISCOVER_CATALOG_ID).withDiscoverCatalogId(discoverCatalogId)
    every { workloadOutputReader.readConnectorOutput(DISCOVER_WORKLOAD_ID) } returns workloadOutput
    val discoverCatalog =
      ActorCatalog().withCatalog(
        Jsons.jsonNode(
          AirbyteCatalog().withStreams(
            listOf(AirbyteStream().withName("streamname")),
          ),
        ),
      )
    every { catalogService.getActorCatalogById(discoverCatalogId) } returns discoverCatalog
    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID)
    val expectedOutput =
      CommandService.DiscoverJobOutput(
        catalogId = discoverCatalogId,
        catalog = discoverCatalog,
        failureReason = null,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getDiscoverJobOutput returns failure without crashing`() {
    val failure = FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withFailureType(FailureReason.FailureType.CONFIG_ERROR)
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.DISCOVER_CATALOG_ID).withFailureReason(failure)
    every { workloadOutputReader.readConnectorOutput(DISCOVER_WORKLOAD_ID) } returns workloadOutput
    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID)
    val expectedOutput =
      CommandService.DiscoverJobOutput(
        catalogId = null,
        catalog = null,
        failureReason = failure,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getReplicationOutput returns an output`() {
    val expectedOutput =
      ReplicationOutput()
        .withReplicationAttemptSummary(ReplicationAttemptSummary())
        .withFailures(
          listOf(
            FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withExternalMessage("Something to validate"),
          ),
        )
    every { workloadOutputReader.readSyncOutput(WORKLOAD_ID) } returns expectedOutput
    val output = service.getReplicationOutput(COMMAND_ID)
    assertEquals(expectedOutput, output)
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

  @Test
  fun `verify we return failure reason when we fail to read from the doc store`() {
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } throws DocStoreAccessException("boom", Exception("boom"))
    val output = service.getCheckJobOutput(COMMAND_ID)
    assertNotNull(output?.failureReason)
  }

  @Test
  fun `verify we return failure reason when we read an empty response from the doc store`() {
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns null
    val output = service.getCheckJobOutput(COMMAND_ID)
    assertNotNull(output?.failureReason)
  }

  companion object {
    val ORGANIZATION = Organization().withOrganizationId(UUID.randomUUID())
    val WORKSPACE_ID = UUID.randomUUID()
    const val UNKNOWN_COMMAND_ID = "i-do-not-exist"
    const val COMMAND_ID = "command-123"
    const val WORKLOAD_ID = "workload-12-123"

    val defaultCheckCommand =
      Command(
        id = COMMAND_ID,
        workloadId = WORKLOAD_ID,
        commandType = CommandType.CHECK.name,
        commandInput = Jsons.deserialize("""{"yo":"lo"}"""),
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    const val DISCOVER_COMMAND_ID = "discover-123"
    const val DISCOVER_WORKLOAD_ID = "workload-13-321"
    val defaultDiscoverCommand =
      Command(
        id = DISCOVER_COMMAND_ID,
        workloadId = DISCOVER_WORKLOAD_ID,
        commandType = CommandType.DISCOVER.name,
        commandInput = Jsons.deserialize("""{"discover":"payload"}"""),
        workspaceId = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
  }
}
