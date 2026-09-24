/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.problems.throwable.generated.ActorNotReadyProblem
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectionContext
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ConnectorJobOutput.OutputType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.FailureReason
import io.airbyte.config.Organization
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.repositories.entities.Actor
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationCatalog
import io.airbyte.protocol.models.v0.DestinationOperation
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.server.helpers.WorkloadIdGenerator
import io.airbyte.server.repositories.CommandsRepository
import io.airbyte.server.repositories.domain.Command
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.output.DocStoreAccessException
import io.airbyte.workload.output.WorkloadOutputDocStoreReader
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.services.ConflictException
import io.airbyte.workload.services.InvalidStatusTransitionException
import io.airbyte.workload.services.WorkloadService
import io.micronaut.data.exceptions.DataAccessException
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.nio.file.Path
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType as JooqActorType

class CommandServiceTest {
  private lateinit var actorRepository: ActorRepository
  private lateinit var catalogService: CatalogService
  private lateinit var commandsRepository: CommandsRepository
  private lateinit var jobInputService: JobInputService
  private lateinit var logClientManager: LogClientManager
  private lateinit var workloadService: WorkloadService
  private lateinit var workloadQueueService: WorkloadQueueService
  private lateinit var workloadOutputReader: WorkloadOutputDocStoreReader
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var actorDefinitionVersionHelper: io.airbyte.config.persistence.ActorDefinitionVersionHelper
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
    logClientManager = mockk(relaxed = true)
    workloadService = mockk(relaxed = true)
    workloadQueueService = mockk(relaxed = true)
    workloadOutputReader = mockk(relaxed = true)
    featureFlagClient = mockk(relaxed = true)
    actorRepository = mockk(relaxed = true)
    sourceService = mockk(relaxed = true)
    destinationService = mockk(relaxed = true)
    actorDefinitionVersionHelper = mockk(relaxed = true)
    service =
      CommandService(
        actorRepository = actorRepository,
        catalogService = catalogService,
        commandsRepository = commandsRepository,
        jobInputService = jobInputService,
        logClientManager = logClientManager,
        organizationService = mockk(relaxed = true) { every { getOrganizationForWorkspaceId(any()) } returns Optional.of(ORGANIZATION) },
        workloadService = workloadService,
        workloadQueueService = workloadQueueService,
        workloadOutputReader = workloadOutputReader,
        workloadIdGenerator = WorkloadIdGenerator(),
        workspaceService = mockk(relaxed = true),
        secretSanitizer = mockk(relaxed = true),
        configurationUpdate = mockk(relaxed = true),
        sourceService = sourceService,
        destinationService = destinationService,
        actorDefinitionVersionHelper = actorDefinitionVersionHelper,
        jsonSchemaValidator = JsonSchemaValidator(),
        airbyteConfig = AirbyteConfig(workspaceRoot = "/test-root"),
        airbyteWorkerConfig =
          AirbyteWorkerConfig(
            discover = AirbyteWorkerConfig.AirbyteWorkerDiscoverConfig(autoRefreshWindow = 0),
          ),
        featureFlagClient = featureFlagClient,
        clock = null,
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
  fun `cancel does nothing for workloads that are already terminated`() {
    every { workloadService.cancelWorkload(WORKLOAD_ID, any(), any()) } throws InvalidStatusTransitionException("I am done")
    service.cancel(COMMAND_ID)
    verify(exactly = 1) { workloadService.cancelWorkload(WORKLOAD_ID, any(), any()) }
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
  fun `creating a check command successfully saves the command and enqueues the workload`() {
    val actorId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val configurationOverride = Jsons.deserialize("""{"token":"ready-actor-override"}""")
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    val workloadInput = slot<String>()
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = actorDefinitionId,
        name = "ready source",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.source,
      )
    every { sourceService.getSourceConnection(actorId) } returns
      SourceConnection()
        .withSourceId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(actorDefinitionId)
        .withConfiguration(Jsons.emptyObject())
        .withIsDraft(false)
    every { jobInputService.getCheckInput(actorId, null, null, configurationOverride, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(configurationOverride),
      )
    every {
      workloadService.createWorkload(any(), any(), capture(workloadInput), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } returns
      mockk()
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    val output =
      service.createCheckCommand(
        commandId = COMMAND_ID,
        actorId = actorId,
        configuration = configurationOverride,
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.HIGH,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertTrue(output)

    verify { commandsRepository.save(any()) }
    verify { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }

    // Ensuring this is added because it impacts nodepool selection in the launcher
    val actualInput = Jsons.deserialize(workloadInput.captured)
    assertEquals(WorkloadPriority.HIGH.toString(), actualInput["launcherConfig"]["priority"].asText())
    assertEquals(configurationOverride, actualInput["checkConnectionInput"]["connectionConfiguration"])
  }

  @Test
  fun `draft source check rejects a configuration override instead of validating stale data`() {
    val actorId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val override = Jsons.deserialize("""{"token":"stale-but-valid"}""")
    val storedConfiguration = Jsons.emptyObject()
    val sourceDefinition = StandardSourceDefinition().withSourceDefinitionId(definitionId)
    val specification =
      ConnectorSpecification().withConnectionSpecification(
        Jsons.deserialize("""{"type":"object","required":["token"],"properties":{"token":{"type":"string"}}}"""),
      )

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = definitionId,
        name = "draft source",
        configuration = storedConfiguration,
        actorType = JooqActorType.source,
      )
    every { sourceService.getSourceConnection(actorId) } returns
      SourceConnection()
        .withSourceId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(definitionId)
        .withConfiguration(storedConfiguration)
        .withIsDraft(true)
    every { sourceService.getStandardSourceDefinition(definitionId) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, actorId) } returns
      ActorDefinitionVersion().withSpec(specification)
    every { jobInputService.getCheckInput(actorId, null, null, override, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(override),
      )

    val problem =
      assertThrows<BadRequestProblem> {
        service.createCheckCommand(
          commandId = COMMAND_ID,
          actorId = actorId,
          configuration = override,
          workloadPriority = WorkloadPriority.DEFAULT,
          signalInput = null,
          commandInput = Jsons.deserialize("""{"actor_id":"$actorId","config":{"token":"stale-but-valid"}}"""),
        )
      }

    assertEquals(
      "Draft actor checks do not accept configuration overrides. Update the draft, then check it by actor ID.",
      problem.problem.getDetail(),
    )
    verify(exactly = 0) { commandsRepository.save(any()) }
    verify(exactly = 0) { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `draft destination check rejects a configuration override instead of validating stale data`() {
    val actorId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val override = Jsons.deserialize("""{"token":"stale-but-valid"}""")
    val storedConfiguration = Jsons.emptyObject()
    val destinationDefinition = StandardDestinationDefinition().withDestinationDefinitionId(definitionId)
    val specification =
      ConnectorSpecification().withConnectionSpecification(
        Jsons.deserialize("""{"type":"object","required":["token"],"properties":{"token":{"type":"string"}}}"""),
      )

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = definitionId,
        name = "draft destination",
        configuration = storedConfiguration,
        actorType = JooqActorType.destination,
      )
    every { destinationService.getDestinationConnection(actorId) } returns
      DestinationConnection()
        .withDestinationId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(definitionId)
        .withConfiguration(storedConfiguration)
        .withIsDraft(true)
    every { destinationService.getStandardDestinationDefinition(definitionId) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, actorId) } returns
      ActorDefinitionVersion().withSpec(specification)
    every { jobInputService.getCheckInput(actorId, null, null, override, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.DESTINATION)
            .withConnectionConfiguration(override),
      )

    val problem =
      assertThrows<BadRequestProblem> {
        service.createCheckCommand(
          commandId = COMMAND_ID,
          actorId = actorId,
          configuration = override,
          workloadPriority = WorkloadPriority.DEFAULT,
          signalInput = null,
          commandInput = Jsons.deserialize("""{"actor_id":"$actorId","config":{"token":"stale-but-valid"}}"""),
        )
      }

    assertEquals(
      "Draft actor checks do not accept configuration overrides. Update the draft, then check it by actor ID.",
      problem.problem.getDetail(),
    )
    verify(exactly = 0) { commandsRepository.save(any()) }
    verify(exactly = 0) { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `actor ID check fully validates a draft source before creating a workload`() {
    val actorId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val actor =
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = definitionId,
        name = "draft source",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.source,
      )
    val source =
      SourceConnection()
        .withSourceId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(definitionId)
        .withConfiguration(Jsons.emptyObject())
        .withIsDraft(true)
    val sourceDefinition = StandardSourceDefinition().withSourceDefinitionId(definitionId)
    val spec =
      ConnectorSpecification().withConnectionSpecification(
        Jsons.deserialize(
          """{"type":"object","required":["password"],"properties":{"password":{"type":"string","airbyte_secret":true}}}""",
        ),
      )

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { actorRepository.findByActorId(actorId) } returns actor
    every { sourceService.getSourceConnection(actorId) } returns source
    every { sourceService.getStandardSourceDefinition(definitionId) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, actorId) } returns
      ActorDefinitionVersion().withSpec(spec)
    every { jobInputService.getCheckInput(actorId, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(Jsons.emptyObject()),
      )

    assertThrows<JsonValidationException> {
      service.createCheckCommand(
        commandId = COMMAND_ID,
        actorId = actorId,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    }

    verify(exactly = 0) { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `actor ID check fully validates a draft destination before creating a workload`() {
    val actorId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val actor =
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = definitionId,
        name = "draft destination",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.destination,
      )
    val destination =
      DestinationConnection()
        .withDestinationId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(definitionId)
        .withConfiguration(Jsons.emptyObject())
        .withIsDraft(true)
    val destinationDefinition = StandardDestinationDefinition().withDestinationDefinitionId(definitionId)
    val spec =
      ConnectorSpecification().withConnectionSpecification(
        Jsons.deserialize("""{"type":"object","required":["host"],"properties":{"host":{"type":"string"}}}"""),
      )

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { actorRepository.findByActorId(actorId) } returns actor
    every { destinationService.getDestinationConnection(actorId) } returns destination
    every { destinationService.getStandardDestinationDefinition(definitionId) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, actorId) } returns
      ActorDefinitionVersion().withSpec(spec)
    every { jobInputService.getCheckInput(actorId, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.DESTINATION)
            .withConnectionConfiguration(Jsons.emptyObject()),
      )

    assertThrows<JsonValidationException> {
      service.createCheckCommand(
        commandId = COMMAND_ID,
        actorId = actorId,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    }

    verify(exactly = 0) { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `actor ID check validates draft secret references as textual placeholders`() {
    val actorId = UUID.randomUUID()
    val definitionId = UUID.randomUUID()
    val secretReference = Jsons.deserialize("""{"password":{"_secret":"secret-coordinate"}}""")
    val actor =
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = definitionId,
        name = "draft source",
        configuration = secretReference,
        actorType = JooqActorType.source,
      )
    val source =
      SourceConnection()
        .withSourceId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(definitionId)
        .withConfiguration(secretReference)
        .withIsDraft(true)
    val sourceDefinition = StandardSourceDefinition().withSourceDefinitionId(definitionId)
    val workloadInput = slot<String>()
    val spec =
      ConnectorSpecification().withConnectionSpecification(
        Jsons.deserialize(
          """{"type":"object","required":["password"],"properties":{"password":{"type":"string","airbyte_secret":true}}}""",
        ),
      )

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { commandsRepository.save(any()) } returns mockk()
    every { actorRepository.findByActorId(actorId) } returns actor
    every { sourceService.getSourceConnection(actorId) } returns source
    every { sourceService.getStandardSourceDefinition(definitionId) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, actorId) } returns
      ActorDefinitionVersion().withSpec(spec)
    every { jobInputService.getCheckInput(actorId, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(secretReference),
      )
    every {
      workloadService.createWorkload(any(), any(), capture(workloadInput), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } returns
      mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    val created =
      service.createCheckCommand(
        commandId = COMMAND_ID,
        actorId = actorId,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )

    assertTrue(created)
    assertEquals(secretReference, Jsons.deserialize(workloadInput.captured)["checkConnectionInput"]["connectionConfiguration"])
    verify(exactly = 1) { jobInputService.getCheckInput(actorId, null, null) }
  }

  @Test
  fun `successful actor ID check promotes draft after reading sidecar output`() {
    val actorId = UUID.randomUUID()
    val source = SourceConnection().withSourceId(actorId).withIsDraft(true)
    val command =
      defaultCheckCommand.copy(
        commandInput = Jsons.deserialize("""{"actor_id":"$actorId"}"""),
      )
    val connectorOutput =
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withConnectorConfigurationUpdated(true)
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))
    val workload: Workload = mockk { every { status } returns WorkloadStatus.SUCCESS }

    every { commandsRepository.findById(COMMAND_ID) } returns Optional.of(command)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns connectorOutput
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = UUID.randomUUID(),
        name = "draft source",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.source,
      )
    every { sourceService.getSourceConnection(actorId) } returns source

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)

    assertEquals(StandardCheckConnectionOutput.Status.SUCCEEDED, output?.status)
    assertTrue(output?.connectorConfigUpdated == true)
    verifyOrder {
      workloadOutputReader.readConnectorOutput(WORKLOAD_ID)
      sourceService.promoteSourceFromDraft(actorId)
    }
  }

  @Test
  fun `successful destination actor ID check promotes draft after reading sidecar output`() {
    val actorId = UUID.randomUUID()
    val destination = DestinationConnection().withDestinationId(actorId).withIsDraft(true)
    val command =
      defaultCheckCommand.copy(
        commandInput = Jsons.deserialize("""{"actor_id":"$actorId"}"""),
      )
    val connectorOutput =
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withConnectorConfigurationUpdated(true)
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))
    val workload: Workload = mockk { every { status } returns WorkloadStatus.SUCCESS }

    every { commandsRepository.findById(COMMAND_ID) } returns Optional.of(command)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns connectorOutput
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = UUID.randomUUID(),
        name = "draft destination",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.destination,
      )
    every { destinationService.getDestinationConnection(actorId) } returns destination

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)

    assertEquals(StandardCheckConnectionOutput.Status.SUCCEEDED, output?.status)
    assertTrue(output?.connectorConfigUpdated == true)
    verifyOrder {
      workloadOutputReader.readConnectorOutput(WORKLOAD_ID)
      destinationService.promoteDestinationFromDraft(actorId)
    }
  }

  @Test
  fun `failed actor ID check retains destination draft`() {
    val actorId = UUID.randomUUID()
    val command = defaultCheckCommand.copy(commandInput = Jsons.deserialize("""{"actor_id":"$actorId"}"""))
    every { commandsRepository.findById(COMMAND_ID) } returns Optional.of(command)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED))

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)

    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output?.status)
    verify(exactly = 0) { destinationService.promoteDestinationFromDraft(any()) }
    verify(exactly = 0) { sourceService.promoteSourceFromDraft(any()) }
  }

  @ParameterizedTest
  @CsvSource("PENDING", "CLAIMED", "LAUNCHED", "RUNNING", "FAILURE", "CANCELLED")
  fun `source actor ID check does not promote draft before workload succeeds`(workloadStatus: WorkloadStatus) {
    val actorId = UUID.randomUUID()
    val command = defaultCheckCommand.copy(commandInput = Jsons.deserialize("""{"actor_id":"$actorId"}"""))
    val connectorOutput =
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))
    val workload: Workload =
      mockk {
        every { status } returns workloadStatus
        every { terminationSource } returns "source"
        every { terminationReason } returns "workload stopped"
      }

    every { commandsRepository.findById(COMMAND_ID) } returns Optional.of(command)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns connectorOutput
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = UUID.randomUUID(),
        name = "draft source",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.source,
      )
    every { sourceService.getSourceConnection(actorId) } returns SourceConnection().withSourceId(actorId).withIsDraft(true)

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)

    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output?.status)
    verify(exactly = 0) { sourceService.promoteSourceFromDraft(any()) }
  }

  @ParameterizedTest
  @CsvSource("PENDING", "CLAIMED", "LAUNCHED", "RUNNING", "FAILURE", "CANCELLED")
  fun `destination actor ID check does not promote draft before workload succeeds`(workloadStatus: WorkloadStatus) {
    val actorId = UUID.randomUUID()
    val command = defaultCheckCommand.copy(commandInput = Jsons.deserialize("""{"actor_id":"$actorId"}"""))
    val connectorOutput =
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))
    val workload: Workload =
      mockk {
        every { status } returns workloadStatus
        every { terminationSource } returns "destination"
        every { terminationReason } returns "workload stopped"
      }

    every { commandsRepository.findById(COMMAND_ID) } returns Optional.of(command)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns connectorOutput
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = UUID.randomUUID(),
        name = "draft destination",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.destination,
      )
    every { destinationService.getDestinationConnection(actorId) } returns DestinationConnection().withDestinationId(actorId).withIsDraft(true)

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)

    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output?.status)
    verify(exactly = 0) { destinationService.promoteDestinationFromDraft(any()) }
  }

  @ParameterizedTest
  @CsvSource("FAILURE", "CANCELLED")
  fun `unsuccessful workload overrides stored successful check output`(workloadStatus: WorkloadStatus) {
    val connectorOutput =
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withConnectorConfigurationUpdated(true)
        .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))
    val workload: Workload =
      mockk {
        every { status } returns workloadStatus
        every { terminationSource } returns "source"
        every { terminationReason } returns "workload stopped"
      }

    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns connectorOutput
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)

    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output?.status)
    assertFalse(output?.connectorConfigUpdated == true)
    assertNotNull(output?.failureReason)
    verify(exactly = 0) { destinationService.promoteDestinationFromDraft(any()) }
    verify(exactly = 0) { sourceService.promoteSourceFromDraft(any()) }
  }

  @Test
  fun `discover command rejects draft sources and destinations`() {
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    every { commandsRepository.existsById(any()) } returns false
    every { actorRepository.findByActorId(sourceId) } returns
      Actor(
        id = sourceId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = UUID.randomUUID(),
        name = "draft source",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.source,
      )
    every { actorRepository.findByActorId(destinationId) } returns
      Actor(
        id = destinationId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = UUID.randomUUID(),
        name = "draft destination",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.destination,
      )
    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withSourceId(sourceId).withIsDraft(true)
    every { destinationService.getDestinationConnection(destinationId) } returns
      DestinationConnection().withDestinationId(destinationId).withIsDraft(true)

    assertThrows<ActorNotReadyProblem> {
      service.createDiscoverCommand(
        commandId = "source-discover",
        actorId = sourceId,
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    }
    assertThrows<ActorNotReadyProblem> {
      service.createDiscoverCommand(
        commandId = "destination-discover",
        actorId = destinationId,
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    }

    verify(exactly = 0) { jobInputService.getDiscoverInput(any(), any(), any()) }
  }

  @Test
  fun `replicate command rejects a draft actor`() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getReplicationInput(connectionId, null, null, 123L, 0L) } returns
      ReplicationActivityInput(
        jobRunConfig = JobRunConfig().withJobId("123").withAttemptId(0L),
        connectionContext =
          ConnectionContext()
            .withConnectionId(connectionId)
            .withWorkspaceId(WORKSPACE_ID)
            .withSourceId(sourceId)
            .withDestinationId(destinationId),
      )
    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withSourceId(sourceId).withIsDraft(true)
    every { destinationService.getDestinationConnection(destinationId) } returns
      DestinationConnection().withDestinationId(destinationId).withIsDraft(false)

    assertThrows<ActorNotReadyProblem> {
      service.createReplicateCommand(
        commandId = COMMAND_ID,
        connectionId = connectionId,
        jobId = "123",
        attemptNumber = 0,
        appliedCatalogDiff = null,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    }

    verify(exactly = 0) { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }

  @Test
  fun `creating a discover command successfully saves the command and enqueues the workload`() {
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    val workloadInput = slot<String>()
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getDiscoverInput(any(), any(), any()) } returns
      DiscoverCommandInput.DiscoverCatalogInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        integrationLauncherConfig = IntegrationLauncherConfig(),
        discoverCatalogInput = StandardDiscoverCatalogInput(),
      )
    every {
      workloadService.createWorkload(any(), any(), capture(workloadInput), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } returns
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

    // Ensuring this is added because it impacts nodepool selection in the launcher
    val actualInput = Jsons.deserialize(workloadInput.captured)
    assertEquals(WorkloadPriority.DEFAULT.toString(), actualInput["launcherConfig"]["priority"].asText())
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
    val connectorOutput =
      ConnectorJobOutput()
        .withOutputType(OutputType.CHECK_CONNECTION)
        .withConnectorConfigurationUpdated(true)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED)
            .withMessage("Success"),
        )
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns connectorOutput
    every { workloadService.getWorkload(WORKLOAD_ID) } returns mockk { every { status } returns WorkloadStatus.SUCCESS }

    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)
    val expectedOutput =
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.SUCCEEDED,
        connectorConfigUpdated = true,
        message = "Success",
        failureReason = null,
        logs = null,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getConnectorJobOutput does nothing for unknown command ids`() {
    val output = service.getCheckJobOutput(UNKNOWN_COMMAND_ID, withLogs = false)
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
    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID, withLogs = false)
    val expectedOutput =
      CommandService.DiscoverJobOutput(
        catalogId = discoverCatalogId,
        catalog = discoverCatalog,
        destinationCatalog = null,
        failureReason = null,
        logs = null,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getDiscoverJobOutput populates catalog field for SOURCE catalog type`() {
    val discoverCatalogId = UUID.randomUUID()
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.DISCOVER_CATALOG_ID).withDiscoverCatalogId(discoverCatalogId)
    every { workloadOutputReader.readConnectorOutput(DISCOVER_WORKLOAD_ID) } returns workloadOutput
    val sourceCatalog =
      ActorCatalog()
        .withCatalogType(ActorCatalog.CatalogType.SOURCE_CATALOG)
        .withCatalog(
          Jsons.jsonNode(
            AirbyteCatalog().withStreams(
              listOf(AirbyteStream().withName("source_stream")),
            ),
          ),
        )
    every { catalogService.getActorCatalogById(discoverCatalogId) } returns sourceCatalog

    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID, withLogs = false)

    assertEquals(discoverCatalogId, output?.catalogId)
    assertEquals(sourceCatalog, output?.catalog)
    assertNull(output?.destinationCatalog)
    assertNull(output?.failureReason)
  }

  @Test
  fun `getDiscoverJobOutput populates destinationCatalog field for DESTINATION catalog type`() {
    val discoverCatalogId = UUID.randomUUID()
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.DISCOVER_CATALOG_ID).withDiscoverCatalogId(discoverCatalogId)
    every { workloadOutputReader.readConnectorOutput(DISCOVER_WORKLOAD_ID) } returns workloadOutput
    val destinationCatalog =
      ActorCatalog()
        .withCatalogType(ActorCatalog.CatalogType.DESTINATION_CATALOG)
        .withCatalog(
          Jsons.jsonNode(
            DestinationCatalog().withOperations(
              listOf(
                DestinationOperation()
                  .withObjectName("destination_table")
                  .withSyncMode(DestinationSyncMode.APPEND),
              ),
            ),
          ),
        )
    every { catalogService.getActorCatalogById(discoverCatalogId) } returns destinationCatalog

    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID, withLogs = false)

    assertEquals(discoverCatalogId, output?.catalogId)
    assertNull(output?.catalog)
    assertEquals(destinationCatalog, output?.destinationCatalog)
    assertNull(output?.failureReason)
  }

  @Test
  fun `getDiscoverJobOutput defaults to catalog field when catalogType is null`() {
    val discoverCatalogId = UUID.randomUUID()
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.DISCOVER_CATALOG_ID).withDiscoverCatalogId(discoverCatalogId)
    every { workloadOutputReader.readConnectorOutput(DISCOVER_WORKLOAD_ID) } returns workloadOutput
    val catalogWithoutType =
      ActorCatalog()
        .withCatalogType(null)
        .withCatalog(
          Jsons.jsonNode(
            AirbyteCatalog().withStreams(
              listOf(AirbyteStream().withName("default_stream")),
            ),
          ),
        )
    every { catalogService.getActorCatalogById(discoverCatalogId) } returns catalogWithoutType

    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID, withLogs = false)

    assertEquals(discoverCatalogId, output?.catalogId)
    assertEquals(catalogWithoutType, output?.catalog)
    assertNull(output?.destinationCatalog)
    assertNull(output?.failureReason)
  }

  @Test
  fun `getDiscoverJobOutput returns failure without crashing`() {
    val failure = FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withFailureType(FailureReason.FailureType.CONFIG_ERROR)
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.DISCOVER_CATALOG_ID).withFailureReason(failure)
    every { workloadOutputReader.readConnectorOutput(DISCOVER_WORKLOAD_ID) } returns workloadOutput
    val output = service.getDiscoverJobOutput(DISCOVER_COMMAND_ID, withLogs = false)
    val expectedOutput =
      CommandService.DiscoverJobOutput(
        catalogId = null,
        catalog = null,
        destinationCatalog = null,
        failureReason = failure,
        logs = null,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getSpecJobOutput returns spec when successful`() {
    val spec =
      ConnectorSpecification()
        .withProtocolVersion("0.2.0")
        .withConnectionSpecification(Jsons.jsonNode(mapOf("type" to "object")))
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.SPEC).withSpec(spec)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns workloadOutput
    val output = service.getSpecJobOutput(COMMAND_ID, withLogs = false)
    val expectedOutput =
      CommandService.SpecJobOutput(
        spec = spec,
        failureReason = null,
        logs = null,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getSpecJobOutput returns failure without crashing`() {
    val failure = FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
    val workloadOutput = ConnectorJobOutput().withOutputType(OutputType.SPEC).withFailureReason(failure)
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns workloadOutput
    val output = service.getSpecJobOutput(COMMAND_ID, withLogs = false)
    val expectedOutput =
      CommandService.SpecJobOutput(
        spec = null,
        failureReason = failure,
        logs = null,
      )
    assertEquals(expectedOutput, output)
  }

  @Test
  fun `getSpecJobOutput returns null for unknown command ids`() {
    val output = service.getSpecJobOutput(UNKNOWN_COMMAND_ID, withLogs = false)
    verify(exactly = 0) { workloadOutputReader.readConnectorOutput(any()) }
    assertNull(output)
  }

  @Test
  fun `getReplicationOutput returns an output (happy path)`() {
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

  @ParameterizedTest
  @CsvSource("FAILURE,FAILED", "CANCELLED,CANCELLED", "SUCCESS,COMPLETED")
  fun `getReplicationOutput returns an output derived from the workload when the output is not found`(
    workloadStatus: WorkloadStatus,
    expectedStatus: StandardSyncSummary.ReplicationStatus,
  ) {
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    val workload: Workload =
      mockk {
        every { status } returns workloadStatus
        every { terminationSource } returns "platform"
        every { terminationReason } returns "probably the workload monitor"
      }
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload
    every { workloadOutputReader.readSyncOutput(WORKLOAD_ID) } throws Exception("bang")
    val output = service.getReplicationOutput(COMMAND_ID)
    assertEquals(expectedStatus, output!!.replicationAttemptSummary.status)
    assertNotNull(output.failures.first().timestamp)
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
    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)
    assertNotNull(output?.failureReason)
    assertNotNull(output?.failureReason?.timestamp)
  }

  @Test
  fun `verify we return failure reason when we read an empty response from the doc store`() {
    every { workloadOutputReader.readConnectorOutput(WORKLOAD_ID) } returns null
    val output = service.getCheckJobOutput(COMMAND_ID, withLogs = false)
    assertNotNull(output?.failureReason)
    assertNotNull(output?.failureReason?.timestamp)
  }

  @Test
  fun `createCheckCommand passes null jobId and attemptNumber to JobInputService when not provided`() {
    val actorId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { actorRepository.findByActorId(actorId) } returns
      Actor(
        id = actorId,
        workspaceId = WORKSPACE_ID,
        actorDefinitionId = actorDefinitionId,
        name = "ready source",
        configuration = Jsons.emptyObject(),
        actorType = JooqActorType.source,
      )
    every { sourceService.getSourceConnection(actorId) } returns
      SourceConnection()
        .withSourceId(actorId)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(actorDefinitionId)
        .withConfiguration(Jsons.emptyObject())
        .withIsDraft(false)
    every { jobInputService.getCheckInput(actorId, null, null) } returns
      CheckConnectionInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        launcherConfig = IntegrationLauncherConfig(),
        checkConnectionInput =
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(Jsons.emptyObject()),
      )
    every { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns
      mockk()
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    service.createCheckCommand(
      commandId = COMMAND_ID,
      actorId = actorId,
      jobId = null,
      attemptNumber = null,
      workloadPriority = WorkloadPriority.DEFAULT,
      signalInput = null,
      commandInput = Jsons.emptyObject(),
    )

    verify { jobInputService.getCheckInput(actorId, null, null) }
  }

  @Test
  fun `createDiscoverCommand passes null jobId and attemptNumber to JobInputService when not provided`() {
    val actorId = UUID.randomUUID()

    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getDiscoverInput(actorId, null, null) } returns
      DiscoverCommandInput.DiscoverCatalogInput(
        jobRunConfig = JobRunConfig().withJobId(UUID.randomUUID().toString()).withAttemptId(0L),
        integrationLauncherConfig = IntegrationLauncherConfig(),
        discoverCatalogInput = StandardDiscoverCatalogInput(),
      )
    every { workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns
      mockk()
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    service.createDiscoverCommand(
      commandId = COMMAND_ID,
      actorId = actorId,
      jobId = null,
      attemptNumber = null,
      workloadPriority = WorkloadPriority.DEFAULT,
      signalInput = null,
      commandInput = Jsons.emptyObject(),
    )

    verify { jobInputService.getDiscoverInput(actorId, null, null) }
  }

  @Test
  fun `getJobLogs returns empty logs when command not found`() {
    val logs = service.getJobLogs(UNKNOWN_COMMAND_ID)
    assertTrue(logs.isStructured())
    assertNull(logs.logEvents)
    assertNull(logs.logLines)
  }

  @Test
  fun `getJobLogs returns empty logs when workload not found`() {
    every { workloadService.getWorkload(WORKLOAD_ID) } throws Exception("Workload not found")
    val logs = service.getJobLogs(COMMAND_ID)
    assertTrue(logs.isStructured())
    assertNull(logs.logEvents)
    assertNull(logs.logLines)
  }

  @Test
  fun `getJobLogs returns structured logs when log events are available`() {
    val logPath = "/test/log/path"
    val workload: Workload = mockk { every { this@mockk.logPath } returns logPath }
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload

    val logEvents =
      LogEvents(
        events =
          listOf(
            io.airbyte.commons.logging.LogEvent(
              timestamp = 1234567890L,
              message = "Test log message",
              level = "INFO",
              logSource = io.airbyte.commons.logging.LogSource.PLATFORM,
            ),
          ),
      )
    every { logClientManager.getLogs(Path.of(logPath)) } returns logEvents

    val logs = service.getJobLogs(COMMAND_ID)
    assertTrue(logs.isStructured())
    assertNotNull(logs.logEvents)
    assertEquals(1, logs.logEvents?.events?.size)
    assertEquals(
      "Test log message",
      logs.logEvents
        ?.events
        ?.first()
        ?.message,
    )
  }

  @Test
  fun `getJobLogs returns formatted logs when structured logs are empty`() {
    val logPath = "/test/log/path"
    val workload: Workload = mockk { every { this@mockk.logPath } returns logPath }
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload

    val emptyLogEvents = LogEvents(events = emptyList())
    val logLines = listOf("line 1", "line 2", "line 3")
    every { logClientManager.getLogs(Path.of(logPath)) } returns emptyLogEvents
    every { logClientManager.getJobLogFile(Path.of(logPath)) } returns logLines

    val logs = service.getJobLogs(COMMAND_ID)
    assertFalse(logs.isStructured())
    assertNotNull(logs.logLines)
    assertEquals(3, logs.logLines?.size)
    assertEquals("line 1", logs.logLines?.first())
  }

  @Test
  fun `getJobLogs returns empty logs when log retrieval throws exception`() {
    val logPath = "/test/log/path"
    val workload: Workload = mockk { every { this@mockk.logPath } returns logPath }
    every { workloadService.getWorkload(WORKLOAD_ID) } returns workload

    every { logClientManager.getLogs(Path.of(logPath)) } throws Exception("Log retrieval failed")

    val logs = service.getJobLogs(COMMAND_ID)
    assertTrue(logs.isStructured())
    assertNull(logs.logEvents)
    assertNull(logs.logLines)
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

  @Test
  fun `createSpec with dockerImage returns false if command exists`() {
    every { commandsRepository.existsById(COMMAND_ID) } returns true
    val output =
      service.createSpecCommand(
        commandId = COMMAND_ID,
        dockerImage = "airbyte/source-test",
        dockerImageTag = "1.0.0",
        workspaceId = WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertFalse(output)
  }

  @Test
  fun `createSpec with actorDefinitionId returns false if command exists`() {
    every { commandsRepository.existsById(COMMAND_ID) } returns true
    val output =
      service.createSpecCommand(
        commandId = COMMAND_ID,
        actorDefinitionId = UUID.randomUUID(),
        dockerImageTag = "1.0.0",
        workspaceId = WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertFalse(output)
  }

  @Test
  fun `creating a spec command with dockerImage successfully saves the command and enqueues the workload`() {
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    val workloadInput = slot<String>()
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getSpecInput(any<String>(), any<String>(), any(), any(), any(), any()) } returns
      io.airbyte.workers.models.SpecInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        launcherConfig = IntegrationLauncherConfig(),
      )
    every {
      workloadService.createWorkload(any(), any(), capture(workloadInput), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } returns
      mockk()
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    val output =
      service.createSpecCommand(
        commandId = COMMAND_ID,
        dockerImage = "airbyte/source-test",
        dockerImageTag = "1.0.0",
        workspaceId = WORKSPACE_ID,
        signalInput = "test-signal",
        commandInput = Jsons.emptyObject(),
      )
    assertTrue(output)

    verify { commandsRepository.save(any()) }
    verify { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }

    // Ensuring this is added because it impacts nodepool selection in the launcher
    val actualInput = Jsons.deserialize(workloadInput.captured)
    assertEquals(WorkloadPriority.HIGH.toString(), actualInput["launcherConfig"]["priority"].asText())
  }

  @Test
  fun `creating a spec command with actorDefinitionId successfully saves the command and enqueues the workload`() {
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    val actorDefinitionId = UUID.randomUUID()
    val workloadInput = slot<String>()
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getSpecInput(any<UUID>(), any<String>(), any(), any(), any()) } returns
      SpecInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        launcherConfig = IntegrationLauncherConfig(),
      )
    every {
      workloadService.createWorkload(any(), any(), capture(workloadInput), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } returns
      mockk()
    every { commandsRepository.save(any()) } returns mockk()
    every { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) } returns Unit

    val output =
      service.createSpecCommand(
        commandId = COMMAND_ID,
        actorDefinitionId = actorDefinitionId,
        dockerImageTag = "2.0.0",
        workspaceId = WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertTrue(output)

    verify { commandsRepository.save(any()) }
    verify { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }

    // Ensuring HIGH priority is set
    val actualInput = Jsons.deserialize(workloadInput.captured)
    assertEquals(WorkloadPriority.HIGH.toString(), actualInput["launcherConfig"]["priority"].asText())
  }

  @Test
  fun `creating a spec command handles ConflictException when workload already exists`() {
    val jobId = UUID.randomUUID().toString()
    val attemptNumber = 0L
    every { commandsRepository.existsById(COMMAND_ID) } returns false
    every { jobInputService.getSpecInput(any<String>(), any<String>(), any(), any(), any(), any()) } returns
      SpecInput(
        jobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptNumber),
        launcherConfig = IntegrationLauncherConfig(),
      )
    every {
      workloadService.createWorkload(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    } throws ConflictException("Workload already exists")
    every { commandsRepository.save(any()) } returns mockk()

    val output =
      service.createSpecCommand(
        commandId = COMMAND_ID,
        dockerImage = "airbyte/source-test",
        dockerImageTag = "1.0.0",
        workspaceId = WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.emptyObject(),
      )
    assertTrue(output)

    // Command should still be saved
    verify { commandsRepository.save(any()) }
    // But workload queue should not be called
    verify(exactly = 0) { workloadQueueService.create(any(), any(), any(), any(), any(), any(), any(), any(), any()) }
  }
}
