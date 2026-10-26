/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.problems.throwable.generated.ActorNotReadyProblem
import io.airbyte.api.server.generated.models.AirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.AirbyteStreamConfiguration
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionReadList
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionStream
import io.airbyte.api.server.generated.models.ConnectionStreamRequestBody
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.DestinationCoreConfig
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.DestinationSyncMode
import io.airbyte.api.server.generated.models.DestinationUpdate
import io.airbyte.api.server.generated.models.FailureOrigin
import io.airbyte.api.server.generated.models.FailureType
import io.airbyte.api.server.generated.models.FieldAdd
import io.airbyte.api.server.generated.models.FieldTransform
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobCreate
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.LogFormatType
import io.airbyte.api.server.generated.models.NonBreakingChangesPreference
import io.airbyte.api.server.generated.models.SourceAutoPropagateChange
import io.airbyte.api.server.generated.models.SourceCoreConfig
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRequestBody
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceUpdate
import io.airbyte.api.server.generated.models.StreamTransform
import io.airbyte.api.server.generated.models.StreamTransformUpdateStream
import io.airbyte.api.server.generated.models.SyncMode
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.converters.AttemptLogs
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper
import io.airbyte.commons.server.helpers.DestinationHelpers
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.commons.server.helpers.SourceHelpers
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata.Companion.fromJobMetadata
import io.airbyte.commons.server.scheduler.SynchronousResponse
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.commons.temporal.ErrorCode
import io.airbyte.commons.temporal.JobMetadata
import io.airbyte.commons.temporal.ManualOperationResult
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.DestinationConnection
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.processConfigSecrets
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType
import io.airbyte.domain.services.dataworker.DataWorkerUsageService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.featureflag.EnforceDataWorkerCapacity
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobCreator
import io.airbyte.persistence.job.JobNotifier
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.factory.SyncJobFactory
import io.airbyte.persistence.job.tracker.JobTracker
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import io.airbyte.validation.json.JsonSchemaValidator
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.mockk.verifyOrder
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.net.URI
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

internal class SchedulerHandlerTest {
  private lateinit var schedulerHandler: SchedulerHandler
  private lateinit var actorDefinitionService: ActorDefinitionService
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var job: Job
  private lateinit var synchronousSchedulerClient: SynchronousSchedulerClient
  private lateinit var jobResponse: SynchronousResponse<*>
  private lateinit var configurationUpdate: ConfigurationUpdate
  private lateinit var jsonSchemaValidator: JsonSchemaValidator
  private lateinit var jobPersistence: JobPersistence
  private lateinit var eventRunner: EventRunner
  private lateinit var jobConverter: JobConverter
  private lateinit var connectionsHandler: ConnectionsHandler
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var streamResetPersistence: StreamResetPersistence
  private lateinit var oAuthConfigSupplier: OAuthConfigSupplier
  private lateinit var jobCreator: JobCreator
  private lateinit var jobFactory: SyncJobFactory
  private lateinit var jobNotifier: JobNotifier
  private lateinit var jobTracker: JobTracker
  private lateinit var workspaceService: WorkspaceService
  private lateinit var secretPersistenceService: SecretPersistenceService
  private lateinit var streamRefreshesHandler: StreamRefreshesHandler
  private lateinit var connectionTimelineEventHelper: ConnectionTimelineEventHelper
  private lateinit var logClientManager: LogClientManager
  private lateinit var logUtils: LogUtils
  private lateinit var catalogService: CatalogService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var connectionService: ConnectionService
  private lateinit var operationService: OperationService
  private val catalogConverter = CatalogConverter(FieldGenerator(), mutableListOf())
  private lateinit var metricClient: MetricClient
  private lateinit var secretStorageService: SecretStorageService
  private lateinit var secretSanitizer: SecretSanitizer
  private lateinit var dataWorkerUsageService: DataWorkerUsageService

  @BeforeEach
  fun setup() {
    job =
      Job(
        JOB_ID,
        ConfigType.SYNC,
        SCOPE.toString(),
        JobConfig()
          .withConfigType(ConfigType.SYNC)
          .withSync(
            JobSyncConfig()
              .withSourceDefinitionVersionId(UUID.randomUUID())
              .withDestinationDefinitionVersionId(UUID.randomUUID()),
          ),
        emptyList(),
        JobStatus.SUCCEEDED,
        1001L,
        1000L,
        1002L,
        true,
      )

    val synchronousJobMetadata =
      SynchronousJobMetadata(
        SCOPE,
        ConfigType.SYNC,
        null,
        CREATED_AT,
        CREATED_AT,
        false,
        CONNECTOR_CONFIG_UPDATED,
        LOG_PATH,
        null,
      )

    jobResponse = SynchronousResponse(null, synchronousJobMetadata)

    configurationUpdate = mockk(relaxUnitFun = true)
    jsonSchemaValidator = mockk(relaxUnitFun = true)
    synchronousSchedulerClient = mockk(relaxUnitFun = true)
    actorDefinitionService = mockk(relaxUnitFun = true)
    catalogService = mockk(relaxUnitFun = true)
    connectionService = mockk(relaxUnitFun = true)
    destinationService = mockk(relaxUnitFun = true)
    sourceService = mockk(relaxUnitFun = true)
    operationService = mockk(relaxUnitFun = true)

    every { connectionService.getStandardSync(any()) } returns StandardSync().withStatus(StandardSync.Status.ACTIVE)
    every { destinationService.getStandardDestinationDefinition(any()) } returns SOME_DESTINATION_DEFINITION
    every { destinationService.getDestinationDefinitionFromConnection(any()) } returns SOME_DESTINATION_DEFINITION

    secretsRepositoryWriter = mockk(relaxUnitFun = true)
    jobPersistence = mockk(relaxUnitFun = true)
    eventRunner = mockk(relaxUnitFun = true)
    connectionsHandler = mockk(relaxUnitFun = true)
    actorDefinitionVersionHelper = mockk(relaxUnitFun = true)
    every { actorDefinitionVersionHelper.getDestinationVersion(any(), any()) } returns SOME_ACTOR_DEFINITION

    streamResetPersistence = mockk(relaxUnitFun = true)
    oAuthConfigSupplier = mockk(relaxUnitFun = true)
    jobCreator = mockk(relaxUnitFun = true)
    jobFactory = mockk(relaxUnitFun = true)
    jobNotifier = mockk(relaxUnitFun = true)
    jobTracker = mockk(relaxUnitFun = true)
    logClientManager = mockk(relaxUnitFun = true)
    logUtils = mockk(relaxUnitFun = true)

    every { logClientManager.getLogs(any()) } returns LogEvents(emptyList(), "1")
    jobConverter = spyk(JobConverter(logClientManager, logUtils))

    featureFlagClient = mockk<TestClient>(relaxUnitFun = true)
    workspaceService = mockk(relaxUnitFun = true)
    secretPersistenceService = mockk(relaxUnitFun = true)
    metricClient = mockk(relaxUnitFun = true)
    secretStorageService = mockk(relaxUnitFun = true)

    streamRefreshesHandler = mockk(relaxUnitFun = true)
    every { streamRefreshesHandler.getRefreshesForConnection(any()) } returns ArrayList()

    connectionTimelineEventHelper = mockk(relaxUnitFun = true)

    secretSanitizer =
      SecretSanitizer(
        actorDefinitionVersionHelper,
        destinationService,
        sourceService,
        secretPersistenceService,
        secretsRepositoryWriter,
        secretStorageService,
      )

    dataWorkerUsageService = mockk(relaxUnitFun = true)

    every { jobPersistence.getJob(any()) } returns job
    every { jobPersistence.listJobsForConnectionWithStatuses(any(), any(), any()) } returns emptyList()
    every { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) } returns mockk()
    every { logClientManager.getJobLogFile(any()) } returns emptyList()
    every { catalogService.getActorCatalog(any(), any(), any()) } returns Optional.empty()
    every { secretStorageService.getByWorkspaceId(any()) } returns null
    every { secretPersistenceService.getPersistenceFromWorkspaceId(any()) } returns mockk()
    every { streamResetPersistence.getStreamResets(any()) } returns emptyList()
    every { actorDefinitionService.getActorDefinitionVersions(any()) } returns emptyList()
    every { oAuthConfigSupplier.injectDestinationOAuthParameters(any(), any(), any(), any()) } answers { lastArg() }

    schedulerHandler =
      SchedulerHandler(
        actorDefinitionService,
        catalogService,
        connectionService,
        synchronousSchedulerClient,
        configurationUpdate,
        jsonSchemaValidator,
        jobPersistence,
        eventRunner,
        jobConverter,
        connectionsHandler,
        actorDefinitionVersionHelper,
        featureFlagClient,
        streamResetPersistence,
        oAuthConfigSupplier,
        jobCreator,
        jobFactory,
        jobNotifier,
        jobTracker,
        workspaceService,
        streamRefreshesHandler,
        connectionTimelineEventHelper,
        sourceService,
        destinationService,
        catalogConverter,
        metricClient,
        secretSanitizer,
        dataWorkerUsageService,
      )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @DisplayName("Test job creation")
  fun createJob(isScheduled: Boolean) {
    every { jobFactory.createSync(CONNECTION_ID, isScheduled) } returns JOB_ID
    every { connectionService.getStandardSync(CONNECTION_ID) } returns StandardSync()
    every { jobPersistence.getJob(JOB_ID) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(JOB_ID)

    val output = schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = isScheduled))

    verify { jobFactory.createSync(CONNECTION_ID, isScheduled) }
    Assertions.assertThat(output.job.id).isEqualTo(JOB_ID)
  }

  private fun stubSyncJobCreation() {
    every { jobFactory.createSync(CONNECTION_ID, true) } returns JOB_ID
    every { connectionService.getStandardSync(CONNECTION_ID) } returns StandardSync()
    every { jobPersistence.getJob(JOB_ID) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(JOB_ID)
  }

  private fun stubWorkspaceForConnection(organizationId: UUID?) {
    every { workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, false) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withOrganizationId(organizationId)
  }

  @Test
  @DisplayName("Passive usage tracking records usage when capacity enforcement is disabled")
  fun createJobTracksUsagePassivelyWhenEnforcementDisabled() {
    val organizationId = UUID.randomUUID()
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId)
    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } returns false

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify { dataWorkerUsageService.insertUsageForCreatedJob(job) }
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when capacity enforcement is enabled")
  fun createJobSkipsPassiveUsageTrackingWhenEnforcementEnabled() {
    val organizationId = UUID.randomUUID()
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId)
    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } returns true

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(exactly = 0) { dataWorkerUsageService.insertUsageForCreatedJob(any()) }
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when the enforcement flag lookup fails")
  fun createJobSkipsPassiveUsageTrackingWhenFlagLookupFails() {
    val organizationId = UUID.randomUUID()
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId)
    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } throws RuntimeException("flag lookup failed")

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(exactly = 0) { dataWorkerUsageService.insertUsageForCreatedJob(any()) }
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when the workspace lookup fails")
  fun createJobSkipsPassiveUsageTrackingWhenWorkspaceLookupFails() {
    stubSyncJobCreation()
    every { workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, false) } throws RuntimeException("workspace lookup failed")

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(exactly = 0) { dataWorkerUsageService.insertUsageForCreatedJob(any()) }
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when the workspace has no organization")
  fun createJobSkipsPassiveUsageTrackingWhenWorkspaceHasNoOrganization() {
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId = null)

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(exactly = 0) { dataWorkerUsageService.insertUsageForCreatedJob(any()) }
    verify { featureFlagClient wasNot Called }
  }

  @Test
  @DisplayName("Test refresh job creation")
  fun createRefreshJob() {
    every {
      jobFactory.createRefresh(
        CONNECTION_ID,
        any(),
      )
    } returns JOB_ID
    every { connectionService.getStandardSync(CONNECTION_ID) } returns StandardSync()
    every { jobPersistence.getJob(JOB_ID) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(JOB_ID)
    every { streamRefreshesHandler.getRefreshesForConnection(CONNECTION_ID) } returns
      listOf(
        StreamRefresh(UUID.randomUUID(), CONNECTION_ID, "name", "namespace", null, RefreshType.TRUNCATE),
      )

    val output = schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify { jobFactory.createRefresh(CONNECTION_ID, any()) }
    Assertions.assertThat(output.job.id).isEqualTo(JOB_ID)
  }

  @Test
  @DisplayName("Test reset job creation")
  fun createResetJob() {
    every { operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID) } returns WEBHOOK_OPERATION
    val standardSync =
      StandardSync().withDestinationId(DESTINATION_ID).withOperationIds(listOf(WEBHOOK_OPERATION_ID)).withSourceId(SOURCE_ID)
    every { connectionService.getStandardSync(CONNECTION_ID) } returns standardSync
    val source =
      SourceConnection()
        .withSourceId(SOURCE_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
    every { sourceService.getSourceConnection(SOURCE_ID) } returns source
    val destination =
      DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withConfiguration(emptyObject())
    every { destinationService.getDestinationConnection(DESTINATION_ID) } returns destination
    val destinationDefinition = StandardDestinationDefinition()
    val destinationVersion = Version(DESTINATION_PROTOCOL_VERSION)
    val actorDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withProtocolVersion(destinationVersion.serialize())
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      )
    } returns actorDefinitionVersion
    every { destinationService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID) } returns destinationDefinition
    val streamsToReset = listOf(STREAM_DESCRIPTOR1, STREAM_DESCRIPTOR2)
    every { streamResetPersistence.getStreamResets(CONNECTION_ID) } returns streamsToReset

    every {
      jobCreator.createResetConnectionJob(
        destination,
        standardSync,
        destinationDefinition,
        actorDefinitionVersion,
        DOCKER_IMAGE_NAME,
        destinationVersion,
        false,
        mutableListOf(),
        streamsToReset,
        WORKSPACE_ID,
      )
    } returns Optional.of<Long>(JOB_ID)

    every { jobPersistence.getJob(JOB_ID) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(JOB_ID)

    val output = schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify { oAuthConfigSupplier.injectDestinationOAuthParameters(any(), any(), any(), any()) }

    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID) }

    Assertions.assertThat(output.job.id).isEqualTo(JOB_ID)
  }

  @Test
  fun testCheckSourceConnectionFromSourceId() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceIdRequestBody(sourceId = source.sourceId)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every {
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        null,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>

    schedulerHandler.checkSourceConnectionFromSourceId(request)

    verify { sourceService.getSourceConnection(source.sourceId) }
    verify { synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) }
  }

  @Test
  fun `successful actor ID checks validate and promote source and destination drafts after connector jobs`() {
    val source = SourceHelpers.generateSource(UUID.randomUUID()).withIsDraft(true)
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID()).withIsDraft(true)
    val sourceRequest = SourceIdRequestBody(sourceId = source.sourceId)
    val destinationRequest = DestinationIdRequestBody(destinationId = destination.destinationId)
    val sourceDefinition = StandardSourceDefinition().withSourceDefinitionId(source.sourceDefinitionId)
    val destinationDefinition = StandardDestinationDefinition().withDestinationDefinitionId(destination.destinationDefinitionId)
    val sourceVersion = ActorDefinitionVersion().withSpec(CONNECTOR_SPECIFICATION)
    val destinationVersion = ActorDefinitionVersion().withSpec(CONNECTOR_SPECIFICATION)
    val succeeded = StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED)
    val sourceResponse = SynchronousResponse(succeeded, SynchronousJobMetadata.mock(ConfigType.CHECK_CONNECTION_SOURCE))
    val destinationResponse = SynchronousResponse(succeeded, SynchronousJobMetadata.mock(ConfigType.CHECK_CONNECTION_DESTINATION))

    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) } returns sourceVersion
    every { synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null) } returns sourceResponse
    every { jobConverter.getSynchronousJobRead(sourceResponse) } returns emptySynchronousJobRead()
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination
    every { destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId) } returns
      destinationVersion
    every { synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null) } returns destinationResponse
    every { jobConverter.getSynchronousJobRead(destinationResponse) } returns emptySynchronousJobRead()

    schedulerHandler.checkSourceConnectionFromSourceId(sourceRequest)
    schedulerHandler.checkDestinationConnectionFromDestinationId(destinationRequest)

    verify(exactly = 2) { jsonSchemaValidator.ensure(CONNECTOR_SPECIFICATION.connectionSpecification, source.configuration) }
    verifyOrder {
      synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null)
      sourceService.promoteSourceFromDraft(source.sourceId)
    }
    verifyOrder {
      synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null)
      destinationService.promoteDestinationFromDraft(destination.destinationId)
    }
  }

  @Test
  fun `draft actor ID checks validate persisted secret references as textual placeholders`() {
    val secretSpec =
      ConnectorSpecification().withConnectionSpecification(
        jsonNode(
          mapOf(
            "type" to "object",
            "required" to listOf("password"),
            "properties" to
              mapOf(
                "password" to mapOf("type" to "string", "airbyte_secret" to true),
              ),
          ),
        ),
      )
    val persistedConfig = jsonNode(mapOf("password" to mapOf("_secret" to "airbyte_workspace_secret_v1")))
    val validationConfig = jsonNode(mapOf("password" to "secret_placeholder"))
    val source = SourceHelpers.generateSource(UUID.randomUUID()).withConfiguration(persistedConfig).withIsDraft(true)
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID()).withConfiguration(persistedConfig).withIsDraft(true)
    val sourceDefinition = StandardSourceDefinition().withSourceDefinitionId(source.sourceDefinitionId)
    val destinationDefinition = StandardDestinationDefinition().withDestinationDefinitionId(destination.destinationDefinitionId)
    val sourceVersion = ActorDefinitionVersion().withSpec(secretSpec)
    val destinationVersion = ActorDefinitionVersion().withSpec(secretSpec)
    val succeeded = StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED)
    val sourceResponse = SynchronousResponse(succeeded, SynchronousJobMetadata.mock(ConfigType.CHECK_CONNECTION_SOURCE))
    val destinationResponse = SynchronousResponse(succeeded, SynchronousJobMetadata.mock(ConfigType.CHECK_CONNECTION_DESTINATION))

    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) } returns sourceVersion
    every { synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null) } returns sourceResponse
    every { jobConverter.getSynchronousJobRead(sourceResponse) } returns emptySynchronousJobRead()
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination
    every { destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId) } returns
      destinationVersion
    every { synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null) } returns destinationResponse
    every { jobConverter.getSynchronousJobRead(destinationResponse) } returns emptySynchronousJobRead()

    schedulerHandler.checkSourceConnectionFromSourceId(SourceIdRequestBody(sourceId = source.sourceId))
    schedulerHandler.checkDestinationConnectionFromDestinationId(DestinationIdRequestBody(destinationId = destination.destinationId))

    verify(exactly = 2) { jsonSchemaValidator.ensure(secretSpec.connectionSpecification, validationConfig) }
    verify { synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null) }
    verify { synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null) }
  }

  @Test
  fun `failed actor ID checks leave source and destination drafts`() {
    val source = SourceHelpers.generateSource(UUID.randomUUID()).withIsDraft(true)
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID()).withIsDraft(true)
    val sourceDefinition = StandardSourceDefinition().withSourceDefinitionId(source.sourceDefinitionId)
    val destinationDefinition = StandardDestinationDefinition().withDestinationDefinitionId(destination.destinationDefinitionId)
    val sourceVersion = ActorDefinitionVersion().withSpec(CONNECTOR_SPECIFICATION)
    val destinationVersion = ActorDefinitionVersion().withSpec(CONNECTOR_SPECIFICATION)
    val failed = StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED)
    val sourceResponse = SynchronousResponse(failed, SynchronousJobMetadata.mock(ConfigType.CHECK_CONNECTION_SOURCE))
    val destinationResponse = SynchronousResponse(failed, SynchronousJobMetadata.mock(ConfigType.CHECK_CONNECTION_DESTINATION))

    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) } returns sourceVersion
    every { synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null) } returns sourceResponse
    every { jobConverter.getSynchronousJobRead(sourceResponse) } returns emptySynchronousJobRead()
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination
    every { destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId) } returns
      destinationVersion
    every { synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null) } returns destinationResponse
    every { jobConverter.getSynchronousJobRead(destinationResponse) } returns emptySynchronousJobRead()

    schedulerHandler.checkSourceConnectionFromSourceId(SourceIdRequestBody(sourceId = source.sourceId))
    schedulerHandler.checkDestinationConnectionFromDestinationId(DestinationIdRequestBody(destinationId = destination.destinationId))

    verify(exactly = 0) { sourceService.promoteSourceFromDraft(any()) }
    verify(exactly = 0) { destinationService.promoteDestinationFromDraft(any()) }
  }

  @Test
  fun testCheckSourceConnectionFromSourceCreate() {
    val source =
      SourceConnection()
        .withWorkspaceId(SOURCE.workspaceId)
        .withSourceDefinitionId(SOURCE.sourceDefinitionId)
        .withConfiguration(SOURCE.getConfiguration())

    val sourceCoreConfig =
      SourceCoreConfig(
        sourceDefinitionId = source.sourceDefinitionId,
        connectionConfiguration = source.getConfiguration(),
        workspaceId = source.workspaceId,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            listOf(
              JobTypeResourceLimit()
                .withJobType(
                  JobTypeResourceLimit.JobType.CHECK_CONNECTION,
                ).withResourceRequirements(RESOURCE_REQUIREMENT),
            ),
          ),
        )
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        null,
      )
    } returns sourceVersion

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every { secretsRepositoryWriter.createEphemeralFromConfig(processed, any()) } returns source.getConfiguration()

    every {
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        RESOURCE_REQUIREMENT,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>

    schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig)

    verify { synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, RESOURCE_REQUIREMENT) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, null) }
  }

  @Test
  fun testCheckSourceConnectionFromUpdate() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val sourceUpdate =
      SourceUpdate(name = source.name, sourceId = source.sourceId, connectionConfiguration = source.getConfiguration())
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every {
      configurationUpdate.source(
        source.sourceId,
        source.name,
        sourceUpdate.connectionConfiguration,
      )
    } returns source
    val submittedSource =
      SourceConnection()
        .withSourceId(source.sourceId)
        .withSourceDefinitionId(source.sourceDefinitionId)
        .withConfiguration(source.getConfiguration())
        .withWorkspaceId(source.workspaceId)
    every {
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        submittedSource,
        sourceVersion,
        false,
        null,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every { secretsRepositoryWriter.createEphemeralFromConfig(processed, any()) } returns source.getConfiguration()

    schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate)

    verify { jsonSchemaValidator.ensure(CONNECTOR_SPECIFICATION.getConnectionSpecification(), source.getConfiguration()) }
    verify { synchronousSchedulerClient.createSourceCheckConnectionJob(submittedSource, sourceVersion, false, null) }
    verify(exactly = 2) { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) }
  }

  @Test
  fun testCheckDestinationConnectionFromDestinationId() {
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID())
    val request = DestinationIdRequestBody(destinationId = destination.destinationId)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.destinationDefinitionId)
    every { destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId) } returns destinationDefinition
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      )
    } returns destinationVersion
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination
    every {
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        null,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>

    schedulerHandler.checkDestinationConnectionFromDestinationId(request)

    verify { destinationService.getDestinationConnection(destination.destinationId) }
    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId) }
    verify { synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null) }
  }

  @Test
  fun testCheckDestinationConnectionFromDestinationCreate() {
    val destination =
      DestinationConnection()
        .withWorkspaceId(DESTINATION.workspaceId)
        .withDestinationDefinitionId(DESTINATION.destinationDefinitionId)
        .withConfiguration(DESTINATION.getConfiguration())

    val destinationCoreConfig =
      DestinationCoreConfig(
        workspaceId = destination.workspaceId,
        destinationDefinitionId = destination.destinationDefinitionId,
        connectionConfiguration = destination.getConfiguration(),
      )

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.destinationDefinitionId)
    every {
      destinationService.getStandardDestinationDefinition(
        destination.destinationDefinitionId,
      )
    } returns destinationDefinition
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        null,
      )
    } returns destinationVersion

    every {
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        null,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>

    val processed =
      processConfigSecrets(
        destination.getConfiguration(),
        destinationVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every { secretsRepositoryWriter.createEphemeralFromConfig(processed, any()) } returns destination.getConfiguration()

    schedulerHandler.checkDestinationConnectionFromDestinationCreate(destinationCoreConfig)

    verify { synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null) }
    verify { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, any()) }
  }

  @Test
  fun testCheckDestinationConnectionFromUpdate() {
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID())
    val destinationUpdate =
      DestinationUpdate(name = destination.name, destinationId = destination.destinationId, connectionConfiguration = destination.getConfiguration())
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.destinationDefinitionId)
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            listOf(
              JobTypeResourceLimit()
                .withJobType(
                  JobTypeResourceLimit.JobType.CHECK_CONNECTION,
                ).withResourceRequirements(RESOURCE_REQUIREMENT),
            ),
          ),
        )
    every {
      destinationService.getStandardDestinationDefinition(
        destination.destinationDefinitionId,
      )
    } returns destinationDefinition
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      )
    } returns destinationVersion
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination
    every {
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        RESOURCE_REQUIREMENT,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>
    every { destinationService.getDestinationConnection(destination.destinationId) } returns destination
    every {
      configurationUpdate.destination(
        destination.destinationId,
        destination.name,
        destinationUpdate.connectionConfiguration,
      )
    } returns destination
    val submittedDestination =
      DestinationConnection()
        .withDestinationId(destination.destinationId)
        .withDestinationDefinitionId(destination.destinationDefinitionId)
        .withConfiguration(destination.getConfiguration())
        .withWorkspaceId(destination.workspaceId)
    every {
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        submittedDestination,
        destinationVersion,
        false,
        RESOURCE_REQUIREMENT,
      )
    } returns jobResponse as SynchronousResponse<StandardCheckConnectionOutput>

    val processed =
      processConfigSecrets(
        destination.getConfiguration(),
        destinationVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every { secretsRepositoryWriter.createEphemeralFromConfig(processed, any()) } returns destination.getConfiguration()

    schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(destinationUpdate)

    verify { jsonSchemaValidator.ensure(CONNECTOR_SPECIFICATION.getConnectionSpecification(), destination.getConfiguration()) }
    verify(exactly = 2) {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      )
    }
    verify { synchronousSchedulerClient.createDestinationCheckConnectionJob(submittedDestination, destinationVersion, false, RESOURCE_REQUIREMENT) }
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  fun testCheckConnectionReadFormat(
    standardCheckConnectionOutputStatusEmittedBySource: Optional<String?>,
    traceMessageEmittedBySource: Boolean,
    expectedCheckConnectionRead: CheckConnectionRead?,
  ) {
    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.fromValue(FAILURE_ORIGIN))
        .withFailureType(FailureReason.FailureType.fromValue(FAILURE_TYPE))
        .withRetryable(RETRYABLE)
        .withExternalMessage(EXTERNAL_MESSAGE)
        .withInternalMessage(INTERNAL_MESSAGE)
        .withStacktrace(STACKTRACE)
        .withTimestamp(CREATED_AT)

    val checkConnectionOutput: StandardCheckConnectionOutput?
    if (standardCheckConnectionOutputStatusEmittedBySource.isPresent()) {
      val status =
        StandardCheckConnectionOutput.Status.fromValue(standardCheckConnectionOutputStatusEmittedBySource.get())
      val message = if (status == StandardCheckConnectionOutput.Status.FAILED) "Something went wrong - check connection failure" else null
      checkConnectionOutput = StandardCheckConnectionOutput().withStatus(status).withMessage(message)
    } else {
      checkConnectionOutput = null
    }

    // This replicates the behavior of the DefaultCheckConnectionWorker. It always adds both the
    // failureReason and the checkConnection if they are available.
    var exceptionWouldBeThrown = false
    val connectorJobOutput = ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
    if (standardCheckConnectionOutputStatusEmittedBySource.isPresent()) {
      connectorJobOutput.setCheckConnection(checkConnectionOutput)
    }
    if (traceMessageEmittedBySource) {
      connectorJobOutput.setFailureReason(failureReason)
    }
    if (!standardCheckConnectionOutputStatusEmittedBySource.isPresent() && !traceMessageEmittedBySource) {
      exceptionWouldBeThrown = true
    }

    // This replicates the behavior of the TemporalClient. If there is a failureReason,
    // it declares the job a failure.
    val jobSucceeded = !exceptionWouldBeThrown && connectorJobOutput.failureReason == null
    val jobMetadata = JobMetadata(jobSucceeded, LOG_PATH)
    val synchronousJobMetadata =
      fromJobMetadata(
        jobMetadata,
        connectorJobOutput,
        SYNC_JOB_ID,
        ConfigType.CHECK_CONNECTION_SOURCE,
        CONFIG_ID.get(),
        CREATED_AT,
        CREATED_AT,
      )

    val checkResponse = SynchronousResponse(checkConnectionOutput!!, synchronousJobMetadata)

    // Below is just to mock checkSourceConnectionFromSourceCreate as a public interface that uses
    // reportConnectionStatus
    val source =
      SourceConnection()
        .withWorkspaceId(SOURCE.workspaceId)
        .withSourceDefinitionId(SOURCE.sourceDefinitionId)
        .withConfiguration(SOURCE.getConfiguration())

    val sourceCoreConfig =
      SourceCoreConfig(
        sourceDefinitionId = source.sourceDefinitionId,
        connectionConfiguration = source.getConfiguration(),
        workspaceId = source.workspaceId,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every { secretsRepositoryWriter.createEphemeralFromConfig(processed, any()) } returns source.getConfiguration()

    every {
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        null,
      )
    } returns checkResponse

    val checkConnectionRead = schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig)
    org.junit.jupiter.api.Assertions
      .assertEquals(expectedCheckConnectionRead, checkConnectionRead)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceId() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)

    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns false
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val metadata =
      SynchronousJobMetadata(
        SCOPE,
        ConfigType.SYNC,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
        CONNECTOR_CONFIG_UPDATED,
        LOG_PATH,
        null,
      )
    val discoverResponse = SynchronousResponse<UUID>(UUID.randomUUID(), metadata)
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID())
    every { catalogService.getActorCatalogById(any()) } returns actorCatalog

    val connectionRead = connectionRead(sourceId = source.sourceId)
    val connectionReadList = ConnectionReadList(connections = listOf(connectionRead))
    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns connectionReadList

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
        .withResourceRequirements(
          ScopedResourceRequirements().withJobSpecific(
            listOf(
              JobTypeResourceLimit()
                .withJobType(
                  JobTypeResourceLimit.JobType.DISCOVER_SCHEMA,
                ).withResourceRequirements(RESOURCE_REQUIREMENT),
            ),
          ),
        )
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every { catalogService.getActorCatalog(any(), any(), any()) } returns Optional.empty<ActorCatalog>()
    every {
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        RESOURCE_REQUIREMENT,
        WorkloadPriority.HIGH,
      )
    } returns discoverResponse

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.catalogId, discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify { sourceService.getSourceConnection(source.sourceId) }
    verify { catalogService.getActorCatalog(request.sourceId, SOURCE_DOCKER_TAG, any()) }

    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) }
    verify { synchronousSchedulerClient.createDiscoverSchemaJob(source, sourceVersion, false, RESOURCE_REQUIREMENT, WorkloadPriority.HIGH) }
  }

  @Test
  fun `draft source cannot be discovered`() {
    val source = SourceHelpers.generateSource(UUID.randomUUID()).withIsDraft(true)
    every { sourceService.getSourceConnection(source.sourceId) } returns source

    assertThrows<ActorNotReadyProblem> {
      schedulerHandler.discoverSchemaForSourceFromSourceId(SourceDiscoverSchemaRequestBody(sourceId = source.sourceId))
    }

    verify { synchronousSchedulerClient wasNot Called }
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdCachedCatalog() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)

    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns false
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val thisCatalogId = UUID.randomUUID()
    val synchronousJobMetadata =
      SynchronousJobMetadata(
        SCOPE,
        ConfigType.SYNC,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
        CONNECTOR_CONFIG_UPDATED,
        LOG_PATH,
        null,
      )
    val discoverResponse = SynchronousResponse<UUID?>(thisCatalogId, synchronousJobMetadata)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(SOURCE_DOCKER_TAG)
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(thisCatalogId)
    every { catalogService.getActorCatalog(any(), any(), any()) } returns Optional.of(actorCatalog)

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.catalogId, discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify { sourceService.getSourceConnection(source.sourceId) }
    verify { catalogService.getActorCatalog(request.sourceId, any(), any()) }

    verify(exactly = 0) { catalogService.writeActorCatalogWithFetchEvent(any<AirbyteCatalog>(), any(), any(), any()) }

    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) }
    verify(exactly = 0) { synchronousSchedulerClient.createDiscoverSchemaJob(any(), any(), any(), any(), any()) }
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdDisableCache() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId, disableCache = true)

    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns false
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val discoveredCatalogId = UUID.randomUUID()
    val synchronousJobMetadata =
      SynchronousJobMetadata(
        SCOPE,
        ConfigType.SYNC,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
        CONNECTOR_CONFIG_UPDATED,
        LOG_PATH,
        null,
      )
    val discoverResponse = SynchronousResponse<UUID>(discoveredCatalogId, synchronousJobMetadata)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId)
    every { catalogService.getActorCatalogById(discoveredCatalogId) } returns actorCatalog
    every {
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      )
    } returns discoverResponse

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify { sourceService.getSourceConnection(source.sourceId) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) }
    verify { synchronousSchedulerClient.createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH) }
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdFailed() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)

    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns false
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    every {
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      )
    } returns jobResponse as SynchronousResponse<UUID>

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertFalse(actual.jobInfo!!.succeeded)
    verify { sourceService.getSourceConnection(source.sourceId) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId) }
    verify { synchronousSchedulerClient.createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH) }
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdDisabledWorkspace() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)
    every { sourceService.getSourceConnection(source.sourceId) } returns source
    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns true
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val e = assertThrows<NotFoundException> { schedulerHandler.discoverSchemaForSourceFromSourceId(request) }
    assertEquals("Cannot run discover on tombstoned workspace", e.message)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceCreateDisabledWorkspace() {
    val source =
      SourceConnection()
        .withSourceDefinitionId(SOURCE.sourceDefinitionId)
        .withConfiguration(SOURCE.configuration)
        .withWorkspaceId(SOURCE.workspaceId)
    val sourceCoreConfig =
      SourceCoreConfig(
        sourceDefinitionId = source.sourceDefinitionId,
        connectionConfiguration = source.configuration,
        workspaceId = source.workspaceId,
      )
    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns true
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val e = assertThrows<NotFoundException> { schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig) }
    assertEquals("Cannot run discover on tombstoned workspace", e.message)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceCreate() {
    val source =
      SourceConnection()
        .withSourceDefinitionId(SOURCE.sourceDefinitionId)
        .withConfiguration(SOURCE.getConfiguration())
        .withWorkspaceId(SOURCE.workspaceId)

    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns false
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val synchronousJobMetadata =
      SynchronousJobMetadata(
        SCOPE,
        ConfigType.SYNC,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
        CONNECTOR_CONFIG_UPDATED,
        LOG_PATH,
        null,
      )

    val discoverResponse = SynchronousResponse<UUID>(UUID.randomUUID(), synchronousJobMetadata)

    val sourceCoreConfig =
      SourceCoreConfig(
        sourceDefinitionId = source.sourceDefinitionId,
        connectionConfiguration = source.getConfiguration(),
        workspaceId = source.workspaceId,
      )
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID())
    every { catalogService.getActorCatalogById(any()) } returns actorCatalog

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        null,
      )
    } returns sourceVersion
    every {
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      )
    } returns discoverResponse

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every { secretsRepositoryWriter.createEphemeralFromConfig(processed, any()) } returns source.getConfiguration()

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.catalogId, discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify { synchronousSchedulerClient.createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH) }
    verify { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, null) }
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceCreateFailed() {
    val source =
      SourceConnection()
        .withSourceDefinitionId(SOURCE.sourceDefinitionId)
        .withConfiguration(SOURCE.getConfiguration())
        .withWorkspaceId(SOURCE.workspaceId)

    val mockWorkspace =
      mockk<StandardWorkspace>().also {
        every { it.tombstone } returns false
      }
    every { workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true) } returns mockWorkspace

    val sourceCoreConfig =
      SourceCoreConfig(
        sourceDefinitionId = source.sourceDefinitionId,
        connectionConfiguration = source.getConfiguration(),
        workspaceId = source.workspaceId,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, null) } returns sourceVersion
    every {
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      )
    } returns jobResponse as SynchronousResponse<UUID>

    val processedSourceConfig =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    every {
      secretsRepositoryWriter.createEphemeralFromConfig(
        processedSourceConfig,
        any(),
      )
    } returns source.getConfiguration()

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig)

    org.junit.jupiter.api.Assertions
      .assertNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertFalse(actual.jobInfo!!.succeeded)
    verify { synchronousSchedulerClient.createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH) }
  }

  @Test
  @DisplayName("Test enum compatibility")
  fun testEnumCompatibility() {
    Assertions.assertThat(isCompatible<ConfigType, JobConfigType>()).isTrue()
  }

  @Test
  fun testEnumConversion() {
    org.junit.jupiter.api.Assertions.assertTrue(
      isCompatible<StandardCheckConnectionOutput.Status, CheckConnectionRead.Status>(),
    )
    org.junit.jupiter.api.Assertions.assertTrue(
      isCompatible<JobStatus, io.airbyte.api.server.generated.models.JobStatus>(),
    )
  }

  @Test
  fun testSyncConnection() {
    val connectionId = UUID.randomUUID()

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    every { eventRunner.startNewManualSync(connectionId) } returns manualOperationResult

    every { jobConverter.getJobInfoRead(any()) } returns sampleJobInfoRead()

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId))

    verify { eventRunner.startNewManualSync(connectionId) }
  }

  @Test
  fun `draft actors cannot sync reset or create jobs`() {
    val connectionId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val destinationId = UUID.randomUUID()
    val sync =
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withStatus(StandardSync.Status.ACTIVE)
    every { connectionService.getStandardSync(connectionId) } returns sync
    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withSourceId(sourceId).withIsDraft(true)
    every { destinationService.getDestinationConnection(destinationId) } returns
      DestinationConnection().withDestinationId(destinationId).withIsDraft(false)

    assertThrows<ActorNotReadyProblem> {
      schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId))
    }
    assertThrows<ActorNotReadyProblem> {
      schedulerHandler.resetConnection(ConnectionIdRequestBody(connectionId = connectionId))
    }
    assertThrows<ActorNotReadyProblem> {
      schedulerHandler.resetConnectionStream(ConnectionStreamRequestBody(connectionId = connectionId, streams = emptyList()))
    }
    assertThrows<ActorNotReadyProblem> {
      schedulerHandler.createJob(JobCreate(connectionId = connectionId, isScheduled = false))
    }

    verify { eventRunner wasNot Called }
  }

  @Test
  fun testSyncConnectionUsesExistingManualSyncPathWhenDataWorkerEnforcementDisabled() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } returns false
    every { eventRunner.startNewManualSync(connectionId) } returns manualOperationResult
    every { jobPersistence.getJob(jobId) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(jobId)

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId)

    verify { eventRunner.startNewManualSync(connectionId) }
    verify(exactly = 0) { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) }
  }

  @Test
  fun testSyncConnectionWaitsOnlyForJobIdWhenRequestedEvenIfDataWorkerEnforcementDisabled() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } returns false
    every { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) } returns manualOperationResult
    every { jobPersistence.getJob(jobId) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(jobId)

    val result = schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId, returnAfterJobId = true)

    verify { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) }
    verify(exactly = 0) { eventRunner.startNewManualSync(connectionId) }
    Assertions.assertThat(result.job.id).isEqualTo(jobId)
  }

  @Test
  fun testSyncConnectionWaitsOnlyForJobIdWhenDataWorkerCapacityIsEnforced() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } returns true
    every { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) } returns manualOperationResult
    every { jobPersistence.getJob(jobId) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(jobId)

    val result = schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId)

    verify { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) }
    verify(exactly = 0) { eventRunner.startNewManualSync(connectionId) }
    Assertions.assertThat(result.job.id).isEqualTo(jobId)
  }

  @Test
  fun testSyncConnectionUsesExistingManualSyncPathWhenNoOrganizationIdIsSupplied() {
    val connectionId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    every { eventRunner.startNewManualSync(connectionId) } returns manualOperationResult
    every { jobPersistence.getJob(jobId) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(jobId)

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId))

    verify { eventRunner.startNewManualSync(connectionId) }
    verify(exactly = 0) { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) }
    verify { featureFlagClient wasNot Called }
  }

  @Test
  fun testSyncConnectionUsesExistingManualSyncPathWhenDataWorkerEnforcementFlagLookupFails() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    every { featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)) } throws RuntimeException("flag lookup failed")
    every { eventRunner.startNewManualSync(connectionId) } returns manualOperationResult
    every { jobPersistence.getJob(jobId) } returns job
    every { jobConverter.getJobInfoRead(job) } returns sampleJobInfoRead(jobId)

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId)

    verify { eventRunner.startNewManualSync(connectionId) }
    verify(exactly = 0) { eventRunner.startNewManualSyncAndWaitForJobId(connectionId) }
  }

  @Test
  fun testSyncConnectionFailWithOtherSyncRunning() {
    val connectionId = UUID.randomUUID()

    val manualOperationResult = ManualOperationResult("another sync running", null, ErrorCode.WORKFLOW_RUNNING)

    every { eventRunner.startNewManualSync(connectionId) } returns manualOperationResult

    org.junit.jupiter.api.Assertions.assertThrows(
      ValueConflictKnownException::class.java,
    ) { schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId)) }
  }

  @Test
  fun disabledSyncThrows() {
    val connectionId = UUID.randomUUID()
    every { connectionService.getStandardSync(connectionId) } returns StandardSync().withStatus(StandardSync.Status.INACTIVE)
    org.junit.jupiter.api.Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      schedulerHandler.syncConnection(
        ConnectionIdRequestBody(connectionId = connectionId),
      )
    }
  }

  @Test
  fun deprecatedSyncThrows() {
    val connectionId = UUID.randomUUID()
    every { connectionService.getStandardSync(connectionId) } returns StandardSync().withStatus(StandardSync.Status.DEPRECATED)
    org.junit.jupiter.api.Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      schedulerHandler.syncConnection(
        ConnectionIdRequestBody(connectionId = connectionId),
      )
    }
  }

  @Test
  fun lockedSyncThrows() {
    val connectionId = UUID.randomUUID()
    every { connectionService.getStandardSync(connectionId) } returns StandardSync().withStatus(StandardSync.Status.LOCKED)
    org.junit.jupiter.api.Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      schedulerHandler.syncConnection(
        ConnectionIdRequestBody(connectionId = connectionId),
      )
    }
  }

  @Test
  fun testResetConnection() {
    val connectionId = UUID.randomUUID()

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    val streamDescriptors = listOf(STREAM_DESCRIPTOR)
    every { connectionService.getAllStreamsForConnection(connectionId) } returns streamDescriptors

    every { eventRunner.resetConnection(connectionId, streamDescriptors) } returns manualOperationResult

    every { jobConverter.getJobInfoRead(any()) } returns sampleJobInfoRead()

    schedulerHandler.resetConnection(ConnectionIdRequestBody(connectionId = connectionId))

    verify { eventRunner.resetConnection(connectionId, streamDescriptors) }
  }

  @Test
  fun testResetConnectionStream() {
    val connectionId = UUID.randomUUID()
    val streamName = "name"
    val streamNamespace = "namespace"

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)
    val streamDescriptors = listOf(StreamDescriptor().withName(streamName).withNamespace(streamNamespace))
    val connectionStreamRequestBody =
      ConnectionStreamRequestBody(
        connectionId = connectionId,
        streams = listOf(ConnectionStream(streamName = streamName, streamNamespace = streamNamespace)),
      )

    every { eventRunner.resetConnection(connectionId, streamDescriptors) } returns manualOperationResult

    every { jobConverter.getJobInfoRead(any()) } returns sampleJobInfoRead()

    schedulerHandler
      .resetConnectionStream(connectionStreamRequestBody)

    verify { eventRunner.resetConnection(connectionId, streamDescriptors) }
  }

  @Test
  fun testResetConnectionStreamWithEmptyList() {
    val connectionId = UUID.randomUUID()
    val streamName = "name"
    val streamNamespace = "namespace"

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)
    val streamDescriptors = listOf(StreamDescriptor().withName(streamName).withNamespace(streamNamespace))
    val connectionStreamRequestBody =
      ConnectionStreamRequestBody(
        connectionId = connectionId,
        streams = emptyList(),
      )

    every { connectionService.getAllStreamsForConnection(connectionId) } returns streamDescriptors
    every { eventRunner.resetConnection(connectionId, streamDescriptors) } returns manualOperationResult

    every { jobConverter.getJobInfoRead(any()) } returns sampleJobInfoRead()

    schedulerHandler
      .resetConnectionStream(connectionStreamRequestBody)

    verify { eventRunner.resetConnection(connectionId, streamDescriptors) }
  }

  @Test
  fun testCancelJob() {
    every { jobPersistence.getJob(JOB_ID) } returns job

    val manualOperationResult = ManualOperationResult(null, JOB_ID, null)

    every { eventRunner.startNewCancellation(SCOPE) } returns manualOperationResult
    val jobInfo =
      sampleJobInfoRead(JOB_ID, createdAt = 123L, updatedAt = 321L)
    every { jobConverter.getJobInfoRead(any()) } returns jobInfo

    schedulerHandler.cancelJob(JobIdRequestBody(id = JOB_ID))

    verify { eventRunner.startNewCancellation(SCOPE) }
  }

  @Test
  fun testAutoPropagateSchemaChangeAddStream() {
    // Verify that if auto propagation is fully enabled, a new stream can be added.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockNewStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val discoveredCatalog = catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
    val catalogWithDiff =
      discoveredCatalog.copy(
        streams =
          discoveredCatalog.streams +
            AirbyteStreamAndConfiguration(
              stream = AirbyteStream(name = A_DIFFERENT_STREAM, supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
              config =
                AirbyteStreamConfiguration(
                  selected = true,
                  syncMode = SyncMode.FULL_REFRESH,
                  destinationSyncMode = DestinationSyncMode.OVERWRITE,
                ),
            ),
      )

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace

    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns ConnectionReadList(connections = listOf(CONNECTION))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) }
  }

  @Test
  fun testAutoPropagateSchemaChangeUpdateStream() {
    // Verify that if auto propagation is fully enabled, an existing stream can be modified.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockUpdateStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithDiff =
      catalogConverter.toApi(
        CatalogHelpers.createAirbyteCatalog(
          SHOES,
          Field.of(SKU, JsonSchemaType.STRING),
          Field.of("aDifferentField", JsonSchemaType.STRING),
        ),
        sourceVersion,
      )

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace

    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns ConnectionReadList(connections = listOf(CONNECTION))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) }
  }

  @Test
  fun testAutoPropagateSchemaChangeRemoveStream() {
    // Verify that if auto propagation is fully enabled, an existing stream can be removed.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockRemoveStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithDiff =
      catalogConverter
        .toApi(clone(airbyteCatalog), sourceVersion)
        .copy(streams = emptyList())

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace

    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns ConnectionReadList(connections = listOf(CONNECTION))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) }
  }

  @Test
  fun testAutoPropagateSchemaChangeColumnsOnly() {
    // Verify that if auto propagation is set to PROPAGATE_COLUMNS, then column changes are applied but
    // a new stream is ignored.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion
    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockUpdateAndAddStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithNewColumn =
      catalogConverter.toApi(
        CatalogHelpers.createAirbyteCatalog(
          SHOES,
          Field.of(SKU, JsonSchemaType.STRING),
        ),
        sourceVersion,
      )
    val catalogWithNewColumnAndStream =
      catalogWithNewColumn.copy(
        streams = catalogWithNewColumn.streams + AirbyteStreamAndConfiguration(stream = AirbyteStream(name = A_DIFFERENT_STREAM)),
      )

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace

    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns ConnectionReadList(connections = listOf(CONNECTION))
    val request =
      SourceAutoPropagateChange(
        sourceId = source.sourceId,
        workspaceId = source.workspaceId,
        catalogId = discoveredSourceId,
        catalog = catalogWithNewColumnAndStream,
      )

    schedulerHandler.applySchemaChangeForSource(request)

    verify { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) }
  }

  @Test
  fun testAutoPropagateSchemaChangeWithIgnoreMode() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockConnectionForDiscoverJobWithAutopropagation(source, sourceVersion, NonBreakingChangesPreference.IGNORE)
    mockNewStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val discoveredCatalog = catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
    val catalogWithDiff =
      discoveredCatalog.copy(
        streams = discoveredCatalog.streams + AirbyteStreamAndConfiguration(stream = AirbyteStream(name = A_DIFFERENT_STREAM)),
      )

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace

    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(exactly = 0) { connectionsHandler.updateConnection(any<ConnectionUpdate>(), any<String>(), any()) }
  }

  @Test
  fun testSchemaPropagatedEmptyDiff() {
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()

    val oldCatalog =
      io.airbyte.api.server.generated.models.AirbyteCatalog(
        streams = listOf(AirbyteStreamAndConfiguration(stream = AirbyteStream(name = "foo", namespace = "ns"))),
      )
    val configuredCatalog =
      io.airbyte.api.server.generated.models.AirbyteCatalog(
        streams =
          listOf(
            AirbyteStreamAndConfiguration(
              stream =
                AirbyteStream(
                  name = "foo",
                  namespace = "ns",
                  jsonSchema = emptyObject(),
                  supportedSyncModes = listOf(SyncMode.FULL_REFRESH),
                ),
              config =
                AirbyteStreamConfiguration(
                  selected = true,
                  syncMode = SyncMode.FULL_REFRESH,
                  destinationSyncMode = DestinationSyncMode.APPEND,
                ),
            ),
          ),
      )

    val newCatalog =
      io.airbyte.api.server.generated.models.AirbyteCatalog(
        streams = listOf(AirbyteStreamAndConfiguration(stream = AirbyteStream(name = "foo", namespace = "ns"))),
      )

    val notificationSettings = NotificationSettings().withSendOnConnectionUpdate(NotificationItem())
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withNotificationSettings(notificationSettings)
    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withSourceId(sourceId)
    val connectionRead =
      connectionRead(
        connectionId = connectionId,
        sourceId = sourceId,
        syncCatalog = configuredCatalog,
        nonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_COLUMNS,
      )
    every { connectionsHandler.listConnectionsForSource(sourceId, false) } returns ConnectionReadList(connections = listOf(connectionRead))
    every { connectionsHandler.getConnectionAirbyteCatalog(connectionId) } returns Optional.of(oldCatalog)

    val diff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.ADD_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = "new_stream"),
            ),
          ),
      )
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns diff

    val spySchedulerHandler = spyk(schedulerHandler)
    val request =
      SourceAutoPropagateChange(sourceId = sourceId, workspaceId = workspaceId, catalogId = catalogId, catalog = newCatalog)
    spySchedulerHandler.applySchemaChangeForSource(request)
    verify { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) }
  }

  @Test
  fun testEmptyDiffIsAlwaysPropagated() {
    // Verify that if auto propagation is fully enabled, a new stream can be added.
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns sourceVersion

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockEmptyDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    // NOTE: this is just a clone of the original catalog.
    val discoveredCatalog =
      catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true) } returns workspace

    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns ConnectionReadList(connections = listOf(CONNECTION))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = discoveredCatalog)

    schedulerHandler.applySchemaChangeForSource(request)

    verify { connectionsHandler.applySchemaChange(any(), any(), any(), any(), any()) }
  }

  private fun mockSourceForDiscoverJob(
    source: SourceConnection,
    sourceDefinition: StandardSourceDefinition,
  ) {
    every { sourceService.getStandardSourceDefinition(source.sourceDefinitionId) } returns sourceDefinition
    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      )
    } returns
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
    every { sourceService.getSourceConnection(source.sourceId) } returns source
  }

  private fun mockSuccessfulDiscoverJob(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion,
  ): UUID {
    val newSourceCatalogId = UUID.randomUUID()

    val synchronousJobMetadata =
      SynchronousJobMetadata(
        SCOPE,
        ConfigType.SYNC,
        null,
        CREATED_AT,
        CREATED_AT,
        true,
        CONNECTOR_CONFIG_UPDATED,
        LOG_PATH,
        null,
      )

    val discoverResponse = SynchronousResponse<UUID>(newSourceCatalogId, synchronousJobMetadata)

    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(newSourceCatalogId)

    every { catalogService.getActorCatalogById(any()) } returns actorCatalog
    every {
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      )
    } returns discoverResponse
    return newSourceCatalogId
  }

  private fun mockConnectionForDiscoverJobWithAutopropagation(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion?,
    nonBreakingChangesPreference: NonBreakingChangesPreference?,
  ): ConnectionRead {
    val connectionRead =
      connectionRead(
        sourceId = source.sourceId,
        syncCatalog = catalogConverter.toApi(airbyteCatalog, sourceVersion),
        notifySchemaChanges = false,
        nonBreakingChangesPreference = nonBreakingChangesPreference,
      )
    val connectionReadList = ConnectionReadList(connections = listOf(connectionRead))
    every { connectionsHandler.listConnectionsForSource(source.sourceId, false) } returns connectionReadList
    val catalogDiff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.ADD_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = A_DIFFERENT_STREAM),
            ),
          ),
      )
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff
    return connectionRead
  }

  private fun mockNewStreamDiff() {
    val catalogDiff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.ADD_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = A_DIFFERENT_STREAM),
            ),
          ),
      )
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff
  }

  private fun mockRemoveStreamDiff() {
    val catalogDiff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.REMOVE_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = SHOES),
            ),
          ),
      )
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff
  }

  private fun mockUpdateStreamDiff() {
    val catalogDiff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = SHOES),
              updateStream =
                StreamTransformUpdateStream(
                  fieldTransforms =
                    listOf(
                      FieldTransform(
                        transformType = FieldTransform.TransformType.ADD_FIELD,
                        fieldName = listOf("aDifferentField"),
                        addField = FieldAdd(schema = deserialize("\"id\": {\"type\": [\"null\", \"integer\"]}")),
                        breaking = false,
                      ),
                    ),
                  streamAttributeTransforms = emptyList(),
                ),
            ),
          ),
      )
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff
  }

  private fun mockUpdateAndAddStreamDiff() {
    val catalogDiff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = SHOES),
              updateStream =
                StreamTransformUpdateStream(
                  fieldTransforms =
                    listOf(
                      FieldTransform(
                        transformType = FieldTransform.TransformType.ADD_FIELD,
                        fieldName = listOf("aDifferentField"),
                        addField = FieldAdd(schema = deserialize("\"id\": {\"type\": [\"null\", \"integer\"]}")),
                        breaking = false,
                      ),
                    ),
                  streamAttributeTransforms = emptyList(),
                ),
            ),
            StreamTransform(
              transformType = StreamTransform.TransformType.ADD_STREAM,
              streamDescriptor =
                io.airbyte.api.server.generated.models
                  .StreamDescriptor(name = A_DIFFERENT_STREAM),
            ),
          ),
      )
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns catalogDiff
  }

  private fun mockEmptyDiff() {
    val emptyDiff = CatalogDiff(transforms = emptyList())
    every { connectionsHandler.getDiff(any(), any(), any(), any()) } returns emptyDiff
  }

  companion object {
    private fun sampleJobInfoRead(
      id: Long = 0L,
      createdAt: Long = 0L,
      updatedAt: Long = 0L,
    ): JobInfoRead =
      JobInfoRead(
        job =
          JobRead(
            id = id,
            configType = JobConfigType.SYNC,
            configId = UUID.randomUUID().toString(),
            createdAt = createdAt,
            updatedAt = updatedAt,
            status = io.airbyte.api.server.generated.models.JobStatus.RUNNING,
          ),
        attempts = emptyList(),
      )

    private const val SOURCE_DOCKER_REPO = "srcimage"
    private const val SOURCE_DOCKER_TAG = "tag"
    private const val SOURCE_PROTOCOL_VERSION = "0.4.5"

    private const val DESTINATION_DOCKER_REPO = "dstimage"
    private const val DESTINATION_DOCKER_TAG = "tag"
    private const val DESTINATION_PROTOCOL_VERSION = "0.7.9"
    private const val NAME = "name"
    private const val DOGS = "dogs"
    private const val SHOES = "shoes"
    private const val SKU = "sku"
    private const val CONNECTION_URL = "connection_url"

    private const val JOB_ID = 123L
    private val SCOPE: UUID = UUID.randomUUID()
    private val SYNC_JOB_ID: UUID = UUID.randomUUID()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val DOCKER_REPOSITORY = "docker-repo"
    private const val DOCKER_IMAGE_TAG = "0.0.1"
    private val DOCKER_IMAGE_NAME: String = DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG
    private val STREAM_DESCRIPTOR1: StreamDescriptor = StreamDescriptor().withName("stream 1").withNamespace("namespace 1")
    private val STREAM_DESCRIPTOR2: StreamDescriptor = StreamDescriptor().withName("stream 2").withNamespace("namespace 2")
    private val CREATED_AT = System.currentTimeMillis() / 1000
    private const val CONNECTOR_CONFIG_UPDATED = false
    private val LOG_PATH: Path = Path.of("log_path")
    private val CONFIG_ID: Optional<UUID> = Optional.of<UUID>(UUID.randomUUID())
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private const val FAILURE_ORIGIN = "source"
    private const val FAILURE_TYPE = "system_error"
    private const val RETRYABLE = true
    private const val EXTERNAL_MESSAGE = "Source did something wrong"
    private const val INTERNAL_MESSAGE = "Internal message related to system error"
    private const val STACKTRACE = "Stacktrace"

    private val airbyteCatalog: AirbyteCatalog =
      CatalogHelpers.createAirbyteCatalog(
        SHOES,
        Field.of(SKU, JsonSchemaType.STRING),
      )

    private val SOURCE: SourceConnection =
      SourceConnection()
        .withName("my postgres db")
        .withWorkspaceId(UUID.randomUUID())
        .withSourceDefinitionId(UUID.randomUUID())
        .withSourceId(UUID.randomUUID())
        .withConfiguration(emptyObject())
        .withTombstone(false)

    private val DESTINATION: DestinationConnection =
      DestinationConnection()
        .withName("my db2 instance")
        .withWorkspaceId(UUID.randomUUID())
        .withDestinationDefinitionId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withConfiguration(emptyObject())
        .withTombstone(false)

    private val CONNECTOR_SPECIFICATION: ConnectorSpecification =
      ConnectorSpecification()
        .withDocumentationUrl(URI("https://google.com"))
        .withChangelogUrl(URI("https://google.com"))
        .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap<Any?, Any?>()))

    private val STREAM_DESCRIPTOR: StreamDescriptor = StreamDescriptor().withName("1")
    const val A_DIFFERENT_STREAM: String = "aDifferentStream"

    private val RESOURCE_REQUIREMENT: ResourceRequirements? = ResourceRequirements().withCpuLimit("1.0").withCpuRequest("0.5")
    private val SOME_DESTINATION_DEFINITION: StandardDestinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
    private val SOME_ACTOR_DEFINITION: ActorDefinitionVersion =
      ActorDefinitionVersion().withSpec(
        ConnectorSpecification()
          .withSupportedDestinationSyncModes(
            listOf(
              io.airbyte.protocol.models.v0.DestinationSyncMode.OVERWRITE,
              io.airbyte.protocol.models.v0.DestinationSyncMode.APPEND,
              io.airbyte.protocol.models.v0.DestinationSyncMode.APPEND_DEDUP,
            ),
          ).withDocumentationUrl(URI.create("unused")),
      )

    val WEBHOOK_OPERATION: StandardSyncOperation =
      StandardSyncOperation()
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(OperatorWebhook())
    private val WEBHOOK_OPERATION_ID: UUID = UUID.randomUUID()

    private fun connectionRead(
      connectionId: UUID = UUID.randomUUID(),
      sourceId: UUID = UUID.randomUUID(),
      syncCatalog: io.airbyte.api.server.generated.models.AirbyteCatalog =
        io.airbyte.api.server.generated.models
          .AirbyteCatalog(streams = emptyList()),
      notifySchemaChanges: Boolean? = null,
      nonBreakingChangesPreference: NonBreakingChangesPreference? = null,
    ): ConnectionRead =
      ConnectionRead(
        connectionId = connectionId,
        name = NAME,
        sourceId = sourceId,
        destinationId = UUID.randomUUID(),
        syncCatalog = syncCatalog,
        status = ConnectionStatus.ACTIVE,
        breakingChange = false,
        notifySchemaChanges = notifySchemaChanges,
        nonBreakingChangesPreference = nonBreakingChangesPreference,
      )

    private fun emptySynchronousJobRead(): SynchronousJobRead =
      SynchronousJobRead(
        id = UUID.randomUUID(),
        configType = JobConfigType.CHECK_CONNECTION_SOURCE,
        createdAt = 0L,
        endedAt = 0L,
        succeeded = true,
      )

    private val CONNECTION: ConnectionRead =
      connectionRead(
        sourceId = SOURCE.sourceId,
        nonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_FULLY,
      )

    private fun baseSynchronousJobRead(
      succeeded: Boolean,
      failureReason: io.airbyte.api.server.generated.models.FailureReason? = null,
    ): SynchronousJobRead =
      SynchronousJobRead(
        id = SYNC_JOB_ID,
        configId = CONFIG_ID.toString(),
        configType = JobConfigType.CHECK_CONNECTION_SOURCE,
        createdAt = CREATED_AT,
        endedAt = CREATED_AT,
        succeeded = succeeded,
        connectorConfigurationUpdated = CONNECTOR_CONFIG_UPDATED,
        logType = LogFormatType.FORMATTED,
        logs = AttemptLogs(),
        failureReason = failureReason,
      )

    private val mockFailureReasonFromTrace: io.airbyte.api.server.generated.models.FailureReason? =
      io.airbyte.api.server.generated.models.FailureReason(
        failureOrigin = FailureOrigin.decode(FAILURE_ORIGIN),
        failureType = FailureType.decode(FAILURE_TYPE),
        retryable = RETRYABLE,
        externalMessage = EXTERNAL_MESSAGE,
        internalMessage = INTERNAL_MESSAGE,
        stacktrace = STACKTRACE,
        timestamp = CREATED_AT,
      )

    private val jobSuccessStatusSuccess: CheckConnectionRead? =
      CheckConnectionRead(
        jobInfo = baseSynchronousJobRead(succeeded = true),
        status = CheckConnectionRead.Status.SUCCEEDED,
      )

    private val jobSuccessStatusFailed: CheckConnectionRead? =
      CheckConnectionRead(
        jobInfo = baseSynchronousJobRead(succeeded = true),
        status = CheckConnectionRead.Status.FAILED,
        message = "Something went wrong - check connection failure",
      )

    private val jobFailedWithFailureReason: CheckConnectionRead? =
      CheckConnectionRead(
        jobInfo = baseSynchronousJobRead(succeeded = false, failureReason = mockFailureReasonFromTrace),
      )

    private val jobFailedWithoutFailureReason: CheckConnectionRead? =
      CheckConnectionRead(jobInfo = baseSynchronousJobRead(succeeded = false))

    @JvmStatic
    private fun provideArguments() =
      listOf<Arguments?>(
        Arguments.of(Optional.of<String?>("succeeded"), false, jobSuccessStatusSuccess),
        Arguments.of(Optional.of<String?>("succeeded"), true, jobFailedWithFailureReason),
        Arguments.of(Optional.of<String?>("failed"), false, jobSuccessStatusFailed),
        Arguments.of(Optional.of<String?>("failed"), true, jobFailedWithFailureReason),
      )
  }

  @Test
  fun `deleted workflow conflicts do not expose workflow internals`() {
    val result = ManualOperationResult("ConnectionManagerWorkflow internal failure", null, ErrorCode.WORKFLOW_DELETED)
    val error = assertThrows<ValueConflictKnownException> { schedulerHandler.readJobFromResult(result) }
    assertEquals("The connection has been deleted, so this operation cannot be completed.", error.message)
  }
}
