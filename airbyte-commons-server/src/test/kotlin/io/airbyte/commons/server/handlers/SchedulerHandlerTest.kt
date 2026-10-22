/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
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
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
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

    configurationUpdate = mock()
    jsonSchemaValidator = mock()
    synchronousSchedulerClient = mock()
    actorDefinitionService = mock()
    catalogService = mock()
    connectionService = mock()
    destinationService = mock()
    sourceService = mock()
    operationService = mock()

    whenever(connectionService.getStandardSync(any())).thenReturn(StandardSync().withStatus(StandardSync.Status.ACTIVE))
    whenever(destinationService.getStandardDestinationDefinition(any())).thenReturn(SOME_DESTINATION_DEFINITION)
    whenever(destinationService.getDestinationDefinitionFromConnection(any())).thenReturn(SOME_DESTINATION_DEFINITION)

    secretsRepositoryWriter = mock()
    jobPersistence = mock()
    eventRunner = mock()
    connectionsHandler = mock()
    actorDefinitionVersionHelper = mock()
    whenever(actorDefinitionVersionHelper.getDestinationVersion(any(), any())).thenReturn(SOME_ACTOR_DEFINITION)

    streamResetPersistence = mock()
    oAuthConfigSupplier = mock()
    jobCreator = mock()
    jobFactory = mock()
    jobNotifier = mock()
    jobTracker = mock()
    logClientManager = mock()
    logUtils = mock()

    whenever(logClientManager.getLogs(any())).thenReturn(LogEvents(emptyList(), "1"))
    jobConverter = spy(JobConverter(logClientManager, logUtils))

    featureFlagClient = mock<TestClient>()
    workspaceService = mock()
    secretPersistenceService = mock()
    metricClient = mock()
    secretStorageService = mock()

    streamRefreshesHandler = mock()
    whenever(streamRefreshesHandler.getRefreshesForConnection(any())).thenReturn(ArrayList())

    connectionTimelineEventHelper = mock()

    secretSanitizer =
      SecretSanitizer(
        actorDefinitionVersionHelper,
        destinationService,
        sourceService,
        secretPersistenceService,
        secretsRepositoryWriter,
        secretStorageService,
      )

    dataWorkerUsageService = mock()

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
    whenever<Long?>(jobFactory.createSync(CONNECTION_ID, isScheduled))
      .thenReturn(JOB_ID)
    whenever<StandardSync?>(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(mock())
    whenever<Job?>(jobPersistence.getJob(JOB_ID))
      .thenReturn(job)
    whenever<JobInfoRead?>(jobConverter.getJobInfoRead(job))
      .thenReturn(sampleJobInfoRead(JOB_ID))

    val output = schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = isScheduled))

    verify(jobFactory).createSync(CONNECTION_ID, isScheduled)
    Assertions.assertThat(output.job.id).isEqualTo(JOB_ID)
  }

  private fun stubSyncJobCreation() {
    whenever<Long?>(jobFactory.createSync(CONNECTION_ID, true))
      .thenReturn(JOB_ID)
    whenever<StandardSync?>(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(mock())
    whenever<Job?>(jobPersistence.getJob(JOB_ID))
      .thenReturn(job)
    whenever<JobInfoRead?>(jobConverter.getJobInfoRead(job))
      .thenReturn(sampleJobInfoRead(JOB_ID))
  }

  private fun stubWorkspaceForConnection(organizationId: UUID?) {
    whenever(workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, false))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withOrganizationId(organizationId))
  }

  @Test
  @DisplayName("Passive usage tracking records usage when capacity enforcement is disabled")
  fun createJobTracksUsagePassivelyWhenEnforcementDisabled() {
    val organizationId = UUID.randomUUID()
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId)
    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenReturn(false)

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(dataWorkerUsageService).insertUsageForCreatedJob(job)
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when capacity enforcement is enabled")
  fun createJobSkipsPassiveUsageTrackingWhenEnforcementEnabled() {
    val organizationId = UUID.randomUUID()
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId)
    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenReturn(true)

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(dataWorkerUsageService, never()).insertUsageForCreatedJob(any())
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when the enforcement flag lookup fails")
  fun createJobSkipsPassiveUsageTrackingWhenFlagLookupFails() {
    val organizationId = UUID.randomUUID()
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId)
    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenThrow(RuntimeException("flag lookup failed"))

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(dataWorkerUsageService, never()).insertUsageForCreatedJob(any())
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when the workspace lookup fails")
  fun createJobSkipsPassiveUsageTrackingWhenWorkspaceLookupFails() {
    stubSyncJobCreation()
    whenever(workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, false))
      .thenThrow(RuntimeException("workspace lookup failed"))

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(dataWorkerUsageService, never()).insertUsageForCreatedJob(any())
  }

  @Test
  @DisplayName("Passive usage tracking is skipped when the workspace has no organization")
  fun createJobSkipsPassiveUsageTrackingWhenWorkspaceHasNoOrganization() {
    stubSyncJobCreation()
    stubWorkspaceForConnection(organizationId = null)

    schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(dataWorkerUsageService, never()).insertUsageForCreatedJob(any())
    verifyNoInteractions(featureFlagClient)
  }

  @Test
  @DisplayName("Test refresh job creation")
  fun createRefreshJob() {
    whenever<Long?>(
      jobFactory.createRefresh(
        eq(CONNECTION_ID),
        any(),
      ),
    ).thenReturn(JOB_ID)
    whenever<StandardSync?>(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(mock())
    whenever<Job?>(jobPersistence.getJob(JOB_ID))
      .thenReturn(job)
    whenever<JobInfoRead?>(jobConverter.getJobInfoRead(job))
      .thenReturn(sampleJobInfoRead(JOB_ID))
    whenever(streamRefreshesHandler.getRefreshesForConnection(CONNECTION_ID))
      .thenReturn(
        listOf(
          StreamRefresh(UUID.randomUUID(), CONNECTION_ID, "name", "namespace", null, RefreshType.TRUNCATE),
        ),
      )

    val output = schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(jobFactory).createRefresh(eq(CONNECTION_ID), any())
    Assertions.assertThat(output.job.id).isEqualTo(JOB_ID)
  }

  @Test
  @DisplayName("Test reset job creation")
  fun createResetJob() {
    whenever<StandardSyncOperation?>(operationService.getStandardSyncOperation(WEBHOOK_OPERATION_ID)).thenReturn(WEBHOOK_OPERATION)
    val standardSync =
      StandardSync().withDestinationId(DESTINATION_ID).withOperationIds(listOf(WEBHOOK_OPERATION_ID)).withSourceId(SOURCE_ID)
    whenever<StandardSync?>(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(standardSync)
    val source =
      SourceConnection()
        .withSourceId(SOURCE_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
    whenever<SourceConnection?>(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(source)
    val destination =
      DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withConfiguration(emptyObject())
    whenever<DestinationConnection?>(destinationService.getDestinationConnection(DESTINATION_ID)).thenReturn(destination)
    val destinationDefinition = StandardDestinationDefinition()
    val destinationVersion = Version(DESTINATION_PROTOCOL_VERSION)
    val actorDefinitionVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withProtocolVersion(destinationVersion.serialize())
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      ),
    ).thenReturn(actorDefinitionVersion)
    whenever(destinationService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID))
      .thenReturn(destinationDefinition)
    val streamsToReset = listOf(STREAM_DESCRIPTOR1, STREAM_DESCRIPTOR2)
    whenever(streamResetPersistence.getStreamResets(CONNECTION_ID)).thenReturn(streamsToReset)

    whenever(
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
      ),
    ).thenReturn(Optional.of<Long>(JOB_ID))

    whenever<Job?>(jobPersistence.getJob(JOB_ID))
      .thenReturn(job)
    whenever<JobInfoRead?>(jobConverter.getJobInfoRead(job))
      .thenReturn(sampleJobInfoRead(JOB_ID))

    val output = schedulerHandler.createJob(JobCreate(connectionId = CONNECTION_ID, isScheduled = true))

    verify(oAuthConfigSupplier).injectDestinationOAuthParameters(any(), any(), any(), any())

    verify(actorDefinitionVersionHelper)
      .getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID)

    Assertions.assertThat(output.job.id).isEqualTo(JOB_ID)
  }

  @Test
  fun testCheckSourceConnectionFromSourceId() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceIdRequestBody(sourceId = source.sourceId)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)
    whenever<SourceConnection?>(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        null,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    schedulerHandler.checkSourceConnectionFromSourceId(request)

    verify(sourceService).getSourceConnection(source.sourceId)
    verify(synchronousSchedulerClient).createSourceCheckConnectionJob(source, sourceVersion, false, null)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)
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

    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId)).thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)).thenReturn(sourceVersion)
    whenever(synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null)).thenReturn(sourceResponse)
    doReturn(emptySynchronousJobRead()).whenever(jobConverter).getSynchronousJobRead(sourceResponse)
    whenever(destinationService.getDestinationConnection(destination.destinationId)).thenReturn(destination)
    whenever(destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)).thenReturn(destinationDefinition)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId),
    ).thenReturn(destinationVersion)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null),
    ).thenReturn(destinationResponse)
    doReturn(emptySynchronousJobRead()).whenever(jobConverter).getSynchronousJobRead(destinationResponse)

    schedulerHandler.checkSourceConnectionFromSourceId(sourceRequest)
    schedulerHandler.checkDestinationConnectionFromDestinationId(destinationRequest)

    verify(jsonSchemaValidator, times(2)).ensure(CONNECTOR_SPECIFICATION.connectionSpecification, source.configuration)
    val sourcePromotionOrder = org.mockito.Mockito.inOrder(synchronousSchedulerClient, sourceService)
    sourcePromotionOrder.verify(synchronousSchedulerClient).createSourceCheckConnectionJob(source, sourceVersion, false, null)
    sourcePromotionOrder.verify(sourceService).promoteSourceFromDraft(source.sourceId)
    val destinationPromotionOrder = org.mockito.Mockito.inOrder(synchronousSchedulerClient, destinationService)
    destinationPromotionOrder
      .verify(synchronousSchedulerClient)
      .createDestinationCheckConnectionJob(destination, destinationVersion, false, null)
    destinationPromotionOrder.verify(destinationService).promoteDestinationFromDraft(destination.destinationId)
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

    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId)).thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)).thenReturn(sourceVersion)
    whenever(synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null)).thenReturn(sourceResponse)
    doReturn(emptySynchronousJobRead()).whenever(jobConverter).getSynchronousJobRead(sourceResponse)
    whenever(destinationService.getDestinationConnection(destination.destinationId)).thenReturn(destination)
    whenever(destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)).thenReturn(destinationDefinition)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId),
    ).thenReturn(destinationVersion)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null),
    ).thenReturn(destinationResponse)
    doReturn(emptySynchronousJobRead()).whenever(jobConverter).getSynchronousJobRead(destinationResponse)

    schedulerHandler.checkSourceConnectionFromSourceId(SourceIdRequestBody(sourceId = source.sourceId))
    schedulerHandler.checkDestinationConnectionFromDestinationId(DestinationIdRequestBody(destinationId = destination.destinationId))

    verify(jsonSchemaValidator, times(2)).ensure(secretSpec.connectionSpecification, validationConfig)
    verify(synchronousSchedulerClient).createSourceCheckConnectionJob(source, sourceVersion, false, null)
    verify(synchronousSchedulerClient).createDestinationCheckConnectionJob(destination, destinationVersion, false, null)
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

    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId)).thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)).thenReturn(sourceVersion)
    whenever(synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, false, null)).thenReturn(sourceResponse)
    doReturn(emptySynchronousJobRead()).whenever(jobConverter).getSynchronousJobRead(sourceResponse)
    whenever(destinationService.getDestinationConnection(destination.destinationId)).thenReturn(destination)
    whenever(destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)).thenReturn(destinationDefinition)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId),
    ).thenReturn(destinationVersion)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, false, null),
    ).thenReturn(destinationResponse)
    doReturn(emptySynchronousJobRead()).whenever(jobConverter).getSynchronousJobRead(destinationResponse)

    schedulerHandler.checkSourceConnectionFromSourceId(SourceIdRequestBody(sourceId = source.sourceId))
    schedulerHandler.checkDestinationConnectionFromDestinationId(DestinationIdRequestBody(destinationId = destination.destinationId))

    verify(sourceService, never()).promoteSourceFromDraft(any())
    verify(destinationService, never()).promoteDestinationFromDraft(any())
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
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        null,
      ),
    ).thenReturn(sourceVersion)

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever(
      secretsRepositoryWriter.createEphemeralFromConfig(eq(processed), anyOrNull()),
    ).thenReturn(source.getConfiguration())

    whenever(
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        RESOURCE_REQUIREMENT,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig)

    verify(synchronousSchedulerClient)
      .createSourceCheckConnectionJob(source, sourceVersion, false, RESOURCE_REQUIREMENT)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, null)
  }

  @Test
  fun testCheckSourceConnectionFromUpdate() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val sourceUpdate =
      SourceUpdate(name = source.name, sourceId = source.sourceId, connectionConfiguration = source.getConfiguration())
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)
    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(
      configurationUpdate.source(
        source.sourceId,
        source.name,
        sourceUpdate.connectionConfiguration,
      ),
    ).thenReturn(source)
    val submittedSource =
      SourceConnection()
        .withSourceId(source.sourceId)
        .withSourceDefinitionId(source.sourceDefinitionId)
        .withConfiguration(source.getConfiguration())
        .withWorkspaceId(source.workspaceId)
    whenever(
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        submittedSource,
        sourceVersion,
        false,
        null,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever(secretsRepositoryWriter.createEphemeralFromConfig(eq(processed), anyOrNull()))
      .thenReturn(source.getConfiguration())

    schedulerHandler.checkSourceConnectionFromSourceIdForUpdate(sourceUpdate)

    verify(jsonSchemaValidator)
      .ensure(CONNECTOR_SPECIFICATION.getConnectionSpecification(), source.getConfiguration())
    verify(synchronousSchedulerClient)
      .createSourceCheckConnectionJob(submittedSource, sourceVersion, false, null)
    verify(actorDefinitionVersionHelper, times(2))
      .getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)
  }

  @Test
  fun testCheckDestinationConnectionFromDestinationId() {
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID())
    val request = DestinationIdRequestBody(destinationId = destination.destinationId)

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.destinationDefinitionId)
    whenever<StandardDestinationDefinition?>(destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId))
      .thenReturn(destinationDefinition)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      ),
    ).thenReturn(destinationVersion)
    whenever<DestinationConnection?>(destinationService.getDestinationConnection(destination.destinationId)).thenReturn(destination)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        null,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    schedulerHandler.checkDestinationConnectionFromDestinationId(request)

    verify(destinationService).getDestinationConnection(destination.destinationId)
    verify(actorDefinitionVersionHelper)
      .getDestinationVersion(destinationDefinition, destination.workspaceId, destination.destinationId)
    verify(synchronousSchedulerClient)
      .createDestinationCheckConnectionJob(destination, destinationVersion, false, null)
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
    whenever(
      destinationService.getStandardDestinationDefinition(
        destination.destinationDefinitionId,
      ),
    ).thenReturn(destinationDefinition)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        null,
      ),
    ).thenReturn(destinationVersion)

    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        null,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    val processed =
      processConfigSecrets(
        destination.getConfiguration(),
        destinationVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever(secretsRepositoryWriter.createEphemeralFromConfig(eq(processed), anyOrNull()))
      .thenReturn(destination.getConfiguration())

    schedulerHandler.checkDestinationConnectionFromDestinationCreate(destinationCoreConfig)

    verify(synchronousSchedulerClient)
      .createDestinationCheckConnectionJob(destination, destinationVersion, false, null)
    verify(actorDefinitionVersionHelper)
      .getDestinationVersion(eq(destinationDefinition), eq(destination.workspaceId), anyOrNull())
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
    whenever(
      destinationService.getStandardDestinationDefinition(
        destination.destinationDefinitionId,
      ),
    ).thenReturn(destinationDefinition)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withSpec(CONNECTOR_SPECIFICATION)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      ),
    ).thenReturn(destinationVersion)
    whenever(destinationService.getDestinationConnection(destination.destinationId))
      .thenReturn(destination)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        RESOURCE_REQUIREMENT,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)
    whenever(destinationService.getDestinationConnection(destination.destinationId))
      .thenReturn(destination)
    whenever(
      configurationUpdate.destination(
        destination.destinationId,
        destination.name,
        destinationUpdate.connectionConfiguration,
      ),
    ).thenReturn(destination)
    val submittedDestination =
      DestinationConnection()
        .withDestinationId(destination.destinationId)
        .withDestinationDefinitionId(destination.destinationDefinitionId)
        .withConfiguration(destination.getConfiguration())
        .withWorkspaceId(destination.workspaceId)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        submittedDestination,
        destinationVersion,
        false,
        RESOURCE_REQUIREMENT,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    val processed =
      processConfigSecrets(
        destination.getConfiguration(),
        destinationVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever(
      secretsRepositoryWriter.createEphemeralFromConfig(eq(processed), anyOrNull()),
    ).thenReturn(destination.getConfiguration())

    schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(destinationUpdate)

    verify(jsonSchemaValidator)
      .ensure(CONNECTOR_SPECIFICATION.getConnectionSpecification(), destination.getConfiguration())
    verify(actorDefinitionVersionHelper, times(2))
      .getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destination.destinationId,
      )
    verify(synchronousSchedulerClient)
      .createDestinationCheckConnectionJob(submittedDestination, destinationVersion, false, RESOURCE_REQUIREMENT)
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
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever(
      secretsRepositoryWriter.createEphemeralFromConfig(eq(processed), anyOrNull()),
    ).thenReturn(source.getConfiguration())

    whenever(
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        null,
      ),
    ).thenReturn(checkResponse)

    val checkConnectionRead = schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig)
    org.junit.jupiter.api.Assertions
      .assertEquals(expectedCheckConnectionRead, checkConnectionRead)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceId() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)

    val mockWorkspace =
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(false)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

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
    whenever(catalogService.getActorCatalogById(any())).thenReturn(actorCatalog)

    val connectionRead = connectionRead(sourceId = source.sourceId)
    val connectionReadList = ConnectionReadList(connections = listOf(connectionRead))
    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(connectionReadList)

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
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)
    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(
      catalogService.getActorCatalog(any(), any(), any()),
    ).thenReturn(Optional.empty<ActorCatalog>())
    whenever(
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        RESOURCE_REQUIREMENT,
        WorkloadPriority.HIGH,
      ),
    ).thenReturn(discoverResponse)

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.catalogId, discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify(sourceService).getSourceConnection(source.sourceId)
    verify(catalogService)
      .getActorCatalog(eq(request.sourceId), eq(SOURCE_DOCKER_TAG), any())

    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, RESOURCE_REQUIREMENT, WorkloadPriority.HIGH)
  }

  @Test
  fun `draft source cannot be discovered`() {
    val source = SourceHelpers.generateSource(UUID.randomUUID()).withIsDraft(true)
    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)

    assertThrows<ActorNotReadyProblem> {
      schedulerHandler.discoverSchemaForSourceFromSourceId(SourceDiscoverSchemaRequestBody(sourceId = source.sourceId))
    }

    verifyNoInteractions(synchronousSchedulerClient)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdCachedCatalog() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)

    val mockWorkspace =
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(false)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

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
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(SOURCE_DOCKER_TAG))
    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(thisCatalogId)
    whenever(
      catalogService.getActorCatalog(any(), any(), any()),
    ).thenReturn(Optional.of(actorCatalog))

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.catalogId, discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify(sourceService).getSourceConnection(source.sourceId)
    verify(catalogService)
      .getActorCatalog(eq(request.sourceId), any(), any())

    verify(catalogService, never()).writeActorCatalogWithFetchEvent(anyOrNull<AirbyteCatalog>(), anyOrNull(), anyOrNull(), anyOrNull())

    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)
    verify(synchronousSchedulerClient, never()).createDiscoverSchemaJob(any(), any(), any(), any(), any())
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdDisableCache() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId, disableCache = true)

    val mockWorkspace =
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(false)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

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
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)
    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(discoveredCatalogId)
    whenever(catalogService.getActorCatalogById(discoveredCatalogId)).thenReturn(actorCatalog)
    whenever(
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      ),
    ).thenReturn(discoverResponse)

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify(sourceService).getSourceConnection(source.sourceId)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdFailed() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)

    val mockWorkspace =
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(false)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)
    whenever<SourceConnection?>(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    whenever(
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<UUID>)

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceId(request)

    org.junit.jupiter.api.Assertions
      .assertNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertFalse(actual.jobInfo!!.succeeded)
    verify(sourceService).getSourceConnection(source.sourceId)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, source.sourceId)
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceIdDisabledWorkspace() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody(sourceId = source.sourceId)
    whenever(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
    val mockWorkspace =
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(true)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

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
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(true)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

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
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(false)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

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
    whenever(catalogService.getActorCatalogById(any()))
      .thenReturn(actorCatalog)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    whenever(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        null,
      ),
    ).thenReturn(sourceVersion)
    whenever(
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      ),
    ).thenReturn(discoverResponse)

    val processed =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever(
      secretsRepositoryWriter.createEphemeralFromConfig(eq(processed), anyOrNull()),
    ).thenReturn(source.getConfiguration())

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig)

    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.catalogId, discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.jobInfo!!.succeeded)
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.workspaceId, null)
  }

  @Test
  fun testDiscoverSchemaForSourceFromSourceCreateFailed() {
    val source =
      SourceConnection()
        .withSourceDefinitionId(SOURCE.sourceDefinitionId)
        .withConfiguration(SOURCE.getConfiguration())
        .withWorkspaceId(SOURCE.workspaceId)

    val mockWorkspace =
      mock<StandardWorkspace>().also {
        whenever(it.tombstone).thenReturn(false)
      }
    whenever(workspaceService.getStandardWorkspaceNoSecrets(source.workspaceId, includeTombstone = true)).thenReturn(mockWorkspace)

    val sourceCoreConfig =
      SourceCoreConfig(
        sourceDefinitionId = source.sourceDefinitionId,
        connectionConfiguration = source.getConfiguration(),
        workspaceId = source.workspaceId,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.sourceDefinitionId)
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, null))
      .thenReturn(sourceVersion)
    whenever(
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<UUID>)

    val processedSourceConfig =
      processConfigSecrets(
        source.getConfiguration(),
        sourceVersion.getSpec().getConnectionSpecification(),
        null,
      )
    whenever<JsonNode?>(
      secretsRepositoryWriter.createEphemeralFromConfig(
        eq(
          processedSourceConfig,
        ),
        anyOrNull(),
      ),
    ).thenReturn(source.getConfiguration())

    val actual = schedulerHandler.discoverSchemaForSourceFromSourceCreate(sourceCoreConfig)

    org.junit.jupiter.api.Assertions
      .assertNull(actual.catalog)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.jobInfo)
    org.junit.jupiter.api.Assertions
      .assertFalse(actual.jobInfo!!.succeeded)
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
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

    whenever<ManualOperationResult?>(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)

    doReturn(sampleJobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId))

    verify(eventRunner).startNewManualSync(connectionId)
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
    whenever(connectionService.getStandardSync(connectionId)).thenReturn(sync)
    whenever(sourceService.getSourceConnection(sourceId)).thenReturn(SourceConnection().withSourceId(sourceId).withIsDraft(true))
    whenever(destinationService.getDestinationConnection(destinationId)).thenReturn(
      DestinationConnection().withDestinationId(destinationId).withIsDraft(false),
    )

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

    verifyNoInteractions(eventRunner)
  }

  @Test
  fun testSyncConnectionUsesExistingManualSyncPathWhenDataWorkerEnforcementDisabled() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenReturn(false)
    whenever(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)
    doReturn(job)
      .whenever(jobPersistence)
      .getJob(jobId)
    doReturn(sampleJobInfoRead(jobId))
      .whenever(jobConverter)
      .getJobInfoRead(job)

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId)

    verify(eventRunner).startNewManualSync(connectionId)
    verify(eventRunner, never()).startNewManualSyncAndWaitForJobId(connectionId)
  }

  @Test
  fun testSyncConnectionWaitsOnlyForJobIdWhenRequestedEvenIfDataWorkerEnforcementDisabled() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenReturn(false)
    whenever(eventRunner.startNewManualSyncAndWaitForJobId(connectionId))
      .thenReturn(manualOperationResult)
    doReturn(job)
      .whenever(jobPersistence)
      .getJob(jobId)
    doReturn(sampleJobInfoRead(jobId))
      .whenever(jobConverter)
      .getJobInfoRead(job)

    val result = schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId, returnAfterJobId = true)

    verify(eventRunner).startNewManualSyncAndWaitForJobId(connectionId)
    verify(eventRunner, never()).startNewManualSync(connectionId)
    Assertions.assertThat(result.job.id).isEqualTo(jobId)
  }

  @Test
  fun testSyncConnectionWaitsOnlyForJobIdWhenDataWorkerCapacityIsEnforced() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenReturn(true)
    whenever(eventRunner.startNewManualSyncAndWaitForJobId(connectionId))
      .thenReturn(manualOperationResult)
    doReturn(job)
      .whenever(jobPersistence)
      .getJob(jobId)
    doReturn(sampleJobInfoRead(jobId))
      .whenever(jobConverter)
      .getJobInfoRead(job)

    val result = schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId)

    verify(eventRunner).startNewManualSyncAndWaitForJobId(connectionId)
    verify(eventRunner, never()).startNewManualSync(connectionId)
    Assertions.assertThat(result.job.id).isEqualTo(jobId)
  }

  @Test
  fun testSyncConnectionUsesExistingManualSyncPathWhenNoOrganizationIdIsSupplied() {
    val connectionId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    whenever(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)
    doReturn(job)
      .whenever(jobPersistence)
      .getJob(jobId)
    doReturn(sampleJobInfoRead(jobId))
      .whenever(jobConverter)
      .getJobInfoRead(job)

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId))

    verify(eventRunner).startNewManualSync(connectionId)
    verify(eventRunner, never()).startNewManualSyncAndWaitForJobId(connectionId)
    verifyNoInteractions(featureFlagClient)
  }

  @Test
  fun testSyncConnectionUsesExistingManualSyncPathWhenDataWorkerEnforcementFlagLookupFails() {
    val connectionId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    whenever(featureFlagClient.boolVariation(EnforceDataWorkerCapacity, Organization(organizationId)))
      .thenThrow(RuntimeException("flag lookup failed"))
    whenever(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)
    doReturn(job)
      .whenever(jobPersistence)
      .getJob(jobId)
    doReturn(sampleJobInfoRead(jobId))
      .whenever(jobConverter)
      .getJobInfoRead(job)

    schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId), organizationId)

    verify(eventRunner).startNewManualSync(connectionId)
    verify(eventRunner, never()).startNewManualSyncAndWaitForJobId(connectionId)
  }

  @Test
  fun testSyncConnectionFailWithOtherSyncRunning() {
    val connectionId = UUID.randomUUID()

    val manualOperationResult = ManualOperationResult("another sync running", null, ErrorCode.WORKFLOW_RUNNING)

    whenever<ManualOperationResult?>(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)

    org.junit.jupiter.api.Assertions.assertThrows(
      ValueConflictKnownException::class.java,
    ) { schedulerHandler.syncConnection(ConnectionIdRequestBody(connectionId = connectionId)) }
  }

  @Test
  fun disabledSyncThrows() {
    val connectionId = UUID.randomUUID()
    whenever<StandardSync?>(connectionService.getStandardSync(connectionId))
      .thenReturn(StandardSync().withStatus(StandardSync.Status.INACTIVE))
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
    whenever<StandardSync?>(connectionService.getStandardSync(connectionId))
      .thenReturn(StandardSync().withStatus(StandardSync.Status.DEPRECATED))
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
    whenever<StandardSync?>(connectionService.getStandardSync(connectionId))
      .thenReturn(StandardSync().withStatus(StandardSync.Status.LOCKED))
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
    whenever(connectionService.getAllStreamsForConnection(connectionId))
      .thenReturn(streamDescriptors)

    whenever<ManualOperationResult?>(eventRunner.resetConnection(connectionId, streamDescriptors))
      .thenReturn(manualOperationResult)

    doReturn(sampleJobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler.resetConnection(ConnectionIdRequestBody(connectionId = connectionId))

    verify(eventRunner).resetConnection(connectionId, streamDescriptors)
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

    whenever<ManualOperationResult?>(eventRunner.resetConnection(connectionId, streamDescriptors))
      .thenReturn(manualOperationResult)

    doReturn(sampleJobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler
      .resetConnectionStream(connectionStreamRequestBody)

    verify(eventRunner).resetConnection(connectionId, streamDescriptors)
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

    whenever(connectionService.getAllStreamsForConnection(connectionId))
      .thenReturn(streamDescriptors)
    whenever<ManualOperationResult?>(eventRunner.resetConnection(connectionId, streamDescriptors))
      .thenReturn(manualOperationResult)

    doReturn(sampleJobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler
      .resetConnectionStream(connectionStreamRequestBody)

    verify(eventRunner).resetConnection(connectionId, streamDescriptors)
  }

  @Test
  fun testCancelJob() {
    whenever<Job?>(jobPersistence.getJob(JOB_ID)).thenReturn(job)

    val manualOperationResult = ManualOperationResult(null, JOB_ID, null)

    whenever<ManualOperationResult?>(eventRunner.startNewCancellation(SCOPE))
      .thenReturn(manualOperationResult)
    val jobInfo =
      sampleJobInfoRead(JOB_ID, createdAt = 123L, updatedAt = 321L)
    doReturn(jobInfo).whenever(jobConverter).getJobInfoRead(any())

    schedulerHandler.cancelJob(JobIdRequestBody(id = JOB_ID))

    verify(eventRunner).startNewCancellation(SCOPE)
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
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)

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
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(ConnectionReadList(connections = listOf(CONNECTION)))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
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
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)

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
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(ConnectionReadList(connections = listOf(CONNECTION)))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
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
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockRemoveStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithDiff =
      catalogConverter
        .toApi(clone(airbyteCatalog), sourceVersion)
        .copy(streams = emptyList())

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(ConnectionReadList(connections = listOf(CONNECTION)))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
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
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)
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
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(ConnectionReadList(connections = listOf(CONNECTION)))
    val request =
      SourceAutoPropagateChange(
        sourceId = source.sourceId,
        workspaceId = source.workspaceId,
        catalogId = discoveredSourceId,
        catalog = catalogWithNewColumnAndStream,
      )

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
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
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)

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
    whenever<StandardWorkspace?>(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler, times(0))
      .updateConnection(any<ConnectionUpdate>(), any<String>(), anyBoolean())
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
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(workspaceId).withNotificationSettings(notificationSettings))
    whenever(sourceService.getSourceConnection(sourceId))
      .thenReturn(SourceConnection().withSourceId(sourceId))
    val connectionRead =
      connectionRead(
        connectionId = connectionId,
        sourceId = sourceId,
        syncCatalog = configuredCatalog,
        nonBreakingChangesPreference = NonBreakingChangesPreference.PROPAGATE_COLUMNS,
      )
    whenever(connectionsHandler.listConnectionsForSource(sourceId, false))
      .thenReturn(ConnectionReadList(connections = listOf(connectionRead)))
    whenever(connectionsHandler.getConnectionAirbyteCatalog(connectionId))
      .thenReturn(Optional.of(oldCatalog))

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
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(diff)

    val spySchedulerHandler = spy(schedulerHandler)
    val request =
      SourceAutoPropagateChange(sourceId = sourceId, workspaceId = workspaceId, catalogId = catalogId, catalog = newCatalog)
    spySchedulerHandler.applySchemaChangeForSource(request)
    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
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
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(sourceVersion)

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockEmptyDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    // NOTE: this is just a clone of the original catalog.
    val discoveredCatalog =
      catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)

    val workspaceId = source.workspaceId
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(ConnectionReadList(connections = listOf(CONNECTION)))
    val request =
      SourceAutoPropagateChange(sourceId = source.sourceId, workspaceId = workspaceId, catalogId = discoveredSourceId, catalog = discoveredCatalog)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  private fun mockSourceForDiscoverJob(
    source: SourceConnection,
    sourceDefinition: StandardSourceDefinition,
  ) {
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.sourceDefinitionId))
      .thenReturn(sourceDefinition)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.workspaceId,
        source.sourceId,
      ),
    ).thenReturn(
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG),
    )
    whenever<SourceConnection?>(sourceService.getSourceConnection(source.sourceId)).thenReturn(source)
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

    whenever(catalogService.getActorCatalogById(any())).thenReturn(actorCatalog)
    whenever(
      synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        false,
        null,
        WorkloadPriority.HIGH,
      ),
    ).thenReturn(discoverResponse)
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
    whenever(connectionsHandler.listConnectionsForSource(source.sourceId, false))
      .thenReturn(connectionReadList)
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
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
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
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
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
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
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
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
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
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
  }

  private fun mockEmptyDiff() {
    val emptyDiff = CatalogDiff(transforms = emptyList())
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(emptyDiff)
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

    val WEBHOOK_OPERATION: StandardSyncOperation? =
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
