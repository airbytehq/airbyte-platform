/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.AttemptInfoReadLogs
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.ConnectionStreamRequestBody
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.DestinationCoreConfig
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationSyncMode
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.FailureOrigin
import io.airbyte.api.model.generated.FailureType
import io.airbyte.api.model.generated.FieldAdd
import io.airbyte.api.model.generated.FieldTransform
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobCreate
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.LogFormatType
import io.airbyte.api.model.generated.NonBreakingChangesPreference
import io.airbyte.api.model.generated.SourceAutoPropagateChange
import io.airbyte.api.model.generated.SourceCoreConfig
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.model.generated.StreamTransform
import io.airbyte.api.model.generated.StreamTransformUpdateStream
import io.airbyte.api.model.generated.SyncMode
import io.airbyte.api.model.generated.SynchronousJobRead
import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.lang.Exceptions.toRuntime
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.StreamRefreshesHandler
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
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.instance.configs.jooq.generated.enums.RefreshType
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.featureflag.FeatureFlagClient
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
import io.airbyte.validation.json.JsonValidationException
import jakarta.validation.Valid
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
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
import java.io.IOException
import java.net.URI
import java.nio.file.Path
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

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

  @BeforeEach
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
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
      )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @DisplayName("Test job creation")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun createJob(isScheduled: Boolean) {
    whenever<Long?>(jobFactory.createSync(CONNECTION_ID, isScheduled))
      .thenReturn(JOB_ID)
    whenever<StandardSync?>(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(mock())
    whenever<Job?>(jobPersistence.getJob(JOB_ID))
      .thenReturn(job)
    whenever<JobInfoRead?>(jobConverter.getJobInfoRead(job))
      .thenReturn(JobInfoRead().job(JobRead().id(JOB_ID)))

    val output = schedulerHandler.createJob(JobCreate().connectionId(CONNECTION_ID).isScheduled(isScheduled))

    verify(jobFactory).createSync(CONNECTION_ID, isScheduled)
    Assertions.assertThat(output.getJob().getId()).isEqualTo(JOB_ID)
  }

  @Test
  @DisplayName("Test refresh job creation")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
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
      .thenReturn(JobInfoRead().job(JobRead().id(JOB_ID)))
    whenever(streamRefreshesHandler.getRefreshesForConnection(CONNECTION_ID))
      .thenReturn(
        listOf(
          StreamRefresh(UUID.randomUUID(), CONNECTION_ID, "name", "namespace", null, RefreshType.TRUNCATE),
        ),
      )

    val output = schedulerHandler.createJob(JobCreate().connectionId(CONNECTION_ID).isScheduled(true))

    verify(jobFactory).createRefresh(eq(CONNECTION_ID), any())
    Assertions.assertThat(output.getJob().getId()).isEqualTo(JOB_ID)
  }

  @Test
  @DisplayName("Test reset job creation")
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
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
      .thenReturn(JobInfoRead().job(JobRead().id(JOB_ID)))

    val output = schedulerHandler.createJob(JobCreate().connectionId(CONNECTION_ID).isScheduled(true))

    verify(oAuthConfigSupplier).injectDestinationOAuthParameters(any(), any(), any(), any())

    verify(actorDefinitionVersionHelper)
      .getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID)

    Assertions.assertThat(output.getJob().getId()).isEqualTo(JOB_ID)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCheckSourceConnectionFromSourceId() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceIdRequestBody().sourceId(source.getSourceId())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)
    whenever<SourceConnection?>(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
    whenever(
      synchronousSchedulerClient.createSourceCheckConnectionJob(
        source,
        sourceVersion,
        false,
        null,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    schedulerHandler.checkSourceConnectionFromSourceId(request)

    verify(sourceService).getSourceConnection(source.getSourceId())
    verify(synchronousSchedulerClient).createSourceCheckConnectionJob(source, sourceVersion, false, null)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId())
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCheckSourceConnectionFromSourceCreate() {
    val source =
      SourceConnection()
        .withWorkspaceId(SOURCE.getWorkspaceId())
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration())

    val sourceCoreConfig =
      SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
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
    whenever(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
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
        source.getWorkspaceId(),
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
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), null)
  }

  @Test
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testCheckSourceConnectionFromUpdate() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val sourceUpdate =
      SourceUpdate()
        .name(source.getName())
        .sourceId(source.getSourceId())
        .connectionConfiguration(source.getConfiguration())
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
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
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)
    whenever(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
    whenever(
      configurationUpdate.source(
        source.getSourceId(),
        source.getName(),
        sourceUpdate.getConnectionConfiguration(),
      ),
    ).thenReturn(source)
    val submittedSource =
      SourceConnection()
        .withSourceId(source.getSourceId())
        .withSourceDefinitionId(source.getSourceDefinitionId())
        .withConfiguration(source.getConfiguration())
        .withWorkspaceId(source.getWorkspaceId())
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
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId())
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testCheckDestinationConnectionFromDestinationId() {
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID())
    val request = DestinationIdRequestBody().destinationId(destination.getDestinationId())

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.getDestinationDefinitionId())
    whenever<StandardDestinationDefinition?>(destinationService.getStandardDestinationDefinition(destination.getDestinationDefinitionId()))
      .thenReturn(destinationDefinition)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(DESTINATION_DOCKER_REPO)
        .withDockerImageTag(DESTINATION_DOCKER_TAG)
        .withProtocolVersion(DESTINATION_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.getWorkspaceId(),
        destination.getDestinationId(),
      ),
    ).thenReturn(destinationVersion)
    whenever<DestinationConnection?>(destinationService.getDestinationConnection(destination.getDestinationId())).thenReturn(destination)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        null,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)

    schedulerHandler.checkDestinationConnectionFromDestinationId(request)

    verify(destinationService).getDestinationConnection(destination.getDestinationId())
    verify(actorDefinitionVersionHelper)
      .getDestinationVersion(destinationDefinition, destination.getWorkspaceId(), destination.getDestinationId())
    verify(synchronousSchedulerClient)
      .createDestinationCheckConnectionJob(destination, destinationVersion, false, null)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testCheckDestinationConnectionFromDestinationCreate() {
    val destination =
      DestinationConnection()
        .withWorkspaceId(DESTINATION.getWorkspaceId())
        .withDestinationDefinitionId(DESTINATION.getDestinationDefinitionId())
        .withConfiguration(DESTINATION.getConfiguration())

    val destinationCoreConfig =
      DestinationCoreConfig()
        .workspaceId(destination.getWorkspaceId())
        .destinationDefinitionId(destination.getDestinationDefinitionId())
        .connectionConfiguration(destination.getConfiguration())

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.getDestinationDefinitionId())
    whenever(
      destinationService.getStandardDestinationDefinition(
        destination.getDestinationDefinitionId(),
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
        destination.getWorkspaceId(),
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
      .getDestinationVersion(eq(destinationDefinition), eq(destination.getWorkspaceId()), anyOrNull())
  }

  @Test
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testCheckDestinationConnectionFromUpdate() {
    val destination = DestinationHelpers.generateDestination(UUID.randomUUID())
    val destinationUpdate =
      DestinationUpdate()
        .name(destination.getName())
        .destinationId(destination.getDestinationId())
        .connectionConfiguration(destination.getConfiguration())
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destination.getDestinationDefinitionId())
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
        destination.getDestinationDefinitionId(),
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
        destination.getWorkspaceId(),
        destination.getDestinationId(),
      ),
    ).thenReturn(destinationVersion)
    whenever(destinationService.getDestinationConnection(destination.getDestinationId()))
      .thenReturn(destination)
    whenever(
      synchronousSchedulerClient.createDestinationCheckConnectionJob(
        destination,
        destinationVersion,
        false,
        RESOURCE_REQUIREMENT,
      ),
    ).thenReturn(jobResponse as SynchronousResponse<StandardCheckConnectionOutput>)
    whenever(destinationService.getDestinationConnection(destination.getDestinationId()))
      .thenReturn(destination)
    whenever(
      configurationUpdate.destination(
        destination.getDestinationId(),
        destination.getName(),
        destinationUpdate.getConnectionConfiguration(),
      ),
    ).thenReturn(destination)
    val submittedDestination =
      DestinationConnection()
        .withDestinationId(destination.getDestinationId())
        .withDestinationDefinitionId(destination.getDestinationDefinitionId())
        .withConfiguration(destination.getConfiguration())
        .withWorkspaceId(destination.getWorkspaceId())
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
        destination.getWorkspaceId(),
        destination.getDestinationId(),
      )
    verify(synchronousSchedulerClient)
      .createDestinationCheckConnectionJob(submittedDestination, destinationVersion, false, RESOURCE_REQUIREMENT)
  }

  @ParameterizedTest
  @MethodSource("provideArguments")
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
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
    val jobSucceeded = !exceptionWouldBeThrown && connectorJobOutput.getFailureReason() == null
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
        .withWorkspaceId(SOURCE.getWorkspaceId())
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration())

    val sourceCoreConfig =
      SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
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
        source.getWorkspaceId(),
        source.getSourceId(),
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

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDiscoverSchemaForSourceFromSourceId(enabled: Boolean) {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId())

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

    val connectionRead = ConnectionRead()
    val connectionReadList = ConnectionReadList().connections(listOf(connectionRead))
    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(connectionReadList)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
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
    whenever(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)
    whenever(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
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
      .assertNotNull(actual.getCatalog())
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.getCatalogId(), discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.getJobInfo())
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.getJobInfo().getSucceeded())
    verify(sourceService).getSourceConnection(source.getSourceId())
    verify(catalogService)
      .getActorCatalog(eq(request.sourceId), eq(SOURCE_DOCKER_TAG), any())

    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId())
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, RESOURCE_REQUIREMENT, WorkloadPriority.HIGH)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDiscoverSchemaForSourceFromSourceIdCachedCatalog(enabled: Boolean) {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId())

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
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(ActorDefinitionVersion().withDockerRepository(SOURCE_DOCKER_REPO).withDockerImageTag(SOURCE_DOCKER_TAG))
    whenever(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
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
      .assertNotNull(actual.getCatalog())
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.getJobInfo())
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.getCatalogId(), discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.getJobInfo().getSucceeded())
    verify(sourceService).getSourceConnection(source.getSourceId())
    verify(catalogService)
      .getActorCatalog(eq(request.getSourceId()), any(), any())

    verify(catalogService, never()).writeActorCatalogWithFetchEvent(anyOrNull<AirbyteCatalog>(), anyOrNull(), anyOrNull(), anyOrNull())

    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId())
    verify(synchronousSchedulerClient, never()).createDiscoverSchemaJob(any(), any(), any(), any(), any())
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDiscoverSchemaForSourceFromSourceIdDisableCache(enabled: Boolean) {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId()).disableCache(true)

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
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)
    whenever(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
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
      .assertNotNull(actual.getCatalog())
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.getJobInfo())
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.getJobInfo().getSucceeded())
    verify(sourceService).getSourceConnection(source.getSourceId())
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId())
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testDiscoverSchemaForSourceFromSourceIdFailed(enabled: Boolean) {
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val request = SourceDiscoverSchemaRequestBody().sourceId(source.getSourceId())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)
    whenever<SourceConnection?>(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
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
      .assertNull(actual.getCatalog())
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.getJobInfo())
    org.junit.jupiter.api.Assertions
      .assertFalse(actual.getJobInfo().getSucceeded())
    verify(sourceService).getSourceConnection(source.getSourceId())
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), source.getSourceId())
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDiscoverSchemaForSourceFromSourceCreate() {
    val source =
      SourceConnection()
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration())
        .withWorkspaceId(SOURCE.getWorkspaceId())

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
      SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId())
    val actorCatalog =
      ActorCatalog()
        .withCatalog(jsonNode<AirbyteCatalog?>(airbyteCatalog))
        .withCatalogHash("")
        .withId(UUID.randomUUID())
    whenever(catalogService.getActorCatalogById(any()))
      .thenReturn(actorCatalog)

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
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
        source.getWorkspaceId(),
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
      .assertNotNull(actual.getCatalog())
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.getJobInfo())
    org.junit.jupiter.api.Assertions
      .assertEquals(actual.getCatalogId(), discoverResponse.output)
    org.junit.jupiter.api.Assertions
      .assertTrue(actual.getJobInfo().getSucceeded())
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
    verify(actorDefinitionVersionHelper)
      .getSourceVersion(sourceDefinition, source.getWorkspaceId(), null)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testDiscoverSchemaForSourceFromSourceCreateFailed() {
    val source =
      SourceConnection()
        .withSourceDefinitionId(SOURCE.getSourceDefinitionId())
        .withConfiguration(SOURCE.getConfiguration())
        .withWorkspaceId(SOURCE.getWorkspaceId())

    val sourceCoreConfig =
      SourceCoreConfig()
        .sourceDefinitionId(source.getSourceDefinitionId())
        .connectionConfiguration(source.getConfiguration())
        .workspaceId(source.getWorkspaceId())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG)
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
        .withSpec(CONNECTOR_SPECIFICATION)
    whenever<ActorDefinitionVersion?>(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), null))
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
      .assertNull(actual.getCatalog())
    org.junit.jupiter.api.Assertions
      .assertNotNull(actual.getJobInfo())
    org.junit.jupiter.api.Assertions
      .assertFalse(actual.getJobInfo().getSucceeded())
    verify(synchronousSchedulerClient)
      .createDiscoverSchemaJob(source, sourceVersion, false, null, WorkloadPriority.HIGH)
  }

  @Test
  @DisplayName("Test enum compatibility")
  fun testEnumCompatibility() {
    Assertions.assertThat(isCompatible<ConfigType, io.airbyte.api.model.generated.JobConfigType>()).isTrue()
  }

  @Test
  fun testEnumConversion() {
    org.junit.jupiter.api.Assertions.assertTrue(
      isCompatible<StandardCheckConnectionOutput.Status, CheckConnectionRead.StatusEnum>(),
    )
    org.junit.jupiter.api.Assertions.assertTrue(
      isCompatible<JobStatus, io.airbyte.api.model.generated.JobStatus>(),
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testSyncConnection() {
    val connectionId = UUID.randomUUID()

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    whenever<ManualOperationResult?>(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)

    doReturn(JobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler.syncConnection(ConnectionIdRequestBody().connectionId(connectionId))

    verify(eventRunner).startNewManualSync(connectionId)
  }

  @Test
  @Throws(IOException::class)
  fun testSyncConnectionFailWithOtherSyncRunning() {
    val connectionId = UUID.randomUUID()

    val manualOperationResult = ManualOperationResult("another sync running", null, ErrorCode.WORKFLOW_RUNNING)

    whenever<ManualOperationResult?>(eventRunner.startNewManualSync(connectionId))
      .thenReturn(manualOperationResult)

    org.junit.jupiter.api.Assertions.assertThrows(
      ValueConflictKnownException::class.java,
    ) { schedulerHandler.syncConnection(ConnectionIdRequestBody().connectionId(connectionId)) }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun disabledSyncThrows() {
    val connectionId = UUID.randomUUID()
    whenever<StandardSync?>(connectionService.getStandardSync(connectionId))
      .thenReturn(StandardSync().withStatus(StandardSync.Status.INACTIVE))
    org.junit.jupiter.api.Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      schedulerHandler.syncConnection(
        ConnectionIdRequestBody().connectionId(connectionId),
      )
    }
  }

  @Test
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun deprecatedSyncThrows() {
    val connectionId = UUID.randomUUID()
    whenever<StandardSync?>(connectionService.getStandardSync(connectionId))
      .thenReturn(StandardSync().withStatus(StandardSync.Status.DEPRECATED))
    org.junit.jupiter.api.Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      schedulerHandler.syncConnection(
        ConnectionIdRequestBody().connectionId(connectionId),
      )
    }
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testResetConnection() {
    val connectionId = UUID.randomUUID()

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)

    val streamDescriptors = listOf(STREAM_DESCRIPTOR)
    whenever(connectionService.getAllStreamsForConnection(connectionId))
      .thenReturn(streamDescriptors)

    whenever<ManualOperationResult?>(eventRunner.resetConnection(connectionId, streamDescriptors))
      .thenReturn(manualOperationResult)

    doReturn(JobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler.resetConnection(ConnectionIdRequestBody().connectionId(connectionId))

    verify(eventRunner).resetConnection(connectionId, streamDescriptors)
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testResetConnectionStream() {
    val connectionId = UUID.randomUUID()
    val streamName = "name"
    val streamNamespace = "namespace"

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)
    val streamDescriptors = listOf(StreamDescriptor().withName(streamName).withNamespace(streamNamespace))
    val connectionStreamRequestBody =
      ConnectionStreamRequestBody()
        .connectionId(connectionId)
        .streams(listOf(ConnectionStream().streamName(streamName).streamNamespace(streamNamespace)))

    whenever<ManualOperationResult?>(eventRunner.resetConnection(connectionId, streamDescriptors))
      .thenReturn(manualOperationResult)

    doReturn(JobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler
      .resetConnectionStream(connectionStreamRequestBody)

    verify(eventRunner).resetConnection(connectionId, streamDescriptors)
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun testResetConnectionStreamWithEmptyList() {
    val connectionId = UUID.randomUUID()
    val streamName = "name"
    val streamNamespace = "namespace"

    val jobId = 123L
    val manualOperationResult = ManualOperationResult(null, jobId, null)
    val streamDescriptors = listOf(StreamDescriptor().withName(streamName).withNamespace(streamNamespace))
    val connectionStreamRequestBody =
      ConnectionStreamRequestBody()
        .connectionId(connectionId)
        .streams(mutableListOf<@Valid ConnectionStream?>())

    whenever(connectionService.getAllStreamsForConnection(connectionId))
      .thenReturn(streamDescriptors)
    whenever<ManualOperationResult?>(eventRunner.resetConnection(connectionId, streamDescriptors))
      .thenReturn(manualOperationResult)

    doReturn(JobInfoRead())
      .whenever(jobConverter)
      .getJobInfoRead(anyOrNull())

    schedulerHandler
      .resetConnectionStream(connectionStreamRequestBody)

    verify(eventRunner).resetConnection(connectionId, streamDescriptors)
  }

  @Test
  @Throws(IOException::class)
  fun testCancelJob() {
    whenever<Job?>(jobPersistence.getJob(JOB_ID)).thenReturn(job)

    val manualOperationResult = ManualOperationResult(null, JOB_ID, null)

    whenever<ManualOperationResult?>(eventRunner.startNewCancellation(SCOPE))
      .thenReturn(manualOperationResult)
    val jobInfo =
      JobInfoRead()
        .job(
          JobRead()
            .id(JOB_ID)
            .createdAt(123L)
            .updatedAt(321L),
        )
    doReturn(jobInfo).whenever(jobConverter).getJobInfoRead(any())

    schedulerHandler.cancelJob(JobIdRequestBody().id(JOB_ID))

    verify(eventRunner).startNewCancellation(SCOPE)
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testAutoPropagateSchemaChangeAddStream() {
    // Verify that if auto propagation is fully enabled, a new stream can be added.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockNewStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithDiff =
      catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
    catalogWithDiff.addStreamsItem(
      AirbyteStreamAndConfiguration()
        .stream(
          AirbyteStream()
            .name(A_DIFFERENT_STREAM)
            .supportedSyncModes(listOf(SyncMode.FULL_REFRESH)),
        ).config(AirbyteStreamConfiguration().selected(true)),
    )

    val workspaceId = source.getWorkspaceId()
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(ConnectionReadList().addConnectionsItem(CONNECTION))
    val request =
      SourceAutoPropagateChange()
        .sourceId(source.getSourceId())
        .workspaceId(workspaceId)
        .catalogId(discoveredSourceId)
        .catalog(catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testAutoPropagateSchemaChangeUpdateStream() {
    // Verify that if auto propagation is fully enabled, an existing stream can be modified.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
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

    val workspaceId = source.getWorkspaceId()
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(ConnectionReadList().addConnectionsItem(CONNECTION))
    val request =
      SourceAutoPropagateChange()
        .sourceId(source.getSourceId())
        .workspaceId(workspaceId)
        .catalogId(discoveredSourceId)
        .catalog(catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testAutoPropagateSchemaChangeRemoveStream() {
    // Verify that if auto propagation is fully enabled, an existing stream can be removed.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockRemoveStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithDiff =
      catalogConverter
        .toApi(clone(airbyteCatalog), sourceVersion)
        .streams(mutableListOf<@Valid AirbyteStreamAndConfiguration?>())

    val workspaceId = source.getWorkspaceId()
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(ConnectionReadList().addConnectionsItem(CONNECTION))
    val request =
      SourceAutoPropagateChange()
        .sourceId(source.getSourceId())
        .workspaceId(workspaceId)
        .catalogId(discoveredSourceId)
        .catalog(catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testAutoPropagateSchemaChangeColumnsOnly() {
    // Verify that if auto propagation is set to PROPAGATE_COLUMNS, then column changes are applied but
    // a new stream is ignored.
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
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
      clone(catalogWithNewColumn)
        .addStreamsItem(AirbyteStreamAndConfiguration().stream(AirbyteStream().name(A_DIFFERENT_STREAM)))

    val workspaceId = source.getWorkspaceId()
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(ConnectionReadList().addConnectionsItem(CONNECTION))
    val request =
      SourceAutoPropagateChange()
        .sourceId(source.getSourceId())
        .workspaceId(source.getWorkspaceId())
        .catalogId(discoveredSourceId)
        .catalog(catalogWithNewColumnAndStream)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  @Test
  @Throws(
    IOException::class,
    ConfigNotFoundException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testAutoPropagateSchemaChangeWithIgnoreMode() {
    val source = SourceHelpers.generateSource(UUID.randomUUID())

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockConnectionForDiscoverJobWithAutopropagation(source, sourceVersion, NonBreakingChangesPreference.IGNORE)
    mockNewStreamDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    val catalogWithDiff =
      catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)
    catalogWithDiff.addStreamsItem(AirbyteStreamAndConfiguration().stream(AirbyteStream().name(A_DIFFERENT_STREAM)))

    val workspaceId = source.getWorkspaceId()
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever<StandardWorkspace?>(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)).thenReturn(workspace)

    val request =
      SourceAutoPropagateChange()
        .sourceId(source.getSourceId())
        .workspaceId(workspaceId)
        .catalogId(discoveredSourceId)
        .catalog(catalogWithDiff)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler, times(0))
      .updateConnection(any<ConnectionUpdate>(), any<String>(), anyBoolean())
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testAutoPropagateSchemaChangeEarlyExits() {
    var request = this.mockedSourceAutoPropagateChange.sourceId(null)
    schedulerHandler.applySchemaChangeForSource(request)
    verifyNoInteractions(connectionsHandler)

    request = this.mockedSourceAutoPropagateChange.catalog(null)
    schedulerHandler.applySchemaChangeForSource(request)
    verifyNoInteractions(connectionsHandler)

    request = this.mockedSourceAutoPropagateChange.catalogId(null)
    schedulerHandler.applySchemaChangeForSource(request)
    verifyNoInteractions(connectionsHandler)

    request = this.mockedSourceAutoPropagateChange.workspaceId(null)
    schedulerHandler.applySchemaChangeForSource(request)
    verifyNoInteractions(connectionsHandler)
  }

  @Test
  @Throws(
    IOException::class,
    JsonValidationException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testSchemaPropagatedEmptyDiff() {
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val catalogId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()

    val oldCatalog =
      io.airbyte.api.model.generated
        .AirbyteCatalog()
        .addStreamsItem(AirbyteStreamAndConfiguration().stream(AirbyteStream().name("foo").namespace("ns")))
    val configuredCatalog =
      io.airbyte.api.model.generated
        .AirbyteCatalog()
        .addStreamsItem(
          AirbyteStreamAndConfiguration()
            .stream(
              AirbyteStream()
                .name("foo")
                .namespace("ns")
                .jsonSchema(emptyObject())
                .supportedSyncModes(listOf(SyncMode.FULL_REFRESH)),
            ).config(
              AirbyteStreamConfiguration().selected(true).syncMode(SyncMode.FULL_REFRESH).destinationSyncMode(
                DestinationSyncMode.APPEND,
              ),
            ),
        )

    val newCatalog =
      io.airbyte.api.model.generated
        .AirbyteCatalog()
        .addStreamsItem(AirbyteStreamAndConfiguration().stream(AirbyteStream().name("foo").namespace("ns")))

    val notificationSettings = NotificationSettings().withSendOnConnectionUpdate(NotificationItem())
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(workspaceId).withNotificationSettings(notificationSettings))
    whenever(sourceService.getSourceConnection(sourceId))
      .thenReturn(SourceConnection().withSourceId(sourceId))
    val connectionRead =
      ConnectionRead()
        .connectionId(connectionId)
        .sourceId(sourceId)
        .syncCatalog(configuredCatalog)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.PROPAGATE_COLUMNS)
    whenever(connectionsHandler.listConnectionsForSource(sourceId, false))
      .thenReturn(ConnectionReadList().addConnectionsItem(connectionRead))
    whenever(connectionsHandler.getConnectionAirbyteCatalog(connectionId))
      .thenReturn(Optional.of(oldCatalog))

    val diff =
      CatalogDiff().addTransformsItem(
        StreamTransform()
          .transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
          .streamDescriptor(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name("new_stream"),
          ),
      )
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(diff)

    val spySchedulerHandler = spy(schedulerHandler)
    val request =
      SourceAutoPropagateChange()
        .sourceId(sourceId)
        .workspaceId(workspaceId)
        .catalogId(catalogId)
        .catalog(newCatalog)
    spySchedulerHandler.applySchemaChangeForSource(request)
    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  @Test
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun testEmptyDiffIsAlwaysPropagated() {
    // Verify that if auto propagation is fully enabled, a new stream can be added.
    val source = SourceHelpers.generateSource(UUID.randomUUID())
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(source.getSourceDefinitionId())
    val sourceVersion =
      ActorDefinitionVersion()
        .withProtocolVersion(SOURCE_PROTOCOL_VERSION)
    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(sourceVersion)

    val discoveredSourceId = mockSuccessfulDiscoverJob(source, sourceVersion)
    mockEmptyDiff()
    mockSourceForDiscoverJob(source, sourceDefinition)

    // NOTE: this is just a clone of the original catalog.
    val discoveredCatalog =
      catalogConverter.toApi(clone(airbyteCatalog), sourceVersion)

    val workspaceId = source.getWorkspaceId()
    val workspace = StandardWorkspace().withWorkspaceId(workspaceId)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true))
      .thenReturn(workspace)

    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(ConnectionReadList().addConnectionsItem(CONNECTION))
    val request =
      SourceAutoPropagateChange()
        .sourceId(source.getSourceId())
        .workspaceId(workspaceId)
        .catalogId(discoveredSourceId)
        .catalog(discoveredCatalog)

    schedulerHandler.applySchemaChangeForSource(request)

    verify(connectionsHandler).applySchemaChange(any(), any(), any(), any(), anyBoolean())
  }

  private val mockedSourceAutoPropagateChange: SourceAutoPropagateChange
    get() =
      SourceAutoPropagateChange()
        .sourceId(UUID.randomUUID())
        .workspaceId(UUID.randomUUID())
        .catalogId(UUID.randomUUID())
        .catalog(
          io.airbyte.api.model.generated
            .AirbyteCatalog(),
        )

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  private fun mockSourceForDiscoverJob(
    source: SourceConnection,
    sourceDefinition: StandardSourceDefinition,
  ) {
    whenever<StandardSourceDefinition?>(sourceService.getStandardSourceDefinition(source.getSourceDefinitionId()))
      .thenReturn(sourceDefinition)
    whenever<ActorDefinitionVersion?>(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        source.getWorkspaceId(),
        source.getSourceId(),
      ),
    ).thenReturn(
      ActorDefinitionVersion()
        .withDockerRepository(SOURCE_DOCKER_REPO)
        .withDockerImageTag(SOURCE_DOCKER_TAG),
    )
    whenever<SourceConnection?>(sourceService.getSourceConnection(source.getSourceId())).thenReturn(source)
  }

  @Throws(ConfigNotFoundException::class, IOException::class)
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

  @Throws(IOException::class, JsonValidationException::class)
  private fun mockConnectionForDiscoverJobWithAutopropagation(
    source: SourceConnection,
    sourceVersion: ActorDefinitionVersion?,
    nonBreakingChangesPreference: NonBreakingChangesPreference?,
  ): ConnectionRead {
    val connectionRead = ConnectionRead()
    connectionRead
      .syncCatalog(catalogConverter.toApi(airbyteCatalog, sourceVersion))
      .connectionId(UUID.randomUUID())
      .notifySchemaChanges(false)
      .nonBreakingChangesPreference(nonBreakingChangesPreference)
    val connectionReadList = ConnectionReadList().connections(listOf(connectionRead))
    whenever(connectionsHandler.listConnectionsForSource(source.getSourceId(), false))
      .thenReturn(connectionReadList)
    val catalogDiff: CatalogDiff = mock()
    val transforms =
      listOf(
        StreamTransform(),
      )
    whenever(catalogDiff.getTransforms()).thenReturn(transforms)
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
    return connectionRead
  }

  @Throws(JsonValidationException::class)
  private fun mockNewStreamDiff() {
    val catalogDiff =
      CatalogDiff().transforms(
        listOf(
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM).streamDescriptor(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(A_DIFFERENT_STREAM),
          ),
        ),
      )
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
  }

  @Throws(JsonValidationException::class)
  private fun mockRemoveStreamDiff() {
    val catalogDiff =
      CatalogDiff().transforms(
        listOf(
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM).streamDescriptor(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(SHOES),
          ),
        ),
      )
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
  }

  @Throws(JsonValidationException::class)
  private fun mockUpdateStreamDiff() {
    val catalogDiff =
      CatalogDiff().transforms(
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .name(SHOES),
            ).updateStream(
              StreamTransformUpdateStream()
                .addFieldTransformsItem(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf<String?>("aDifferentField"))
                    .addField(FieldAdd().schema(deserialize("\"id\": {\"type\": [\"null\", \"integer\"]}")))
                    .breaking(false),
                ),
            ),
        ),
      )
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
  }

  @Throws(JsonValidationException::class)
  private fun mockUpdateAndAddStreamDiff() {
    val catalogDiff =
      CatalogDiff().transforms(
        listOf(
          StreamTransform()
            .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .streamDescriptor(
              io.airbyte.api.model.generated
                .StreamDescriptor()
                .name(SHOES),
            ).updateStream(
              StreamTransformUpdateStream()
                .addFieldTransformsItem(
                  FieldTransform()
                    .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)
                    .fieldName(mutableListOf<String?>("aDifferentField"))
                    .addField(FieldAdd().schema(deserialize("\"id\": {\"type\": [\"null\", \"integer\"]}")))
                    .breaking(false),
                ),
            ),
          StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM).streamDescriptor(
            io.airbyte.api.model.generated
              .StreamDescriptor()
              .name(A_DIFFERENT_STREAM),
          ),
        ),
      )
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(catalogDiff)
  }

  @Throws(JsonValidationException::class)
  private fun mockEmptyDiff() {
    val emptyDiff = CatalogDiff().transforms(mutableListOf<@Valid StreamTransform?>())
    whenever(
      connectionsHandler.getDiff(any(), any(), any(), any()),
    ).thenReturn(emptyDiff)
  }

  companion object {
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
        .withDocumentationUrl(toRuntime<URI?> { URI("https://google.com") })
        .withChangelogUrl(toRuntime<URI?> { URI("https://google.com") })
        .withConnectionSpecification(jsonNode<HashMap<Any?, Any?>?>(HashMap<Any?, Any?>()))

    private val CONNECTOR_SPECIFICATION_WITHOUT_DOCS_URL: ConnectorSpecification =
      ConnectorSpecification()
        .withChangelogUrl(toRuntime<URI?> { URI("https://google.com") })
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
    private val CONNECTION: ConnectionRead? =
      ConnectionRead()
        .connectionId(UUID.randomUUID())
        .sourceId(SOURCE.getSourceId())
        .syncCatalog(null)
        .nonBreakingChangesPreference(NonBreakingChangesPreference.PROPAGATE_FULLY)

    private fun baseSynchronousJobRead(): SynchronousJobRead? {
      // return a new base every time this method is called so that we are not updating
      // fields on the existing one in subsequent tests
      return SynchronousJobRead()
        .id(SYNC_JOB_ID)
        .configId(CONFIG_ID.toString())
        .configType(JobConfigType.CHECK_CONNECTION_SOURCE)
        .createdAt(CREATED_AT)
        .endedAt(CREATED_AT)
        .connectorConfigurationUpdated(CONNECTOR_CONFIG_UPDATED)
        .logType(LogFormatType.FORMATTED)
        .logs(AttemptInfoReadLogs().logLines(mutableListOf<String?>()))
    }

    private val mockFailureReasonFromTrace: io.airbyte.api.model.generated.FailureReason? =
      io.airbyte.api.model.generated
        .FailureReason()
        .failureOrigin(FailureOrigin.fromValue(FAILURE_ORIGIN))
        .failureType(FailureType.fromValue(FAILURE_TYPE))
        .retryable(RETRYABLE)
        .externalMessage(EXTERNAL_MESSAGE)
        .internalMessage(INTERNAL_MESSAGE)
        .stacktrace(STACKTRACE)
        .timestamp(CREATED_AT)

    private val jobSuccessStatusSuccess: CheckConnectionRead? =
      CheckConnectionRead()
        .jobInfo(baseSynchronousJobRead()!!.succeeded(true))
        .status(CheckConnectionRead.StatusEnum.SUCCEEDED)

    private val jobSuccessStatusFailed: CheckConnectionRead? =
      CheckConnectionRead()
        .jobInfo(baseSynchronousJobRead()!!.succeeded(true))
        .status(CheckConnectionRead.StatusEnum.FAILED)
        .message("Something went wrong - check connection failure")

    private val jobFailedWithFailureReason: CheckConnectionRead? =
      CheckConnectionRead()
        .jobInfo(baseSynchronousJobRead()!!.succeeded(false).failureReason(mockFailureReasonFromTrace))

    private val jobFailedWithoutFailureReason: CheckConnectionRead? =
      CheckConnectionRead()
        .jobInfo(baseSynchronousJobRead()!!.succeeded(false))

    @JvmStatic
    private fun provideArguments(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(Optional.of<String?>("succeeded"), false, jobSuccessStatusSuccess),
        Arguments.of(Optional.of<String?>("succeeded"), true, jobFailedWithFailureReason),
        Arguments.of(Optional.of<String?>("failed"), false, jobSuccessStatusFailed),
        Arguments.of(Optional.of<String?>("failed"), true, jobFailedWithFailureReason),
      )
  }
}
