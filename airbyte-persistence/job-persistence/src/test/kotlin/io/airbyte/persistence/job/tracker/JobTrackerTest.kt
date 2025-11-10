/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.json.Jsons.arrayNode
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.resources.Resources.read
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.AirbyteStream
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptStatus
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Metadata
import io.airbyte.config.RefreshConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.Schedule
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.CatalogHelpers
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.tracker.JobTracker.Companion.configToMetadata
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.BiConsumer
import java.util.function.Consumer

internal class JobTrackerTest {
  private lateinit var jobPersistence: JobPersistence
  private lateinit var trackingClient: TrackingClient
  private lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var actorDefinitionVersionHelper: ActorDefinitionVersionHelper
  private lateinit var jobTracker: JobTracker

  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var connectionService: ConnectionService
  private lateinit var operationService: OperationService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var jobService: JobService

  @BeforeEach
  fun setup() {
    jobPersistence = mock<JobPersistence>()
    workspaceHelper = mock<WorkspaceHelper>()
    trackingClient = mock<TrackingClient>()
    sourceService = mock<SourceService>()
    destinationService = mock<DestinationService>()
    connectionService = mock<ConnectionService>()
    operationService = mock<OperationService>()
    workspaceService = mock<WorkspaceService>()
    actorDefinitionVersionHelper = mock<ActorDefinitionVersionHelper>()
    jobService = mock<JobService>()
    jobTracker =
      JobTracker(
        jobPersistence,
        workspaceHelper,
        trackingClient,
        actorDefinitionVersionHelper,
        sourceService,
        destinationService,
        connectionService,
        operationService,
        workspaceService,
        jobService,
        mock<MetricClient>(),
      )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME).withDataplaneGroupId(DATA_PLANE_GROUP_ID))
  }

  @Test
  fun testTrackCheckConnectionSource() {
    val metadata: MutableMap<String?, Any?> =
      mutableMapOf(
        JOB_TYPE to ConfigType.CHECK_CONNECTION_SOURCE,
        JOB_ID_KEY to JOB_ID.toString(),
        ATTEMPT_ID to 0,
        CONNECTOR_SOURCE_KEY to SOURCE_DEF_NAME,
        CONNECTOR_SOURCE_DEFINITION_ID_KEY to UUID1,
        CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY to CONNECTOR_REPOSITORY,
        CONNECTOR_SOURCE_VERSION_KEY to CONNECTOR_VERSION,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    whenever(sourceService.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
      .thenReturn(sourceVersion)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))

    assertCheckConnCorrectMessageForEachState(
      { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker.trackCheckConnectionSource<Any?>(
          JOB_ID,
          UUID1,
          WORKSPACE_ID,
          SOURCE_ID,
          jobState!!,
          output,
        )
      },
      ConfigType.CHECK_CONNECTION_SOURCE,
      metadata,
      true,
    )
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, null))
      .thenReturn(sourceVersion)
    assertCheckConnCorrectMessageForEachState(
      { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker.trackCheckConnectionSource<Any?>(
          JOB_ID,
          UUID1,
          WORKSPACE_ID,
          null,
          jobState!!,
          output,
        )
      },
      ConfigType.CHECK_CONNECTION_SOURCE,
      metadata,
      true,
    )
  }

  @Test
  fun testTrackCheckConnectionDestination() {
    val metadata: MutableMap<String?, Any?> =
      mutableMapOf(
        JOB_TYPE to ConfigType.CHECK_CONNECTION_DESTINATION,
        JOB_ID_KEY to JOB_ID.toString(),
        ATTEMPT_ID to 0,
        "connector_destination" to DESTINATION_DEF_NAME,
        "connector_destination_definition_id" to UUID2,
        "connector_destination_docker_repository" to CONNECTOR_REPOSITORY,
        "connector_destination_version" to CONNECTOR_VERSION,
      )

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    whenever(destinationService.getStandardDestinationDefinition(UUID2))
      .thenReturn(destinationDefinition)
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      ),
    ).thenReturn(destinationVersion)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))

    assertCheckConnCorrectMessageForEachState(
      { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker.trackCheckConnectionDestination<Any?>(
          JOB_ID,
          UUID2,
          WORKSPACE_ID,
          DESTINATION_ID,
          jobState!!,
          output,
        )
      },
      ConfigType.CHECK_CONNECTION_DESTINATION,
      metadata,
      true,
    )
    whenever(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, null))
      .thenReturn(destinationVersion)
    assertCheckConnCorrectMessageForEachState(
      { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker.trackCheckConnectionDestination<Any?>(
          JOB_ID,
          UUID2,
          WORKSPACE_ID,
          null,
          jobState!!,
          output,
        )
      },
      ConfigType.CHECK_CONNECTION_DESTINATION,
      metadata,
      true,
    )
  }

  @Test
  fun testTrackDiscover() {
    val metadata: MutableMap<String?, Any?> =
      mutableMapOf(
        JOB_TYPE to ConfigType.DISCOVER_SCHEMA,
        JOB_ID_KEY to JOB_ID.toString(),
        ATTEMPT_ID to 0,
        CONNECTOR_SOURCE_KEY to SOURCE_DEF_NAME,
        CONNECTOR_SOURCE_DEFINITION_ID_KEY to UUID1,
        CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY to CONNECTOR_REPOSITORY,
        CONNECTOR_SOURCE_VERSION_KEY to CONNECTOR_VERSION,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    whenever(sourceService.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
      .thenReturn(sourceVersion)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))

    assertDiscoverCorrectMessageForEachState(
      { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker.trackDiscover(
          JOB_ID,
          UUID1,
          WORKSPACE_ID,
          SOURCE_ID,
          ActorType.SOURCE,
          jobState!!,
          output,
        )
      },
      metadata,
      true,
    )
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, null))
      .thenReturn(sourceVersion)
    assertDiscoverCorrectMessageForEachState(
      { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker.trackDiscover(
          JOB_ID,
          UUID1,
          WORKSPACE_ID,
          null,
          null,
          jobState!!,
          output,
        )
      },
      metadata,
      false,
    )
  }

  @Test
  fun testTrackSync() {
    testAsynchronous(ConfigType.SYNC, SYNC_CONFIG_METADATA)
  }

  @Test
  fun testTrackRefresh() {
    val expectedExtraMetadata: MutableMap<String?, Any?> =
      mergeMaps(
        SYNC_CONFIG_METADATA,
        mutableMapOf("refresh_types" to listOf(RefreshStream.RefreshType.TRUNCATE.toString())),
      )
    testAsynchronous(ConfigType.REFRESH, expectedExtraMetadata)
  }

  @Test
  fun testTrackSyncForInternalFailure() {
    val jobId = 12345L
    val attemptNumber = 2
    val jobState = JobTracker.JobState.SUCCEEDED
    val exception: Exception = IOException("test")

    whenever(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)).thenReturn(WORKSPACE_ID)
    whenever(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(CATALOG)
          .withManual(true),
      )
    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))
    whenever(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(CATALOG)
          .withManual(false)
          .withSchedule(Schedule().withUnits(1L).withTimeUnit(Schedule.TimeUnit.MINUTES)),
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME)

    whenever(sourceService.getSourceDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(sourceDefinition)
    whenever(destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(destinationDefinition)
    whenever(sourceService.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(destinationService.getStandardDestinationDefinition(UUID2))
      .thenReturn(destinationDefinition)
    whenever(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
      .thenReturn(
        ActorDefinitionVersion()
          .withDockerRepository(CONNECTOR_REPOSITORY)
          .withDockerImageTag(CONNECTOR_VERSION)
          .withSpec(SOURCE_SPEC),
      )
    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      ),
    ).thenReturn(
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(DESTINATION_SPEC),
    )

    jobTracker.trackSyncForInternalFailure(jobId, CONNECTION_ID, attemptNumber, jobState, exception)
    val metadata =
      buildMap {
        put("namespace_definition", JobSyncConfig.NamespaceDefinitionType.SOURCE)
        put("number_of_streams", 1)
        put("internal_error_type", exception.javaClass.getName())
        put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
        put("internal_error_cause", exception.message)
        put(FREQUENCY_KEY, "1 min")
        put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
        put("workspace_id", WORKSPACE_ID)
        put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        put(ATTEMPT_STAGE_KEY, "ENDED")
        put("attempt_completion_status", jobState)
        put("connection_id", CONNECTION_ID)
        put(JOB_ID_KEY, jobId.toString())
        put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        put("connector_destination_version", CONNECTOR_VERSION)
        put("attempt_id", attemptNumber)
        put("connector_destination", DESTINATION_DEF_NAME)
        put("operation_count", 0)
        put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
        put("table_prefix", false)
        put("workspace_name", WORKSPACE_NAME)
        put("connector_destination_definition_id", UUID2)
        put("source_id", SOURCE_ID)
        put("destination_id", DESTINATION_ID)
      }
    verify(trackingClient).track(
      scopeId = WORKSPACE_ID,
      scopeType = ScopeType.WORKSPACE,
      action = JobTracker.INTERNAL_FAILURE_SYNC_EVENT,
      metadata =
        metadata
          .mapKeys {
            it.key
          }.mapValues { it.value },
    )
  }

  @Test
  fun testTrackReset() {
    testAsynchronous(ConfigType.RESET_CONNECTION)
  }

  // todo update with connection-specific test
  @JvmOverloads
  fun testAsynchronous(
    configType: ConfigType,
    additionalExpectedMetadata: MutableMap<String?, Any?>? = mutableMapOf(),
  ) {
    // for sync the job id is a long not a uuid.
    val jobId = 10L
    whenever(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)).thenReturn(WORKSPACE_ID)

    val metadata = getJobMetadata(configType, jobId)
    val job = getJobMock(configType, jobId)

    // test when frequency is manual.
    whenever(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(CATALOG)
          .withManual(true),
      )

    whenever(actorDefinitionVersionHelper.getSourceVersion(any(), eq(WORKSPACE_ID), eq(SOURCE_ID)))
      .thenReturn(
        ActorDefinitionVersion()
          .withActorDefinitionId(SOURCE_DEFINITION_ID)
          .withDockerRepository(CONNECTOR_REPOSITORY)
          .withDockerImageTag(CONNECTOR_VERSION)
          .withSpec(SOURCE_SPEC),
      )
    whenever(actorDefinitionVersionHelper.getDestinationVersion(any(), eq(WORKSPACE_ID), eq(DESTINATION_ID)))
      .thenReturn(
        ActorDefinitionVersion()
          .withActorDefinitionId(DESTINATION_DEFINITION_ID)
          .withDockerRepository(CONNECTOR_REPOSITORY)
          .withDockerImageTag(CONNECTOR_VERSION)
          .withSpec(DESTINATION_SPEC),
      )

    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))
    val manualMetadata: MutableMap<String?, Any?> =
      mergeMaps(
        metadata,
        mutableMapOf(FREQUENCY_KEY to "manual"),
        additionalExpectedMetadata,
      )
    assertCorrectMessageForEachState({ jobState: JobTracker.JobState? -> jobTracker.trackSync(job, jobState!!) }, manualMetadata)

    // test when frequency is scheduled.
    whenever(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(CATALOG)
          .withManual(false)
          .withSchedule(Schedule().withUnits(1L).withTimeUnit(Schedule.TimeUnit.MINUTES)),
      )
    val scheduledMetadata: MutableMap<String?, Any?> =
      mergeMaps(
        metadata,
        mutableMapOf(FREQUENCY_KEY to "1 min"),
        additionalExpectedMetadata,
      )
    assertCorrectMessageForEachState({ jobState: JobTracker.JobState? -> jobTracker.trackSync(job, jobState!!) }, scheduledMetadata)
  }

  @Test
  fun testTrackSyncAttempt() {
    testAsynchronousAttempt(ConfigType.SYNC, SYNC_CONFIG_METADATA)
  }

  @Test
  fun testTrackResetAttempt() {
    testAsynchronousAttempt(ConfigType.RESET_CONNECTION)
  }

  // todo (cgardens)
  @Disabled
  @Test
  fun testTrackSyncAttemptWithFailures() {
    testAsynchronousAttemptWithFailures(ConfigType.SYNC, SYNC_CONFIG_METADATA)
  }

  @Test
  fun testConfigToMetadata() {
    val configJson = read("example_config.json")
    val config = deserialize(configJson)

    val schemaJson = read("example_config_schema.json")
    val schema = deserialize(schemaJson)

    val expected: MutableMap<String?, Any?> =
      mutableMapOf(
        "username" to JobTracker.SET,
        "has_ssl" to false,
        "password" to JobTracker.SET,
        "one_of.type_key" to "foo",
        "one_of.some_key" to JobTracker.SET,
        "const_object.sub_key" to "bar",
        "const_object.sub_array" to "[1,2,3]",
        "const_object.sub_object.sub_sub_key" to "baz",
        "enum_string" to "foo",
        "additionalPropertiesUnset.foo" to JobTracker.SET,
        "additionalPropertiesBoolean.foo" to JobTracker.SET,
        "additionalPropertiesSchema.foo" to JobTracker.SET,
        "additionalPropertiesConst.foo" to 42,
        "additionalPropertiesEnumString" to "foo",
      )

    val actual = configToMetadata(config, schema).toMutableMap()

    Assertions.assertEquals(expected, actual)
  }

  @Test
  fun testGenerateMetadata() {
    val jobId = "shouldBeLong"
    val attemptId = 2
    val configType = ConfigType.REFRESH
    val previousJob =
      Job(
        0,
        ConfigType.RESET_CONNECTION,
        CONNECTION_ID.toString(),
        JobConfig(),
        mutableListOf(),
        JobStatus.RUNNING,
        0L,
        0L,
        0L,
        true,
      )

    val metadata = jobTracker.generateJobMetadata(jobId, configType, attemptId, Optional.of<Job>(previousJob)).toMutableMap()
    Assertions.assertEquals(jobId, metadata["job_id"])
    Assertions.assertEquals(attemptId, metadata["attempt_id"])
    Assertions.assertEquals(configType, metadata["job_type"])
    Assertions.assertEquals(ConfigType.RESET_CONNECTION, metadata["previous_job_type"])
  }

  fun testAsynchronousAttempt(
    configType: ConfigType,
    additionalExpectedMetadata: MutableMap<String?, Any?>?,
  ) {
    testAsynchronousAttempt(configType, getJobWithAttemptsMock(configType, LONG_JOB_ID), additionalExpectedMetadata)
  }

  @JvmOverloads
  fun testAsynchronousAttempt(
    configType: ConfigType?,
    job: Job =
      getJobWithAttemptsMock(
        configType!!,
        LONG_JOB_ID,
      ),
    additionalExpectedMetadata: MutableMap<String?, Any?>? = mutableMapOf(),
  ) {
    val metadata = getJobMetadata(configType, LONG_JOB_ID)
    // test when frequency is manual.
    whenever(connectionService.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withManual(true)
          .withCatalog(CATALOG),
      )

    whenever(actorDefinitionVersionHelper.getSourceVersion(any(), eq(WORKSPACE_ID), eq(SOURCE_ID)))
      .thenReturn(
        ActorDefinitionVersion()
          .withActorDefinitionId(SOURCE_DEFINITION_ID)
          .withDockerRepository(CONNECTOR_REPOSITORY)
          .withDockerImageTag(CONNECTOR_VERSION)
          .withSpec(SOURCE_SPEC),
      )
    whenever(actorDefinitionVersionHelper.getDestinationVersion(any(), eq(WORKSPACE_ID), eq(DESTINATION_ID)))
      .thenReturn(
        ActorDefinitionVersion()
          .withActorDefinitionId(DESTINATION_DEFINITION_ID)
          .withDockerRepository(CONNECTOR_REPOSITORY)
          .withDockerImageTag(CONNECTOR_VERSION)
          .withSpec(DESTINATION_SPEC),
      )

    whenever(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(LONG_JOB_ID)).thenReturn(WORKSPACE_ID)
    whenever(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))
    val manualMetadata: MutableMap<String?, Any?> =
      mergeMaps(
        ATTEMPT_METADATA,
        metadata,
        mutableMapOf(FREQUENCY_KEY to "manual"),
        additionalExpectedMetadata,
      )

    jobTracker.trackSync(job, JobTracker.JobState.SUCCEEDED)
    assertCorrectMessageForSucceededState(JobTracker.SYNC_EVENT, manualMetadata)

    jobTracker.trackSync(job, JobTracker.JobState.FAILED)
    assertCorrectMessageForFailedState(JobTracker.SYNC_EVENT, manualMetadata)
  }

  private fun configFailureJson(): JsonNode {
    val linkedHashMap: MutableMap<String?, Any?> =
      linkedMapOf(
        "failureOrigin" to "source",
        "failureType" to "config_error",
        "internalMessage" to "Internal config error error msg",
        "externalMessage" to "Config error related msg",
        METADATA to mapOf(SOME to METADATA),
        "retryable" to true,
        "timestamp" to 1010,
      )
    return jsonNode(linkedHashMap)
  }

  private fun systemFailureJson(): JsonNode {
    val linkedHashMap1: MutableMap<String?, Any?> =
      linkedMapOf(
        "failureOrigin" to "replication",
        "failureType" to "system_error",
        "internalMessage" to "Internal system error error msg",
        "externalMessage" to "System error related msg",
        METADATA to mapOf<String?, String?>(SOME to METADATA),
        "retryable" to true,
        "timestamp" to 1100,
      )
    return jsonNode(linkedHashMap1)
  }

  private fun unknownFailureJson(): JsonNode {
    val linkedHashMap2: MutableMap<String?, Any?> =
      linkedMapOf(
        "failureOrigin" to null,
        "failureType" to null,
        "internalMessage" to "Internal unknown error error msg",
        "externalMessage" to "Unknown error related msg",
        METADATA to mapOf<String?, String?>(SOME to METADATA),
        "retryable" to true,
        "timestamp" to 1110,
      )
    return jsonNode(linkedHashMap2)
  }

  fun testAsynchronousAttemptWithFailures(
    configType: ConfigType,
    additionalExpectedMetadata: MutableMap<String?, Any?>,
  ) {
    val failureMetadata: MutableMap<String?, Any?> =
      mutableMapOf(
        "failure_reasons" to arrayNode().addAll(listOf(configFailureJson(), systemFailureJson(), unknownFailureJson())).toString(),
        "main_failure_reason" to configFailureJson().toString(),
      )
    testAsynchronousAttempt(
      configType,
      getJobWithFailuresMock(configType, LONG_JOB_ID),
      mergeMaps<String?, Any?>(additionalExpectedMetadata, failureMetadata),
    )
  }

  private fun getJobMock(
    configType: ConfigType,
    jobId: Long,
  ): Job = getJobMock(configType, jobId, null)

  private fun getJobMock(
    configType: ConfigType,
    jobId: Long,
    attempts: MutableList<Attempt?>?,
  ): Job {
    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME)

    whenever(sourceService.getSourceDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(sourceDefinition)
    whenever(
      destinationService.getDestinationDefinitionFromConnection(
        CONNECTION_ID,
      ),
    ).thenReturn(destinationDefinition)
    whenever(sourceService.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(destinationService.getStandardDestinationDefinition(UUID2))
      .thenReturn(destinationDefinition)

    whenever(
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        WORKSPACE_ID,
        SOURCE_ID,
      ),
    ).thenReturn(
      ActorDefinitionVersion()
        .withSpec(SOURCE_SPEC)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withDockerRepository(CONNECTOR_REPOSITORY),
    )

    whenever(
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      ),
    ).thenReturn(
      ActorDefinitionVersion()
        .withSpec(DESTINATION_SPEC)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withDockerRepository(CONNECTOR_REPOSITORY),
    )

    val catalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream("stream", emptyObject(), listOf(SyncMode.FULL_REFRESH)).withNamespace("namespace"),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val attemptSyncConfig =
      AttemptSyncConfig()
        .withSourceConfiguration(jsonNode(mapOf(KEY to "some_value")))
        .withDestinationConfiguration(jsonNode(mapOf(KEY to false)))

    val jobConfig = mock<JobConfig>()
    whenever(jobConfig.configType).thenReturn(configType)

    if (configType == ConfigType.SYNC) {
      val jobSyncConfig =
        JobSyncConfig()
          .withConfiguredAirbyteCatalog(catalog)
      whenever(jobConfig.sync).thenReturn(jobSyncConfig)
    }
    if (configType == ConfigType.REFRESH) {
      val refreshConfig =
        RefreshConfig()
          .withConfiguredAirbyteCatalog(catalog)
          .withStreamsToRefresh(
            catalog.streams
              .stream()
              .map { s: ConfiguredAirbyteStream? ->
                RefreshStream()
                  .withRefreshType(RefreshStream.RefreshType.TRUNCATE)
                  .withStreamDescriptor(StreamDescriptor().withName(s!!.stream.name).withNamespace(s.stream.namespace))
              }.toList(),
          )
      whenever(jobConfig.refresh).thenReturn(refreshConfig)
    }

    val defaultAttempt =
      Attempt(
        700,
        jobId,
        null,
        attemptSyncConfig,
        null,
        AttemptStatus.RUNNING,
        null,
        null,
        1000L,
        1000L,
        null,
      )

    return Job(
      jobId,
      configType,
      CONNECTION_ID.toString(),
      jobConfig,
      attempts?.filterNotNull() ?: listOf(defaultAttempt),
      JobStatus.RUNNING,
      1000L,
      1000L,
      1000L,
      true,
    )
  }

  private val attemptMock: Attempt
    get() {
      val attempt = mock<Attempt>()
      val jobOutput = mock<JobOutput>()
      val syncOutput = mock<StandardSyncOutput>()
      val syncSummary = mock<StandardSyncSummary>()
      val syncStats = mock<SyncStats>()

      val attemptSyncConfig =
        AttemptSyncConfig()
          .withSourceConfiguration(
            jsonNode(mapOf(KEY to "some_value")),
          ).withDestinationConfiguration(
            jsonNode(mapOf(KEY to false)),
          )
      whenever(attempt.syncConfig)
        .thenReturn(Optional.of(attemptSyncConfig).orElse(null))
      whenever(syncSummary.startTime).thenReturn(SYNC_START_TIME)
      whenever(syncSummary.endTime).thenReturn(SYNC_END_TIME)
      whenever(syncSummary.bytesSynced).thenReturn(SYNC_BYTES_SYNC)
      whenever(syncSummary.recordsSynced).thenReturn(SYNC_RECORDS_SYNC)
      whenever(syncOutput.standardSyncSummary).thenReturn(syncSummary)
      whenever(syncSummary.totalStats).thenReturn(syncStats)
      whenever(jobOutput.sync).thenReturn(syncOutput)
      whenever(attempt.output).thenReturn(jobOutput)
      whenever(syncStats.sourceStateMessagesEmitted).thenReturn(3L)
      whenever(syncStats.destinationStateMessagesEmitted).thenReturn(1L)
      whenever(syncStats.maxSecondsBeforeSourceStateMessageEmitted).thenReturn(5L)
      whenever(syncStats.meanSecondsBeforeSourceStateMessageEmitted).thenReturn(4L)
      whenever(syncStats.maxSecondsBetweenStateMessageEmittedandCommitted).thenReturn(7L)
      whenever(syncStats.meanSecondsBetweenStateMessageEmittedandCommitted).thenReturn(6L)
      whenever(syncStats.replicationStartTime).thenReturn(7L)
      whenever(syncStats.replicationEndTime).thenReturn(8L)
      whenever(syncStats.sourceReadStartTime).thenReturn(9L)
      whenever(syncStats.sourceReadEndTime).thenReturn(10L)
      whenever(syncStats.destinationWriteStartTime).thenReturn(11L)
      whenever(syncStats.destinationWriteEndTime).thenReturn(12L)

      return attempt
    }

  private fun getJobWithAttemptsMock(
    configType: ConfigType,
    jobId: Long,
  ): Job = getJobWithAttemptsMock(configType, jobId, mutableListOf(this.attemptMock))

  private fun getJobWithAttemptsMock(
    configType: ConfigType,
    jobId: Long,
    attempts: MutableList<Attempt?>?,
  ): Job {
    val job = getJobMock(configType, jobId, attempts)
    whenever(jobPersistence.getJob(jobId)).thenReturn(job)
    return job
  }

  private val configFailureReasonMock: FailureReason?
    get() =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
        .withRetryable(true)
        .withMetadata(Metadata().withAdditionalProperty(SOME, METADATA))
        .withExternalMessage("Config error related msg")
        .withInternalMessage("Internal config error error msg")
        .withStacktrace("Don't include stacktrace in call to track")
        .withTimestamp(SYNC_START_TIME + 10)

  private val systemFailureReasonMock: FailureReason?
    get() =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.REPLICATION)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withRetryable(true)
        .withMetadata(Metadata().withAdditionalProperty(SOME, METADATA))
        .withExternalMessage("System error related msg")
        .withInternalMessage("Internal system error error msg")
        .withStacktrace("Don't include stacktrace in call to track")
        .withTimestamp(SYNC_START_TIME + 100)

  private val unknownFailureReasonMock: FailureReason?
    get() =
      FailureReason()
        .withRetryable(true)
        .withMetadata(Metadata().withAdditionalProperty(SOME, METADATA))
        .withExternalMessage("Unknown error related msg")
        .withInternalMessage("Internal unknown error error msg")
        .withStacktrace("Don't include stacktrace in call to track")
        .withTimestamp(SYNC_START_TIME + 110)

  private val attemptsWithFailuresMock: MutableList<Attempt?>
    get() {
      val attemptWithSingleFailure = this.attemptMock
      val singleFailureSummary = mock<AttemptFailureSummary>()
      whenever(singleFailureSummary.failures)
        .thenReturn(listOf(this.configFailureReasonMock))
      whenever(attemptWithSingleFailure.failureSummary)
        .thenReturn(singleFailureSummary)

      val attemptWithMultipleFailures = this.attemptMock
      val multipleFailuresSummary = mock<AttemptFailureSummary>()
      whenever(multipleFailuresSummary.failures)
        .thenReturn(listOf(this.systemFailureReasonMock, this.unknownFailureReasonMock))
      whenever(attemptWithMultipleFailures.failureSummary)
        .thenReturn(multipleFailuresSummary)

      val attemptWithNoFailures = this.attemptMock
      whenever(attemptWithNoFailures.failureSummary)
        .thenReturn(null)

      // in non-test cases we shouldn't actually get failures out of order chronologically
      // this is to verify that we are explicitly sorting the results with tracking failure metadata
      return mutableListOf(attemptWithMultipleFailures, attemptWithSingleFailure, attemptWithNoFailures)
    }

  private fun getJobWithFailuresMock(
    configType: ConfigType,
    jobId: Long,
  ): Job = getJobWithAttemptsMock(configType, jobId, this.attemptsWithFailuresMock)

  private fun getJobMetadata(
    configType: ConfigType?,
    jobId: Long,
  ): MutableMap<String?, Any?> =
    mutableMapOf(
      JOB_TYPE to if (configType != ConfigType.RESET_CONNECTION) configType else ConfigType.CLEAR,
      JOB_ID_KEY to jobId.toString(),
      ATTEMPT_ID to 1,
      "connection_id" to CONNECTION_ID,
      CONNECTOR_SOURCE_KEY to SOURCE_DEF_NAME,
      CONNECTOR_SOURCE_DEFINITION_ID_KEY to UUID1,
      CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY to CONNECTOR_REPOSITORY,
      CONNECTOR_SOURCE_VERSION_KEY to CONNECTOR_VERSION,
      "connector_destination" to DESTINATION_DEF_NAME,
      "connector_destination_definition_id" to UUID2,
      "connector_destination_docker_repository" to CONNECTOR_REPOSITORY,
      "connector_destination_version" to CONNECTOR_VERSION,
      "namespace_definition" to JobSyncConfig.NamespaceDefinitionType.SOURCE,
      "table_prefix" to false,
      "operation_count" to 0,
      "number_of_streams" to 1,
      "source_id" to SOURCE_ID,
      "destination_id" to DESTINATION_ID,
    )

  private fun assertCheckConnCorrectMessageForEachState(
    jobStateConsumer: BiConsumer<JobTracker.JobState?, ConnectorJobOutput?>,
    configType: ConfigType?,
    metadata: MutableMap<String?, Any?>,
    workspaceSet: Boolean,
  ) {
    reset(trackingClient)

    // Output does not exist when job has started.
    jobStateConsumer.accept(JobTracker.JobState.STARTED, null)

    val connectionCheckSuccessOutput = StandardCheckConnectionOutput()
    connectionCheckSuccessOutput.status = StandardCheckConnectionOutput.Status.SUCCEEDED
    val checkConnectionSuccessJobOutput = ConnectorJobOutput().withCheckConnection(connectionCheckSuccessOutput)
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED, checkConnectionSuccessJobOutput)
    val checkConnSuccessMetadata: MutableMap<String?, Any?> = mutableMapOf("check_connection_outcome" to "succeeded")

    val connectionCheckFailureOutput = StandardCheckConnectionOutput()
    connectionCheckFailureOutput.status = StandardCheckConnectionOutput.Status.FAILED
    connectionCheckFailureOutput.message = "Please check your Personal Access Token."
    val checkConnectionFailureJobOutput = ConnectorJobOutput().withCheckConnection(connectionCheckFailureOutput)
    jobStateConsumer.accept(
      JobTracker.JobState.SUCCEEDED,
      checkConnectionFailureJobOutput,
    ) // The job still succeeded, only the connection check failed
    val checkConnFailureMetadata: MutableMap<String?, Any?> =
      mutableMapOf(
        "check_connection_outcome" to "failed",
        "check_connection_message" to "Please check your Personal Access Token.",
      )

    // Failure implies the job threw an exception which almost always meant no output.
    val failedCheckJobOutput = ConnectorJobOutput()
    failedCheckJobOutput.failureReason = this.configFailureReasonMock
    jobStateConsumer.accept(JobTracker.JobState.FAILED, failedCheckJobOutput)
    val failedCheckJobMetadata: MutableMap<String?, Any?> = mutableMapOf("failure_reason" to configFailureJson().toString())

    if (workspaceSet) {
      assertCorrectMessageForStartedState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        metadata,
      )
      assertCorrectMessageForSucceededState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps<String?, Any?>(metadata, checkConnSuccessMetadata),
      )
      assertCorrectMessageForSucceededState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps<String?, Any?>(metadata, checkConnFailureMetadata),
      )
      assertCorrectMessageForFailedState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps<String?, Any?>(metadata, failedCheckJobMetadata),
      )
    } else {
      verifyNoInteractions(trackingClient)
    }
  }

  private fun assertDiscoverCorrectMessageForEachState(
    jobStateConsumer: BiConsumer<JobTracker.JobState?, ConnectorJobOutput?>,
    metadata: MutableMap<String?, Any?>,
    workspaceSet: Boolean,
  ) {
    reset(trackingClient)

    // Output does not exist when job has started.
    jobStateConsumer.accept(JobTracker.JobState.STARTED, null)

    val discoverCatalogID = UUID.randomUUID()
    val discoverSuccessJobOutput = ConnectorJobOutput().withDiscoverCatalogId(discoverCatalogID)
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED, discoverSuccessJobOutput)

    // Failure implies the job threw an exception which almost always meant no output.
    val failedDiscoverOutput = ConnectorJobOutput()
    failedDiscoverOutput.failureReason = this.systemFailureReasonMock
    jobStateConsumer.accept(JobTracker.JobState.FAILED, failedDiscoverOutput)

    val failedDiscoverMetadata: MutableMap<String?, Any?> = mutableMapOf("failure_reason" to systemFailureJson().toString())

    if (workspaceSet) {
      assertCorrectMessageForStartedState(JobTracker.DISCOVER_EVENT, metadata)
      assertCorrectMessageForSucceededState(JobTracker.DISCOVER_EVENT, metadata)
      assertCorrectMessageForFailedState(JobTracker.DISCOVER_EVENT, mergeMaps<String?, Any?>(metadata, failedDiscoverMetadata))
    } else {
      verifyNoInteractions(trackingClient)
    }
  }

  /**
   * Tests that the tracker emits the correct message for when the job starts, succeeds, and fails.
   *
   * @param jobStateConsumer - consumer that takes in a job state and then calls the relevant method
   * on the job tracker with it. if testing discover, it calls trackDiscover, etc.
   * @param expectedMetadata - expected metadata (except job state).
   */
  private fun assertCorrectMessageForEachState(
    jobStateConsumer: Consumer<JobTracker.JobState?>,
    expectedMetadata: MutableMap<String?, Any?>,
  ) {
    jobStateConsumer.accept(JobTracker.JobState.STARTED)
    assertCorrectMessageForStartedState(JobTracker.SYNC_EVENT, expectedMetadata)
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED)
    assertCorrectMessageForSucceededState(JobTracker.SYNC_EVENT, expectedMetadata)
    jobStateConsumer.accept(JobTracker.JobState.FAILED)
    assertCorrectMessageForFailedState(JobTracker.SYNC_EVENT, expectedMetadata)
  }

  private fun assertCorrectMessageForStartedState(
    action: String?,
    metadata: MutableMap<String?, Any?>,
  ) {
    verify(trackingClient)
      .track(
        WORKSPACE_ID,
        ScopeType.WORKSPACE,
        action,
        mergeMaps(metadata, STARTED_STATE_METADATA, mockWorkspaceInfo())
          .mapKeys {
            it.key!!
          }.mapValues { it.value },
      )
  }

  private fun assertCorrectMessageForSucceededState(
    action: String?,
    metadata: MutableMap<String?, Any?>,
  ) {
    verify(trackingClient, atLeastOnce())
      .track(eq(WORKSPACE_ID), eq(ScopeType.WORKSPACE), eq(action), any())
  }

  private fun assertCorrectMessageForFailedState(
    action: String?,
    metadata: MutableMap<String?, Any?>,
  ) {
    verify(trackingClient, atLeastOnce())
      .track(eq(WORKSPACE_ID), eq(ScopeType.WORKSPACE), eq(action), any())
  }

  private fun mockWorkspaceInfo(): MutableMap<String?, Any?> =
    mutableMapOf(
      "workspace_id" to WORKSPACE_ID,
      "workspace_name" to WORKSPACE_NAME,
    )

  companion object {
    private val OBJECT_MAPPER = ObjectMapper()

    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val WORKSPACE_NAME = "WORKSPACE_TEST"
    private val DATA_PLANE_GROUP_ID = UUID.randomUUID()
    private val JOB_ID: UUID = UUID.randomUUID()
    private val UUID1: UUID = UUID.randomUUID()
    private val UUID2: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION_ID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_DEFINITION_ID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private const val SOURCE_DEF_NAME = "postgres"
    private const val DESTINATION_DEF_NAME = "bigquery"
    private const val CONNECTOR_REPOSITORY = "test/test"
    private const val CONNECTOR_VERSION = "test"
    private const val JOB_TYPE = "job_type"
    private const val JOB_ID_KEY = "job_id"
    private const val ATTEMPT_ID = "attempt_id"
    private const val METADATA = "metadata"
    private const val SOME = "some"
    private const val ATTEMPT_STAGE_KEY = "attempt_stage"
    private const val CONNECTOR_SOURCE_KEY = "connector_source"
    private const val CONNECTOR_SOURCE_DEFINITION_ID_KEY = "connector_source_definition_id"
    private const val CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY = "connector_source_docker_repository"
    private const val CONNECTOR_SOURCE_VERSION_KEY = "connector_source_version"
    private const val FREQUENCY_KEY = "frequency"

    private const val SYNC_START_TIME = 1000L
    private const val SYNC_END_TIME = 10000L
    private const val SYNC_DURATION = 9L // in sync between end and start time
    private const val SYNC_BYTES_SYNC = 42L
    private const val SYNC_RECORDS_SYNC = 4L
    private const val LONG_JOB_ID = 10L // for sync the job id is a long not a uuid.

    private val STARTED_STATE_METADATA: MutableMap<String?, Any?> = mutableMapOf(ATTEMPT_STAGE_KEY to "STARTED")
    private val SUCCEEDED_STATE_METADATA: MutableMap<String?, Any?> =
      mutableMapOf(
        ATTEMPT_STAGE_KEY to "ENDED",
        "attempt_completion_status" to JobTracker.JobState.SUCCEEDED,
      )
    private val FAILED_STATE_METADATA: MutableMap<String?, Any?> =
      mutableMapOf(
        ATTEMPT_STAGE_KEY to "ENDED",
        "attempt_completion_status" to JobTracker.JobState.FAILED,
      )
    private val ATTEMPT_METADATA: MutableMap<String?, Any?> =
      mutableMapOf(
        "sync_start_time" to SYNC_START_TIME,
        "duration" to SYNC_DURATION,
        "volume_rows" to SYNC_RECORDS_SYNC,
        "volume_mb" to SYNC_BYTES_SYNC,
        "count_state_messages_from_source" to 3L,
        "count_state_messages_from_destination" to 1L,
        "max_seconds_before_source_state_message_emitted" to 5L,
        "mean_seconds_before_source_state_message_emitted" to 4L,
        "max_seconds_between_state_message_emit_and_commit" to 7L,
        "mean_seconds_between_state_message_emit_and_commit" to 6L,
        "replication_start_time" to 7L,
        "replication_end_time" to 8L,
        "source_read_start_time" to 9L,
        "source_read_end_time" to 10L,
        "destination_write_start_time" to 11L,
        "destination_write_end_time" to 12L,
      )
    private val SYNC_CONFIG_METADATA: MutableMap<String?, Any?> =
      mutableMapOf(
        JobTracker.CONFIG + ".source" to "{\"key\":\"set\"}",
        JobTracker.CONFIG + ".destination" to "{\"key\":false}",
        JobTracker.CATALOG + ".sync_mode.full_refresh" to JobTracker.SET,
        JobTracker.CATALOG + ".destination_sync_mode.append" to JobTracker.SET,
        "namespace_definition" to JobSyncConfig.NamespaceDefinitionType.SOURCE,
        "table_prefix" to false,
        "operation_count" to 0,
        "source_id" to SOURCE_ID,
        "destination_id" to DESTINATION_ID,
      )
    private val catalogHelpers = CatalogHelpers(FieldGenerator())
    private val CATALOG: ConfiguredAirbyteCatalog =
      catalogHelpers
        .createConfiguredAirbyteCatalog(
          "stream_name",
          "stream_namespace",
          Field.of("int_field", JsonSchemaType.NUMBER),
        )

    private val SOURCE_SPEC: ConnectorSpecification?
    private val DESTINATION_SPEC: ConnectorSpecification?
    const val KEY: String = "key"

    init {
      try {
        SOURCE_SPEC =
          ConnectorSpecification().withConnectionSpecification(
            OBJECT_MAPPER.readTree(
              """
              {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string"
                  }
                }
              }
              
              """.trimIndent(),
            ),
          )
        DESTINATION_SPEC =
          ConnectorSpecification().withConnectionSpecification(
            OBJECT_MAPPER.readTree(
              """
              {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "boolean"
                  }
                }
              }
              
              """.trimIndent(),
            ),
          )
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }

    /**
     * Combine the contents of multiple maps. In the event of duplicate keys, the contents of maps later
     * in the input args overwrite those earlier in the list.
     *
     * @param maps whose contents to combine
     * @param <K> type of key
     * @param <V> type of value
     * @return map with contents of input maps
     </V></K> */
    @SafeVarargs
    private fun <K, V> mergeMaps(vararg maps: MutableMap<K?, V?>?): MutableMap<K?, V?> {
      val outputMap: MutableMap<K?, V?> = HashMap()

      for (map in maps) {
        requireNotNull(map!!)
        outputMap.putAll(map)
      }

      return outputMap
    }
  }
}
