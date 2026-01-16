/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
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
    jobPersistence = mockk<JobPersistence>(relaxed = true)
    workspaceHelper = mockk<WorkspaceHelper>()
    trackingClient =
      mockk<TrackingClient> {
        every { track(any(), any(), any(), any()) } returns Unit
      }
    sourceService = mockk<SourceService>()
    destinationService = mockk<DestinationService>(relaxed = true)
    connectionService = mockk<ConnectionService>()
    operationService = mockk<OperationService>()
    workspaceService = mockk<WorkspaceService>()
    actorDefinitionVersionHelper = mockk<ActorDefinitionVersionHelper>(relaxed = true)
    jobService = mockk<JobService>()
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
        mockk<MetricClient>(relaxed = true),
      )
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, false) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME).withDataplaneGroupId(DATA_PLANE_GROUP_ID)
  }

  @Test
  fun testTrackCheckConnectionSource() {
    val metadata: Map<String, Any?> =
      mapOf(
        JOB_TYPE to ConfigType.CHECK_CONNECTION_SOURCE,
        JOB_ID_KEY to JOB_ID.toString(),
        ATTEMPT_ID to 0,
        CONNECTOR_SOURCE_KEY to SOURCE_DEF_NAME,
        CONNECTOR_SOURCE_DEFINITION_ID_KEY to UUID1,
        CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY to CONNECTOR_REPOSITORY,
        CONNECTOR_SOURCE_VERSION_KEY to CONNECTOR_VERSION,
        WORKSPACE_ID_KEY to WORKSPACE_ID,
        WORKSPACE_NAME_KEY to WORKSPACE_NAME,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    every { sourceService.getStandardSourceDefinition(UUID1) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID) } returns sourceVersion
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)

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
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, null) } returns sourceVersion
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
    val metadata: Map<String, Any?> =
      mapOf(
        JOB_TYPE to ConfigType.CHECK_CONNECTION_DESTINATION,
        JOB_ID_KEY to JOB_ID.toString(),
        ATTEMPT_ID to 0,
        "connector_destination" to DESTINATION_DEF_NAME,
        "connector_destination_definition_id" to UUID2,
        "connector_destination_docker_repository" to CONNECTOR_REPOSITORY,
        "connector_destination_version" to CONNECTOR_VERSION,
        WORKSPACE_ID_KEY to WORKSPACE_ID,
        WORKSPACE_NAME_KEY to WORKSPACE_NAME,
      )

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    every { destinationService.getStandardDestinationDefinition(UUID2) } returns destinationDefinition
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      )
    } returns destinationVersion
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)

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
    every { actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, null) } returns destinationVersion
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
    val metadata: Map<String, Any?> =
      mapOf(
        JOB_TYPE to ConfigType.DISCOVER_SCHEMA,
        JOB_ID_KEY to JOB_ID.toString(),
        ATTEMPT_ID to 0,
        CONNECTOR_SOURCE_KEY to SOURCE_DEF_NAME,
        CONNECTOR_SOURCE_DEFINITION_ID_KEY to UUID1,
        CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY to CONNECTOR_REPOSITORY,
        CONNECTOR_SOURCE_VERSION_KEY to CONNECTOR_VERSION,
        WORKSPACE_ID_KEY to WORKSPACE_ID,
        WORKSPACE_NAME_KEY to WORKSPACE_NAME,
      )

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    every { sourceService.getStandardSourceDefinition(UUID1) } returns sourceDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID) } returns sourceVersion
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)

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
    )
  }

  @Test
  fun testTrackSync() {
    testAsynchronous(ConfigType.SYNC, mergeMaps(SYNC_CONFIG_METADATA, mapOf(WORKSPACE_ID_KEY to WORKSPACE_ID, WORKSPACE_NAME_KEY to WORKSPACE_NAME)))
  }

  @Test
  fun testTrackRefresh() {
    val expectedExtraMetadata: Map<String, Any?> =
      mergeMaps(
        SYNC_CONFIG_METADATA,
        mapOf("refresh_types" to listOf(RefreshStream.RefreshType.TRUNCATE.toString()), ATTEMPT_STAGE_KEY to "ENDED"),
      )
    testAsynchronous(ConfigType.REFRESH, expectedExtraMetadata)
  }

  @Test
  fun testTrackSyncForInternalFailure() {
    val jobId = 12345L
    val attemptNumber = 2
    val jobState = JobTracker.JobState.SUCCEEDED
    val exception: Exception = IOException("test")

    every { workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId) } returns WORKSPACE_ID
    every { connectionService.getStandardSync(CONNECTION_ID) } returns
      StandardSync()
        .withConnectionId(CONNECTION_ID)
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withCatalog(CATALOG)
        .withManual(true)
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)
    every { connectionService.getStandardSync(CONNECTION_ID) } returns
      StandardSync()
        .withConnectionId(CONNECTION_ID)
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withCatalog(CATALOG)
        .withManual(false)
        .withSchedule(Schedule().withUnits(1L).withTimeUnit(Schedule.TimeUnit.MINUTES))

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME)

    every { sourceService.getSourceDefinitionFromConnection(CONNECTION_ID) } returns sourceDefinition
    every { destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID) } returns destinationDefinition
    every { sourceService.getStandardSourceDefinition(UUID1) } returns sourceDefinition
    every { destinationService.getStandardDestinationDefinition(UUID2) } returns destinationDefinition
    every { actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID) } returns
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(SOURCE_SPEC)
    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      )
    } returns
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(DESTINATION_SPEC)

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
        put(WORKSPACE_ID_KEY, WORKSPACE_ID)
        put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        put(ATTEMPT_STAGE_KEY, "ENDED")
        put(ATTEMPT_COMPLETION_STATUS_KEY, jobState)
        put("connection_id", CONNECTION_ID)
        put(JOB_ID_KEY, jobId.toString())
        put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        put("connector_destination_version", CONNECTOR_VERSION)
        put("attempt_id", attemptNumber)
        put("connector_destination", DESTINATION_DEF_NAME)
        put("operation_count", 0)
        put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
        put("table_prefix", false)
        put(WORKSPACE_NAME_KEY, WORKSPACE_NAME)
        put("connector_destination_definition_id", UUID2)
        put("source_id", SOURCE_ID)
        put("destination_id", DESTINATION_ID)
      }
    verify {
      trackingClient.track(
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
  }

  @Test
  fun testTrackReset() {
    testAsynchronous(ConfigType.RESET_CONNECTION, mapOf(WORKSPACE_ID_KEY to WORKSPACE_ID, WORKSPACE_NAME_KEY to WORKSPACE_NAME))
  }

  // todo update with connection-specific test
  @JvmOverloads
  fun testAsynchronous(
    configType: ConfigType,
    additionalExpectedMetadata: Map<String, Any?>? = emptyMap(),
  ) {
    // for sync the job id is a long not a uuid.
    val jobId = 10L
    every { workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId) } returns WORKSPACE_ID

    val metadata = getJobMetadata(configType, jobId)
    val job = getJobMock(configType, jobId)

    // test when frequency is manual.
    every { connectionService.getStandardSync(CONNECTION_ID) } returns
      StandardSync()
        .withConnectionId(CONNECTION_ID)
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withCatalog(CATALOG)
        .withManual(true)

    every { actorDefinitionVersionHelper.getSourceVersion(any(), WORKSPACE_ID, SOURCE_ID) } returns
      ActorDefinitionVersion()
        .withActorDefinitionId(SOURCE_DEFINITION_ID)
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(SOURCE_SPEC)
    every { actorDefinitionVersionHelper.getDestinationVersion(any(), WORKSPACE_ID, DESTINATION_ID) } returns
      ActorDefinitionVersion()
        .withActorDefinitionId(DESTINATION_DEFINITION_ID)
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(DESTINATION_SPEC)

    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)
    val manualMetadata: Map<String, Any?> =
      mergeMaps(
        metadata,
        mapOf(FREQUENCY_KEY to "manual", ATTEMPT_STAGE_KEY to "ENDED"),
        additionalExpectedMetadata,
      )
    assertCorrectMessageForEachState({ jobState: JobTracker.JobState? -> jobTracker.trackSync(job, jobState!!) }, manualMetadata)

    // Clear mocks before the next scenario
    clearMocks(trackingClient, answers = false)

    // test when frequency is scheduled.
    every { connectionService.getStandardSync(CONNECTION_ID) } returns
      StandardSync()
        .withConnectionId(CONNECTION_ID)
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withCatalog(CATALOG)
        .withManual(false)
        .withSchedule(Schedule().withUnits(1L).withTimeUnit(Schedule.TimeUnit.MINUTES))
    val scheduledMetadata: Map<String, Any?> =
      mergeMaps(
        metadata,
        mapOf(FREQUENCY_KEY to "1 min", ATTEMPT_STAGE_KEY to "ENDED"),
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
    testAsynchronousAttempt(ConfigType.RESET_CONNECTION, mapOf(WORKSPACE_ID_KEY to WORKSPACE_ID, WORKSPACE_NAME_KEY to WORKSPACE_NAME))
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

    val expected: Map<String, Any?> =
      mapOf(
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

    val actual = configToMetadata(config, schema)

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

    val metadata = jobTracker.generateJobMetadata(jobId, configType, attemptId, Optional.of<Job>(previousJob))
    Assertions.assertEquals(jobId, metadata["job_id"])
    Assertions.assertEquals(attemptId, metadata["attempt_id"])
    Assertions.assertEquals(configType, metadata["job_type"])
    Assertions.assertEquals(ConfigType.RESET_CONNECTION, metadata["previous_job_type"])
  }

  fun testAsynchronousAttempt(
    configType: ConfigType,
    additionalExpectedMetadata: Map<String, Any?>?,
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
    additionalExpectedMetadata: Map<String, Any?>? = emptyMap(),
  ) {
    clearMocks(trackingClient, answers = false)
    val metadata = getJobMetadata(configType, LONG_JOB_ID)
    // test when frequency is manual.
    every { connectionService.getStandardSync(CONNECTION_ID) } returns
      StandardSync()
        .withConnectionId(CONNECTION_ID)
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withManual(true)
        .withCatalog(CATALOG)

    every { actorDefinitionVersionHelper.getSourceVersion(any(), WORKSPACE_ID, SOURCE_ID) } returns
      ActorDefinitionVersion()
        .withActorDefinitionId(SOURCE_DEFINITION_ID)
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(SOURCE_SPEC)
    every { actorDefinitionVersionHelper.getDestinationVersion(any(), WORKSPACE_ID, DESTINATION_ID) } returns
      ActorDefinitionVersion()
        .withActorDefinitionId(DESTINATION_DEFINITION_ID)
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withSpec(DESTINATION_SPEC)

    every { workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(LONG_JOB_ID) } returns WORKSPACE_ID
    every { workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true) } returns
      StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME)
    val manualMetadata: Map<String, Any?> =
      mergeMaps(
        ATTEMPT_METADATA,
        metadata,
        mapOf(
          FREQUENCY_KEY to "manual",
          ATTEMPT_STAGE_KEY to "ENDED",
          ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.SUCCEEDED,
        ),
        additionalExpectedMetadata,
      )

    jobTracker.trackSync(job, JobTracker.JobState.SUCCEEDED)
    assertCorrectMessageForSucceededState(JobTracker.SYNC_EVENT, manualMetadata)

    jobTracker.trackSync(job, JobTracker.JobState.FAILED)
    assertCorrectMessageForFailedState(JobTracker.SYNC_EVENT, manualMetadata)
  }

  private fun configFailureJson(): JsonNode {
    val linkedHashMap: Map<String, Any?> =
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
    val linkedHashMap1: Map<String, Any?> =
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
    val linkedHashMap2: Map<String, Any?> =
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
    additionalExpectedMetadata: Map<String, Any?>,
  ) {
    val failureMetadata: Map<String, Any?> =
      mapOf(
        "failure_reasons" to arrayNode().addAll(listOf(configFailureJson(), systemFailureJson(), unknownFailureJson())).toString(),
        "main_failure_reason" to configFailureJson().toString(),
      )
    testAsynchronousAttempt(
      configType,
      getJobWithFailuresMock(configType, LONG_JOB_ID),
      mergeMaps(additionalExpectedMetadata, failureMetadata),
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

    every { sourceService.getSourceDefinitionFromConnection(CONNECTION_ID) } returns sourceDefinition
    every {
      destinationService.getDestinationDefinitionFromConnection(
        CONNECTION_ID,
      )
    } returns destinationDefinition
    every { sourceService.getStandardSourceDefinition(UUID1) } returns sourceDefinition
    every { destinationService.getStandardDestinationDefinition(UUID2) } returns destinationDefinition

    every {
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        WORKSPACE_ID,
        SOURCE_ID,
      )
    } returns
      ActorDefinitionVersion()
        .withSpec(SOURCE_SPEC)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withDockerRepository(CONNECTOR_REPOSITORY)

    every {
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      )
    } returns
      ActorDefinitionVersion()
        .withSpec(DESTINATION_SPEC)
        .withDockerImageTag(CONNECTOR_VERSION)
        .withDockerRepository(CONNECTOR_REPOSITORY)

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

    val jobConfig = mockk<JobConfig>(relaxed = true)
    every { jobConfig.configType } returns configType

    if (configType == ConfigType.SYNC) {
      val jobSyncConfig =
        JobSyncConfig()
          .withConfiguredAirbyteCatalog(catalog)
      every { jobConfig.sync } returns jobSyncConfig
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
      every { jobConfig.refresh } returns refreshConfig
    }
    if (configType == ConfigType.RESET_CONNECTION) {
      val jobSyncConfig =
        JobSyncConfig()
          .withConfiguredAirbyteCatalog(catalog)
      every { jobConfig.sync } returns jobSyncConfig
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

    val job =
      Job(
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

    // Mock listJobsIncludingId to return just this job
    every {
      jobPersistence.listJobsIncludingId(
        setOf(ConfigType.SYNC, ConfigType.RESET_CONNECTION, ConfigType.REFRESH),
        CONNECTION_ID.toString(),
        jobId,
        2,
      )
    } returns listOf(job)

    return job
  }

  private val attemptMock: Attempt
    get() {
      val attempt = mockk<Attempt>(relaxed = true)
      val jobOutput = mockk<JobOutput>(relaxed = true)
      val syncOutput = mockk<StandardSyncOutput>(relaxed = true)
      val syncSummary = mockk<StandardSyncSummary>(relaxed = true)
      val syncStats = mockk<SyncStats>(relaxed = true)

      val attemptSyncConfig =
        AttemptSyncConfig()
          .withSourceConfiguration(
            jsonNode(mapOf(KEY to "some_value")),
          ).withDestinationConfiguration(
            jsonNode(mapOf(KEY to false)),
          )
      every { attempt.syncConfig } returns Optional.of(attemptSyncConfig).orElse(null)
      every { syncSummary.startTime } returns SYNC_START_TIME
      every { syncSummary.endTime } returns SYNC_END_TIME
      every { syncSummary.bytesSynced } returns SYNC_BYTES_SYNC
      every { syncSummary.recordsSynced } returns SYNC_RECORDS_SYNC
      every { syncOutput.standardSyncSummary } returns syncSummary
      every { syncSummary.totalStats } returns syncStats
      every { jobOutput.sync } returns syncOutput
      every { attempt.output } returns jobOutput
      every { syncStats.sourceStateMessagesEmitted } returns 3L
      every { syncStats.destinationStateMessagesEmitted } returns 1L
      every { syncStats.maxSecondsBeforeSourceStateMessageEmitted } returns 5L
      every { syncStats.meanSecondsBeforeSourceStateMessageEmitted } returns 4L
      every { syncStats.maxSecondsBetweenStateMessageEmittedandCommitted } returns 7L
      every { syncStats.meanSecondsBetweenStateMessageEmittedandCommitted } returns 6L
      every { syncStats.replicationStartTime } returns 7L
      every { syncStats.replicationEndTime } returns 8L
      every { syncStats.sourceReadStartTime } returns 9L
      every { syncStats.sourceReadEndTime } returns 10L
      every { syncStats.destinationWriteStartTime } returns 11L
      every { syncStats.destinationWriteEndTime } returns 12L

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
    every { jobPersistence.getJob(jobId) } returns job
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
      val singleFailureSummary = mockk<AttemptFailureSummary>()
      every { singleFailureSummary.failures } returns listOf(this.configFailureReasonMock)
      every { attemptWithSingleFailure.failureSummary } returns singleFailureSummary

      val attemptWithMultipleFailures = this.attemptMock
      val multipleFailuresSummary = mockk<AttemptFailureSummary>()
      every { multipleFailuresSummary.failures } returns listOf(this.systemFailureReasonMock, this.unknownFailureReasonMock)
      every { attemptWithMultipleFailures.failureSummary } returns multipleFailuresSummary

      val attemptWithNoFailures = this.attemptMock
      every { attemptWithNoFailures.failureSummary } returns null

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
  ): Map<String, Any?> =
    mapOf(
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
    metadata: Map<String, Any?>,
    workspaceSet: Boolean,
  ) {
    clearMocks(trackingClient)

    // Output does not exist when job has started.
    jobStateConsumer.accept(JobTracker.JobState.STARTED, null)

    val connectionCheckSuccessOutput = StandardCheckConnectionOutput()
    connectionCheckSuccessOutput.status = StandardCheckConnectionOutput.Status.SUCCEEDED
    val checkConnectionSuccessJobOutput = ConnectorJobOutput().withCheckConnection(connectionCheckSuccessOutput)
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED, checkConnectionSuccessJobOutput)
    val checkConnSuccessMetadata: Map<String, Any?> =
      mapOf(
        "check_connection_outcome" to "succeeded",
        ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.SUCCEEDED,
        ATTEMPT_STAGE_KEY to "ENDED",
      )

    val connectionCheckFailureOutput = StandardCheckConnectionOutput()
    connectionCheckFailureOutput.status = StandardCheckConnectionOutput.Status.FAILED
    connectionCheckFailureOutput.message = "Please check your Personal Access Token."
    val checkConnectionFailureJobOutput = ConnectorJobOutput().withCheckConnection(connectionCheckFailureOutput)
    jobStateConsumer.accept(
      JobTracker.JobState.SUCCEEDED,
      checkConnectionFailureJobOutput,
    ) // The job still succeeded, only the connection check failed
    val checkConnFailureMetadata: Map<String, Any?> =
      mapOf(
        "check_connection_outcome" to "failed",
        "check_connection_message" to "Please check your Personal Access Token.",
        ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.SUCCEEDED,
        ATTEMPT_STAGE_KEY to "ENDED",
      )

    // Failure implies the job threw an exception which almost always meant no output.
    val failedCheckJobOutput = ConnectorJobOutput()
    failedCheckJobOutput.failureReason = this.configFailureReasonMock
    jobStateConsumer.accept(JobTracker.JobState.FAILED, failedCheckJobOutput)
    val failedCheckJobMetadata: Map<String, Any?> =
      mapOf(
        "failure_reason" to configFailureJson().toString(),
        ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.FAILED,
        ATTEMPT_STAGE_KEY to "ENDED",
      )

    if (workspaceSet) {
      assertCorrectMessageForStartedState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps(metadata, mapOf(ATTEMPT_STAGE_KEY to JobTracker.JobState.STARTED.name)),
      )
      assertCorrectMessageForSucceededState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps(metadata, checkConnSuccessMetadata),
      )
      assertCorrectMessageForSucceededState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps(metadata, checkConnFailureMetadata),
      )
      assertCorrectMessageForFailedState(
        if (configType ==
          ConfigType.CHECK_CONNECTION_SOURCE
        ) {
          JobTracker.CHECK_CONNECTION_SOURCE_EVENT
        } else {
          JobTracker.CHECK_CONNECTION_DESTINATION_EVENT
        },
        mergeMaps(metadata, failedCheckJobMetadata),
      )
    } else {
      verify(exactly = 0) { trackingClient.track(any(), any(), any(), any()) }
    }
  }

  private fun assertDiscoverCorrectMessageForEachState(
    jobStateConsumer: BiConsumer<JobTracker.JobState?, ConnectorJobOutput?>,
    metadata: Map<String, Any?>,
  ) {
    // Output does not exist when job has started.
    jobStateConsumer.accept(JobTracker.JobState.STARTED, null)

    val discoverCatalogID = UUID.randomUUID()
    val discoverSuccessJobOutput = ConnectorJobOutput().withDiscoverCatalogId(discoverCatalogID)
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED, discoverSuccessJobOutput)

    // Failure implies the job threw an exception which almost always meant no output.
    val failedDiscoverOutput = ConnectorJobOutput()
    failedDiscoverOutput.failureReason = this.systemFailureReasonMock
    jobStateConsumer.accept(JobTracker.JobState.FAILED, failedDiscoverOutput)

    val failedDiscoverMetadata: Map<String, Any?> =
      mapOf(
        "failure_reason" to systemFailureJson().toString(),
        ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.FAILED,
        ATTEMPT_STAGE_KEY to "ENDED",
      )

    assertCorrectMessageForStartedState(JobTracker.DISCOVER_EVENT, mergeMaps(metadata, mapOf(ATTEMPT_STAGE_KEY to JobTracker.JobState.STARTED.name)))
    assertCorrectMessageForSucceededState(
      JobTracker.DISCOVER_EVENT,
      mergeMaps(
        metadata,
        mapOf(ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.SUCCEEDED, ATTEMPT_STAGE_KEY to "ENDED"),
      ),
    )
    assertCorrectMessageForFailedState(JobTracker.DISCOVER_EVENT, mergeMaps(metadata, failedDiscoverMetadata))
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
    expectedMetadata: Map<String, Any?>,
  ) {
    jobStateConsumer.accept(JobTracker.JobState.STARTED)
    assertCorrectMessageForStartedState(
      JobTracker.SYNC_EVENT,
      mergeMaps(expectedMetadata, mapOf(ATTEMPT_STAGE_KEY to JobTracker.JobState.STARTED.name)),
    )
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED)
    assertCorrectMessageForSucceededState(
      JobTracker.SYNC_EVENT,
      mergeMaps(
        expectedMetadata,
        mapOf(ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.SUCCEEDED),
      ),
    )
    jobStateConsumer.accept(JobTracker.JobState.FAILED)
    assertCorrectMessageForFailedState(
      JobTracker.SYNC_EVENT,
      mergeMaps(
        expectedMetadata,
        mapOf(ATTEMPT_COMPLETION_STATUS_KEY to JobTracker.JobState.FAILED),
      ),
    )
  }

  private fun assertCorrectMessageForStartedState(
    action: String,
    metadata: Map<String, Any?>,
  ) {
    verify(atLeast = 1) {
      trackingClient.track(any(), any(), eq(action), match { map -> map == metadata })
    }
  }

  private fun assertCorrectMessageForSucceededState(
    action: String,
    metadata: Map<String, Any?>,
  ) {
    verify(atLeast = 1) {
      trackingClient.track(any(), any(), eq(action), match { map -> map == metadata })
    }
  }

  private fun assertCorrectMessageForFailedState(
    action: String,
    metadata: Map<String, Any?>,
  ) {
    verify(atLeast = 1) {
      trackingClient.track(any(), any(), eq(action), match { map -> map == metadata })
    }
  }

  companion object {
    private val OBJECT_MAPPER = ObjectMapper()

    private const val ATTEMPT_COMPLETION_STATUS_KEY = "attempt_completion_status"
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val WORKSPACE_ID_KEY = "workspace_id"
    private const val WORKSPACE_NAME = "WORKSPACE_TEST"
    private const val WORKSPACE_NAME_KEY = "workspace_name"
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
    private const val SYNC_DURATION = 9 // in sync between end and start time
    private const val SYNC_BYTES_SYNC = 42L
    private const val SYNC_RECORDS_SYNC = 4L
    private const val LONG_JOB_ID = 10L // for sync the job id is a long not a uuid.

    private val ATTEMPT_METADATA: Map<String, Any?> =
      mapOf(
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
    private val SYNC_CONFIG_METADATA: Map<String, Any?> =
      mapOf(
        "connection_id" to CONNECTION_ID,
        "source_id" to SOURCE_ID,
        "destination_id" to DESTINATION_ID,
        WORKSPACE_ID_KEY to WORKSPACE_ID,
        WORKSPACE_NAME_KEY to WORKSPACE_NAME,
        JobTracker.CONFIG + ".source" to "{\"key\":\"set\"}",
        JobTracker.CONFIG + ".destination" to "{\"key\":false}",
        JobTracker.CATALOG + ".sync_mode.full_refresh" to JobTracker.SET,
        JobTracker.CATALOG + ".destination_sync_mode.append" to JobTracker.SET,
        "namespace_definition" to JobSyncConfig.NamespaceDefinitionType.SOURCE,
        "table_prefix" to false,
        "operation_count" to 0,
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
    private fun <K, V> mergeMaps(vararg maps: Map<K, V?>?): Map<K, V?> =
      buildMap {
        maps.filterNotNull().forEach { map -> putAll(map) }
      }
  }
}
