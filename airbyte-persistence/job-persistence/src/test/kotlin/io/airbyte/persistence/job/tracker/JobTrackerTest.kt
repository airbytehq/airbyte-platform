/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableMap
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
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.tracker.JobTracker.Companion.configToMetadata
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.Arrays
import java.util.List
import java.util.Map
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
      )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackCheckConnectionSource() {
    val metadata: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(JOB_TYPE, ConfigType.CHECK_CONNECTION_SOURCE)
        .put(JOB_ID_KEY, JOB_ID.toString())
        .put(ATTEMPT_ID, 0)
        .put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
        .put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
        .put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        .put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        .build()

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    whenever(sourceService!!.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper!!.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
      .thenReturn(sourceVersion)
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))

    assertCheckConnCorrectMessageForEachState(
      BiConsumer { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker!!.trackCheckConnectionSource<Any?>(
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
    whenever(actorDefinitionVersionHelper!!.getSourceVersion(sourceDefinition, WORKSPACE_ID, null))
      .thenReturn(sourceVersion)
    assertCheckConnCorrectMessageForEachState(
      BiConsumer { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker!!.trackCheckConnectionSource<Any?>(
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
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackCheckConnectionDestination() {
    val metadata: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(JOB_TYPE, ConfigType.CHECK_CONNECTION_DESTINATION)
        .put(JOB_ID_KEY, JOB_ID.toString())
        .put(ATTEMPT_ID, 0)
        .put("connector_destination", DESTINATION_DEF_NAME)
        .put("connector_destination_definition_id", UUID2)
        .put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
        .put("connector_destination_version", CONNECTOR_VERSION)
        .build()

    val destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME)
    val destinationVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    whenever(destinationService!!.getStandardDestinationDefinition(UUID2))
      .thenReturn(destinationDefinition)
    whenever(
      actorDefinitionVersionHelper!!.getDestinationVersion(
        destinationDefinition,
        WORKSPACE_ID,
        DESTINATION_ID,
      ),
    ).thenReturn(destinationVersion)
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))

    assertCheckConnCorrectMessageForEachState(
      BiConsumer { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker!!.trackCheckConnectionDestination<Any?>(
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
    whenever(actorDefinitionVersionHelper!!.getDestinationVersion(destinationDefinition, WORKSPACE_ID, null))
      .thenReturn(destinationVersion)
    assertCheckConnCorrectMessageForEachState(
      BiConsumer { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker!!.trackCheckConnectionDestination<Any?>(
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
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackDiscover() {
    val metadata: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(JOB_TYPE, ConfigType.DISCOVER_SCHEMA)
        .put(JOB_ID_KEY, JOB_ID.toString())
        .put(ATTEMPT_ID, 0)
        .put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
        .put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
        .put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        .put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        .build()

    val sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME)
    val sourceVersion =
      ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION)
    whenever(sourceService!!.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(actorDefinitionVersionHelper!!.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
      .thenReturn(sourceVersion)
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))

    assertDiscoverCorrectMessageForEachState(
      BiConsumer { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker!!.trackDiscover(
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
    whenever(actorDefinitionVersionHelper!!.getSourceVersion(sourceDefinition, WORKSPACE_ID, null))
      .thenReturn(sourceVersion)
    assertDiscoverCorrectMessageForEachState(
      BiConsumer { jobState: JobTracker.JobState?, output: ConnectorJobOutput? ->
        jobTracker!!.trackDiscover(
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
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackSync() {
    testAsynchronous(ConfigType.SYNC, SYNC_CONFIG_METADATA)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackRefresh() {
    val expectedExtraMetadata: MutableMap<String?, Any?> =
      mergeMaps<String?, Any?>(
        SYNC_CONFIG_METADATA,
        Map.of<String?, Any?>("refresh_types", List.of<String?>(RefreshStream.RefreshType.TRUNCATE.toString())),
      )
    testAsynchronous(ConfigType.REFRESH, expectedExtraMetadata)
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun testTrackSyncForInternalFailure() {
    val jobId = 12345L
    val attemptNumber = 2
    val jobState = JobTracker.JobState.SUCCEEDED
    val exception: Exception = IOException("test")

    whenever(workspaceHelper!!.getWorkspaceForJobIdIgnoreExceptions(jobId)).thenReturn(WORKSPACE_ID)
    whenever(connectionService!!.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(CATALOG)
          .withManual(true),
      )
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))
    whenever(connectionService!!.getStandardSync(CONNECTION_ID))
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

    whenever(sourceService!!.getSourceDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(sourceDefinition)
    whenever(destinationService!!.getDestinationDefinitionFromConnection(CONNECTION_ID))
      .thenReturn(destinationDefinition)
    whenever(sourceService!!.getStandardSourceDefinition(UUID1))
      .thenReturn(sourceDefinition)
    whenever(destinationService!!.getStandardDestinationDefinition(UUID2))
      .thenReturn(destinationDefinition)
    whenever(actorDefinitionVersionHelper!!.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
      .thenReturn(
        ActorDefinitionVersion()
          .withDockerRepository(CONNECTOR_REPOSITORY)
          .withDockerImageTag(CONNECTOR_VERSION)
          .withSpec(SOURCE_SPEC),
      )
    whenever(
      actorDefinitionVersionHelper!!.getDestinationVersion(
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

    jobTracker!!.trackSyncForInternalFailure(jobId, CONNECTION_ID, attemptNumber, jobState, exception)
    val metadata: MutableMap<String?, Any?> = mutableMapOf<String?, Any?>()
    metadata.put("namespace_definition", JobSyncConfig.NamespaceDefinitionType.SOURCE)
    metadata.put("number_of_streams", 1)
    metadata.put("internal_error_type", exception.javaClass.getName())
    metadata.put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
    metadata.put("internal_error_cause", exception.message)
    metadata.put(FREQUENCY_KEY, "1 min")
    metadata.put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
    metadata.put("workspace_id", WORKSPACE_ID)
    metadata.put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
    metadata.put(ATTEMPT_STAGE_KEY, "ENDED")
    metadata.put("attempt_completion_status", jobState)
    metadata.put("connection_id", CONNECTION_ID)
    metadata.put(JOB_ID_KEY, jobId.toString())
    metadata.put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
    metadata.put("connector_destination_version", CONNECTOR_VERSION)
    metadata.put("attempt_id", attemptNumber)
    metadata.put("connector_destination", DESTINATION_DEF_NAME)
    metadata.put("operation_count", 0)
    metadata.put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
    metadata.put("table_prefix", false)
    metadata.put("workspace_name", WORKSPACE_NAME)
    metadata.put("connector_destination_definition_id", UUID2)
    metadata.put("source_id", SOURCE_ID)
    metadata.put("destination_id", DESTINATION_ID)

    verify(trackingClient).track(
      WORKSPACE_ID,
      ScopeType.WORKSPACE,
      JobTracker.INTERNAL_FAILURE_SYNC_EVENT,
      metadata
        .mapKeys {
          it.key!!
        }.mapValues { it.value },
    )
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackReset() {
    testAsynchronous(ConfigType.RESET_CONNECTION)
  }

  // todo update with connection-specific test
  @JvmOverloads
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testAsynchronous(
    configType: ConfigType,
    additionalExpectedMetadata: MutableMap<String?, Any?>? = mutableMapOf<String?, Any?>(),
  ) {
    // for sync the job id is a long not a uuid.
    val jobId = 10L
    whenever(workspaceHelper!!.getWorkspaceForJobIdIgnoreExceptions(jobId)).thenReturn(WORKSPACE_ID)

    val metadata = getJobMetadata(configType, jobId)
    val job = getJobMock(configType, jobId)

    // test when frequency is manual.
    whenever(connectionService!!.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withCatalog(CATALOG)
          .withManual(true),
      )
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))
    val manualMetadata: MutableMap<String?, Any?> =
      mergeMaps<String?, Any?>(
        metadata,
        Map.of<String?, Any?>(FREQUENCY_KEY, "manual"),
        additionalExpectedMetadata,
      )
    assertCorrectMessageForEachState(Consumer { jobState: JobTracker.JobState? -> jobTracker!!.trackSync(job, jobState!!) }, manualMetadata)

    // test when frequency is scheduled.
    whenever(connectionService!!.getStandardSync(CONNECTION_ID))
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
      mergeMaps<String?, Any?>(
        metadata,
        Map.of<String?, Any?>(FREQUENCY_KEY, "1 min"),
        additionalExpectedMetadata,
      )
    assertCorrectMessageForEachState(Consumer { jobState: JobTracker.JobState? -> jobTracker!!.trackSync(job, jobState!!) }, scheduledMetadata)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackSyncAttempt() {
    testAsynchronousAttempt(ConfigType.SYNC, SYNC_CONFIG_METADATA)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackResetAttempt() {
    testAsynchronousAttempt(ConfigType.RESET_CONNECTION)
  }

  // todo (cgardens)
  @Disabled
  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testTrackSyncAttemptWithFailures() {
    testAsynchronousAttemptWithFailures(ConfigType.SYNC, SYNC_CONFIG_METADATA)
  }

  @Test
  @Throws(IOException::class)
  fun testConfigToMetadata() {
    val configJson = read("example_config.json")
    val config = deserialize(configJson)

    val schemaJson = read("example_config_schema.json")
    val schema = deserialize(schemaJson)

    val expected: MutableMap<String?, Any?> =
      ImmutableMap
        .Builder<String?, Any?>()
        .put("username", JobTracker.SET)
        .put("has_ssl", false)
        .put("password", JobTracker.SET)
        .put("one_of.type_key", "foo")
        .put("one_of.some_key", JobTracker.SET)
        .put("const_object.sub_key", "bar")
        .put("const_object.sub_array", "[1,2,3]")
        .put("const_object.sub_object.sub_sub_key", "baz")
        .put("enum_string", "foo")
        .put("additionalPropertiesUnset.foo", JobTracker.SET)
        .put("additionalPropertiesBoolean.foo", JobTracker.SET)
        .put("additionalPropertiesSchema.foo", JobTracker.SET)
        .put("additionalPropertiesConst.foo", 42)
        .put("additionalPropertiesEnumString", "foo")
        .build()

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
        mutableListOf<Attempt>(),
        JobStatus.RUNNING,
        0L,
        0L,
        0L,
        true,
      )

    val metadata = jobTracker!!.generateJobMetadata(jobId, configType, attemptId, Optional.of<Job>(previousJob)).toMutableMap()
    Assertions.assertEquals(jobId, metadata.get("job_id"))
    Assertions.assertEquals(attemptId, metadata.get("attempt_id"))
    Assertions.assertEquals(configType, metadata.get("job_type"))
    Assertions.assertEquals(ConfigType.RESET_CONNECTION, metadata.get("previous_job_type"))
  }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testAsynchronousAttempt(
    configType: ConfigType,
    additionalExpectedMetadata: MutableMap<String?, Any?>?,
  ) {
    testAsynchronousAttempt(configType, getJobWithAttemptsMock(configType, LONG_JOB_ID), additionalExpectedMetadata)
  }

  @JvmOverloads
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testAsynchronousAttempt(
    configType: ConfigType?,
    job: Job =
      getJobWithAttemptsMock(
        configType!!,
        LONG_JOB_ID,
      ),
    additionalExpectedMetadata: MutableMap<String?, Any?>? = mutableMapOf<String?, Any?>(),
  ) {
    val metadata = getJobMetadata(configType, LONG_JOB_ID)
    // test when frequency is manual.
    whenever(connectionService!!.getStandardSync(CONNECTION_ID))
      .thenReturn(
        StandardSync()
          .withConnectionId(CONNECTION_ID)
          .withSourceId(SOURCE_ID)
          .withDestinationId(DESTINATION_ID)
          .withManual(true)
          .withCatalog(CATALOG),
      )
    whenever(workspaceHelper!!.getWorkspaceForJobIdIgnoreExceptions(LONG_JOB_ID)).thenReturn(WORKSPACE_ID)
    whenever(workspaceService!!.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
      .thenReturn(StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME))
    val manualMetadata: MutableMap<String?, Any?> =
      mergeMaps<String?, Any?>(
        ATTEMPT_METADATA,
        metadata,
        Map.of<String?, Any?>(FREQUENCY_KEY, "manual"),
        additionalExpectedMetadata,
      )

    jobTracker!!.trackSync(job, JobTracker.JobState.SUCCEEDED)
    assertCorrectMessageForSucceededState(JobTracker.SYNC_EVENT, manualMetadata)

    jobTracker!!.trackSync(job, JobTracker.JobState.FAILED)
    assertCorrectMessageForFailedState(JobTracker.SYNC_EVENT, manualMetadata)
  }

  private fun configFailureJson(): JsonNode {
    val linkedHashMap: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
    linkedHashMap.put("failureOrigin", "source")
    linkedHashMap.put("failureType", "config_error")
    linkedHashMap.put("internalMessage", "Internal config error error msg")
    linkedHashMap.put("externalMessage", "Config error related msg")
    linkedHashMap.put(METADATA, ImmutableMap.of<String?, String?>(SOME, METADATA))
    linkedHashMap.put("retryable", true)
    linkedHashMap.put("timestamp", 1010)
    return jsonNode<MutableMap<String?, Any?>?>(linkedHashMap)
  }

  private fun systemFailureJson(): JsonNode {
    val linkedHashMap1: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
    linkedHashMap1.put("failureOrigin", "replication")
    linkedHashMap1.put("failureType", "system_error")
    linkedHashMap1.put("internalMessage", "Internal system error error msg")
    linkedHashMap1.put("externalMessage", "System error related msg")
    linkedHashMap1.put(METADATA, ImmutableMap.of<String?, String?>(SOME, METADATA))
    linkedHashMap1.put("retryable", true)
    linkedHashMap1.put("timestamp", 1100)
    return jsonNode<MutableMap<String?, Any?>?>(linkedHashMap1)
  }

  private fun unknownFailureJson(): JsonNode {
    val linkedHashMap2: MutableMap<String?, Any?> = LinkedHashMap<String?, Any?>()
    linkedHashMap2.put("failureOrigin", null)
    linkedHashMap2.put("failureType", null)
    linkedHashMap2.put("internalMessage", "Internal unknown error error msg")
    linkedHashMap2.put("externalMessage", "Unknown error related msg")
    linkedHashMap2.put(METADATA, ImmutableMap.of<String?, String?>(SOME, METADATA))
    linkedHashMap2.put("retryable", true)
    linkedHashMap2.put("timestamp", 1110)
    return jsonNode<MutableMap<String?, Any?>?>(linkedHashMap2)
  }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testAsynchronousAttemptWithFailures(
    configType: ConfigType,
    additionalExpectedMetadata: MutableMap<String?, Any?>,
  ) {
    val failureMetadata: MutableMap<String?, Any?> =
      ImmutableMap.of<String?, Any?>(
        "failure_reasons",
        arrayNode().addAll(Arrays.asList<JsonNode?>(configFailureJson(), systemFailureJson(), unknownFailureJson())).toString(),
        "main_failure_reason",
        configFailureJson().toString(),
      )
    testAsynchronousAttempt(
      configType,
      getJobWithFailuresMock(configType, LONG_JOB_ID),
      mergeMaps<String?, Any?>(additionalExpectedMetadata, failureMetadata),
    )
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  private fun getJobMock(
    configType: ConfigType,
    jobId: Long,
  ): Job = getJobMock(configType, jobId, null)

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
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
        List.of<ConfiguredAirbyteStream>(
          ConfiguredAirbyteStream(
            AirbyteStream("stream", emptyObject(), List.of<SyncMode?>(SyncMode.FULL_REFRESH)).withNamespace("namespace"),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    val attemptSyncConfig =
      AttemptSyncConfig()
        .withSourceConfiguration(jsonNode<ImmutableMap<String?, String?>?>(ImmutableMap.of<String?, String?>(KEY, "some_value")))
        .withDestinationConfiguration(jsonNode<ImmutableMap<String?, Boolean?>?>(ImmutableMap.of<String?, Boolean?>(KEY, false)))

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
              .map<RefreshStream?> { s: ConfiguredAirbyteStream? ->
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
      if (attempts != null) attempts.filterNotNull() else listOf(defaultAttempt),
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
            jsonNode<ImmutableMap<String?, String?>?>(
              ImmutableMap.of<String?, String?>(KEY, "some_value"),
            ),
          ).withDestinationConfiguration(
            jsonNode<ImmutableMap<String?, Boolean?>?>(
              ImmutableMap.of<String?, Boolean?>(KEY, false),
            ),
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
      whenever(attempt.getOutput()).thenReturn(Optional.of(jobOutput))
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

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  private fun getJobWithAttemptsMock(
    configType: ConfigType,
    jobId: Long,
  ): Job = getJobWithAttemptsMock(configType, jobId, mutableListOf<Attempt?>(this.attemptMock))

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  private fun getJobWithAttemptsMock(
    configType: ConfigType,
    jobId: Long,
    attempts: MutableList<Attempt?>?,
  ): Job {
    val job = getJobMock(configType, jobId, attempts)
    whenever(jobPersistence!!.getJob(jobId)).thenReturn(job)
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
      whenever(singleFailureSummary.getFailures())
        .thenReturn(List.of<FailureReason?>(this.configFailureReasonMock))
      whenever(attemptWithSingleFailure.getFailureSummary())
        .thenReturn(Optional.of<AttemptFailureSummary>(singleFailureSummary))

      val attemptWithMultipleFailures = this.attemptMock
      val multipleFailuresSummary = mock<AttemptFailureSummary>()
      whenever(multipleFailuresSummary.getFailures())
        .thenReturn(List.of<FailureReason?>(this.systemFailureReasonMock, this.unknownFailureReasonMock))
      whenever(attemptWithMultipleFailures.getFailureSummary())
        .thenReturn(Optional.of<AttemptFailureSummary>(multipleFailuresSummary))

      val attemptWithNoFailures = this.attemptMock
      whenever(attemptWithNoFailures.getFailureSummary())
        .thenReturn(Optional.empty<AttemptFailureSummary>())

      // in non-test cases we shouldn't actually get failures out of order chronologically
      // this is to verify that we are explicitly sorting the results with tracking failure metadata
      return List.of<Attempt?>(attemptWithMultipleFailures, attemptWithSingleFailure, attemptWithNoFailures)
    }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  private fun getJobWithFailuresMock(
    configType: ConfigType,
    jobId: Long,
  ): Job = getJobWithAttemptsMock(configType, jobId, this.attemptsWithFailuresMock)

  private fun getJobMetadata(
    configType: ConfigType?,
    jobId: Long,
  ): MutableMap<String?, Any?> =
    ImmutableMap
      .builder<String?, Any?>()
      .put(JOB_TYPE, if (configType != ConfigType.RESET_CONNECTION) configType else ConfigType.CLEAR)
      .put(JOB_ID_KEY, jobId.toString())
      .put(ATTEMPT_ID, 1)
      .put("connection_id", CONNECTION_ID)
      .put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
      .put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
      .put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
      .put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
      .put("connector_destination", DESTINATION_DEF_NAME)
      .put("connector_destination_definition_id", UUID2)
      .put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
      .put("connector_destination_version", CONNECTOR_VERSION)
      .put("namespace_definition", JobSyncConfig.NamespaceDefinitionType.SOURCE)
      .put("table_prefix", false)
      .put("operation_count", 0)
      .put("number_of_streams", 1)
      .put("source_id", SOURCE_ID)
      .put("destination_id", DESTINATION_ID)
      .build()

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
    connectionCheckSuccessOutput.setStatus(StandardCheckConnectionOutput.Status.SUCCEEDED)
    val checkConnectionSuccessJobOutput = ConnectorJobOutput().withCheckConnection(connectionCheckSuccessOutput)
    jobStateConsumer.accept(JobTracker.JobState.SUCCEEDED, checkConnectionSuccessJobOutput)
    val checkConnSuccessMetadata: MutableMap<String?, Any?> = ImmutableMap.of<String?, Any?>("check_connection_outcome", "succeeded")

    val connectionCheckFailureOutput = StandardCheckConnectionOutput()
    connectionCheckFailureOutput.setStatus(StandardCheckConnectionOutput.Status.FAILED)
    connectionCheckFailureOutput.setMessage("Please check your Personal Access Token.")
    val checkConnectionFailureJobOutput = ConnectorJobOutput().withCheckConnection(connectionCheckFailureOutput)
    jobStateConsumer.accept(
      JobTracker.JobState.SUCCEEDED,
      checkConnectionFailureJobOutput,
    ) // The job still succeeded, only the connection check failed
    val checkConnFailureMetadata: MutableMap<String?, Any?> =
      ImmutableMap.of<String?, Any?>(
        "check_connection_outcome",
        "failed",
        "check_connection_message",
        "Please check your Personal Access Token.",
      )

    // Failure implies the job threw an exception which almost always meant no output.
    val failedCheckJobOutput = ConnectorJobOutput()
    failedCheckJobOutput.setFailureReason(this.configFailureReasonMock)
    jobStateConsumer.accept(JobTracker.JobState.FAILED, failedCheckJobOutput)
    val failedCheckJobMetadata: MutableMap<String?, Any?> = ImmutableMap.of<String?, Any?>("failure_reason", configFailureJson().toString())

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
    failedDiscoverOutput.setFailureReason(this.systemFailureReasonMock)
    jobStateConsumer.accept(JobTracker.JobState.FAILED, failedDiscoverOutput)

    val failedDiscoverMetadata: MutableMap<String?, Any?> = ImmutableMap.of<String?, Any?>("failure_reason", systemFailureJson().toString())

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

  private fun mockWorkspaceInfo(): MutableMap<String?, Any?> {
    val map: MutableMap<String?, Any?> = HashMap<String?, Any?>()
    map.put("workspace_id", WORKSPACE_ID)
    map.put("workspace_name", WORKSPACE_NAME)
    return map
  }

  companion object {
    private val OBJECT_MAPPER = ObjectMapper()

    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val WORKSPACE_NAME = "WORKSPACE_TEST"
    private val JOB_ID: UUID = UUID.randomUUID()
    private val UUID1: UUID = UUID.randomUUID()
    private val UUID2: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
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

    private val STARTED_STATE_METADATA: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(ATTEMPT_STAGE_KEY, "STARTED")
        .build()
    private val SUCCEEDED_STATE_METADATA: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(ATTEMPT_STAGE_KEY, "ENDED")
        .put("attempt_completion_status", JobTracker.JobState.SUCCEEDED)
        .build()
    private val FAILED_STATE_METADATA: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(ATTEMPT_STAGE_KEY, "ENDED")
        .put("attempt_completion_status", JobTracker.JobState.FAILED)
        .build()
    private val ATTEMPT_METADATA: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put("sync_start_time", SYNC_START_TIME)
        .put("duration", SYNC_DURATION)
        .put("volume_rows", SYNC_RECORDS_SYNC)
        .put("volume_mb", SYNC_BYTES_SYNC)
        .put("count_state_messages_from_source", 3L)
        .put("count_state_messages_from_destination", 1L)
        .put("max_seconds_before_source_state_message_emitted", 5L)
        .put("mean_seconds_before_source_state_message_emitted", 4L)
        .put("max_seconds_between_state_message_emit_and_commit", 7L)
        .put("mean_seconds_between_state_message_emit_and_commit", 6L)
        .put("replication_start_time", 7L)
        .put("replication_end_time", 8L)
        .put("source_read_start_time", 9L)
        .put("source_read_end_time", 10L)
        .put("destination_write_start_time", 11L)
        .put("destination_write_end_time", 12L)
        .build()
    private val SYNC_CONFIG_METADATA: MutableMap<String?, Any?> =
      ImmutableMap
        .builder<String?, Any?>()
        .put(JobTracker.CONFIG + ".source", "{\"key\":\"set\"}")
        .put(JobTracker.CONFIG + ".destination", "{\"key\":false}")
        .put(JobTracker.CATALOG + ".sync_mode.full_refresh", JobTracker.SET)
        .put(JobTracker.CATALOG + ".destination_sync_mode.append", JobTracker.SET)
        .put("namespace_definition", JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .put("table_prefix", false)
        .put("operation_count", 0)
        .put("source_id", SOURCE_ID)
        .put("destination_id", DESTINATION_ID)
        .build()
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
      val outputMap: MutableMap<K?, V?> = HashMap<K?, V?>()

      for (map in maps) {
        Preconditions.checkNotNull<MutableMap<K?, V?>?>(map!!)
        outputMap.putAll(map)
      }

      return outputMap
    }
  }
}
