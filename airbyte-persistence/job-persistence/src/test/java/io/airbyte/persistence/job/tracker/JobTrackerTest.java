/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.tracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.DestinationSyncMode;
import io.airbyte.config.FailureReason;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.Metadata;
import io.airbyte.config.RefreshConfig;
import io.airbyte.config.RefreshStream;
import io.airbyte.config.Schedule;
import io.airbyte.config.Schedule.TimeUnit;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.protocol.models.v0.Field;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class JobTrackerTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String WORKSPACE_NAME = "WORKSPACE_TEST";
  private static final UUID JOB_ID = UUID.randomUUID();
  private static final UUID UUID1 = UUID.randomUUID();
  private static final UUID UUID2 = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final String SOURCE_DEF_NAME = "postgres";
  private static final String DESTINATION_DEF_NAME = "bigquery";
  private static final String CONNECTOR_REPOSITORY = "test/test";
  private static final String CONNECTOR_VERSION = "test";
  private static final String JOB_TYPE = "job_type";
  private static final String JOB_ID_KEY = "job_id";
  private static final String ATTEMPT_ID = "attempt_id";
  private static final String METADATA = "metadata";
  private static final String SOME = "some";
  private static final String ATTEMPT_STAGE_KEY = "attempt_stage";
  private static final String CONNECTOR_SOURCE_KEY = "connector_source";
  private static final String CONNECTOR_SOURCE_DEFINITION_ID_KEY = "connector_source_definition_id";
  private static final String CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY = "connector_source_docker_repository";
  private static final String CONNECTOR_SOURCE_VERSION_KEY = "connector_source_version";
  private static final String FREQUENCY_KEY = "frequency";

  private static final long SYNC_START_TIME = 1000L;
  private static final long SYNC_END_TIME = 10000L;
  private static final long SYNC_DURATION = 9L; // in sync between end and start time
  private static final long SYNC_BYTES_SYNC = 42L;
  private static final long SYNC_RECORDS_SYNC = 4L;
  private static final long LONG_JOB_ID = 10L; // for sync the job id is a long not a uuid.

  private static final Map<String, Object> STARTED_STATE_METADATA = ImmutableMap.<String, Object>builder()
      .put(ATTEMPT_STAGE_KEY, "STARTED")
      .build();
  private static final Map<String, Object> SUCCEEDED_STATE_METADATA = ImmutableMap.<String, Object>builder()
      .put(ATTEMPT_STAGE_KEY, "ENDED")
      .put("attempt_completion_status", JobState.SUCCEEDED)
      .build();
  private static final Map<String, Object> FAILED_STATE_METADATA = ImmutableMap.<String, Object>builder()
      .put(ATTEMPT_STAGE_KEY, "ENDED")
      .put("attempt_completion_status", JobState.FAILED)
      .build();
  private static final Map<String, Object> ATTEMPT_METADATA = ImmutableMap.<String, Object>builder()
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
      .build();
  private static final Map<String, Object> SYNC_CONFIG_METADATA = ImmutableMap.<String, Object>builder()
      .put(JobTracker.CONFIG + ".source", "{\"key\":\"set\"}")
      .put(JobTracker.CONFIG + ".destination", "{\"key\":false}")
      .put(JobTracker.CATALOG + ".sync_mode.full_refresh", JobTracker.SET)
      .put(JobTracker.CATALOG + ".destination_sync_mode.append", JobTracker.SET)
      .put("namespace_definition", NamespaceDefinitionType.SOURCE)
      .put("table_prefix", false)
      .put("operation_count", 0)
      .build();
  private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());
  private static final ConfiguredAirbyteCatalog CATALOG = catalogHelpers
      .createConfiguredAirbyteCatalog("stream_name", "stream_namespace",
          Field.of("int_field", JsonSchemaType.NUMBER));

  private static final ConnectorSpecification SOURCE_SPEC;
  private static final ConnectorSpecification DESTINATION_SPEC;

  static {
    try {
      SOURCE_SPEC = new ConnectorSpecification().withConnectionSpecification(OBJECT_MAPPER.readTree(
          """
          {
            "type": "object",
            "properties": {
              "key": {
                "type": "string"
              }
            }
          }
          """));
      DESTINATION_SPEC = new ConnectorSpecification().withConnectionSpecification(OBJECT_MAPPER.readTree(
          """
          {
            "type": "object",
            "properties": {
              "key": {
                "type": "boolean"
              }
            }
          }
          """));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private JobPersistence jobPersistence;
  private TrackingClient trackingClient;
  private WorkspaceHelper workspaceHelper;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private JobTracker jobTracker;

  private SourceService sourceService;
  private DestinationService destinationService;
  private ConnectionService connectionService;
  private OperationService operationService;
  private WorkspaceService workspaceService;

  @BeforeEach
  void setup() {
    jobPersistence = mock(JobPersistence.class);
    workspaceHelper = mock(WorkspaceHelper.class);
    trackingClient = mock(TrackingClient.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);
    connectionService = mock(ConnectionService.class);
    operationService = mock(OperationService.class);
    workspaceService = mock(WorkspaceService.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    jobTracker = new JobTracker(
        jobPersistence,
        workspaceHelper,
        trackingClient,
        actorDefinitionVersionHelper,
        sourceService,
        destinationService,
        connectionService,
        operationService,
        workspaceService);
  }

  @Test
  void testTrackCheckConnectionSource()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final Map<String, Object> metadata = ImmutableMap.<String, Object>builder()
        .put(JOB_TYPE, ConfigType.CHECK_CONNECTION_SOURCE)
        .put(JOB_ID_KEY, JOB_ID.toString())
        .put(ATTEMPT_ID, 0)
        .put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
        .put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
        .put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        .put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        .build();

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME);
    final ActorDefinitionVersion sourceVersion = new ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION);
    when(sourceService.getStandardSourceDefinition(UUID1))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID)).thenReturn(sourceVersion);
    when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME));

    assertCheckConnCorrectMessageForEachState(
        (jobState, output) -> jobTracker.trackCheckConnectionSource(JOB_ID, UUID1, WORKSPACE_ID, SOURCE_ID, jobState, output),
        ConfigType.CHECK_CONNECTION_SOURCE,
        metadata,
        true);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, null, null)).thenReturn(sourceVersion);
    assertCheckConnCorrectMessageForEachState(
        (jobState, output) -> jobTracker.trackCheckConnectionSource(JOB_ID, UUID1, null, null, jobState, output),
        ConfigType.CHECK_CONNECTION_SOURCE,
        metadata,
        false);
  }

  @Test
  void testTrackCheckConnectionDestination()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final Map<String, Object> metadata = ImmutableMap.<String, Object>builder()
        .put(JOB_TYPE, ConfigType.CHECK_CONNECTION_DESTINATION)
        .put(JOB_ID_KEY, JOB_ID.toString())
        .put(ATTEMPT_ID, 0)
        .put("connector_destination", DESTINATION_DEF_NAME)
        .put("connector_destination_definition_id", UUID2)
        .put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
        .put("connector_destination_version", CONNECTOR_VERSION)
        .build();

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME);
    final ActorDefinitionVersion destinationVersion = new ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION);
    when(destinationService.getStandardDestinationDefinition(UUID2))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID)).thenReturn(destinationVersion);
    when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME));

    assertCheckConnCorrectMessageForEachState(
        (jobState, output) -> jobTracker.trackCheckConnectionDestination(JOB_ID, UUID2, WORKSPACE_ID, DESTINATION_ID, jobState, output),
        ConfigType.CHECK_CONNECTION_DESTINATION,
        metadata,
        true);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, null, null)).thenReturn(destinationVersion);
    assertCheckConnCorrectMessageForEachState(
        (jobState, output) -> jobTracker.trackCheckConnectionDestination(JOB_ID, UUID2, null, null, jobState, output),
        ConfigType.CHECK_CONNECTION_DESTINATION,
        metadata,
        false);
  }

  @Test
  void testTrackDiscover() throws IOException, JsonValidationException, ConfigNotFoundException {
    final Map<String, Object> metadata = ImmutableMap.<String, Object>builder()
        .put(JOB_TYPE, ConfigType.DISCOVER_SCHEMA)
        .put(JOB_ID_KEY, JOB_ID.toString())
        .put(ATTEMPT_ID, 0)
        .put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
        .put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
        .put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        .put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        .build();

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME);
    final ActorDefinitionVersion sourceVersion = new ActorDefinitionVersion()
        .withDockerRepository(CONNECTOR_REPOSITORY)
        .withDockerImageTag(CONNECTOR_VERSION);
    when(sourceService.getStandardSourceDefinition(UUID1))
        .thenReturn(sourceDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID)).thenReturn(sourceVersion);
    when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME));

    assertDiscoverCorrectMessageForEachState(
        (jobState, output) -> jobTracker.trackDiscover(JOB_ID, UUID1, WORKSPACE_ID, SOURCE_ID, ActorType.SOURCE, jobState, output),
        metadata,
        true);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, null, null)).thenReturn(sourceVersion);
    assertDiscoverCorrectMessageForEachState(
        (jobState, output) -> jobTracker.trackDiscover(JOB_ID, UUID1, null, null, null, jobState, output),
        metadata,
        false);
  }

  @Test
  void testTrackSync() throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronous(ConfigType.SYNC, SYNC_CONFIG_METADATA);
  }

  @Test
  void testTrackRefresh() throws IOException, JsonValidationException, ConfigNotFoundException {
    final Map<String, Object> expectedExtraMetadata = MoreMaps.merge(
        SYNC_CONFIG_METADATA,
        Map.of("refresh_types", List.of(RefreshStream.RefreshType.TRUNCATE.toString())));
    testAsynchronous(ConfigType.REFRESH, expectedExtraMetadata);
  }

  @Test
  void testTrackSyncForInternalFailure()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final Long jobId = 12345L;
    final Integer attemptNumber = 2;
    final JobState jobState = JobState.SUCCEEDED;
    final Exception exception = new IOException("test");

    when(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)).thenReturn(WORKSPACE_ID);
    when(connectionService.getStandardSync(CONNECTION_ID))
        .thenReturn(new StandardSync()
            .withConnectionId(CONNECTION_ID).withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID).withCatalog(CATALOG)
            .withManual(true));
    when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME));
    when(connectionService.getStandardSync(CONNECTION_ID))
        .thenReturn(new StandardSync()
            .withConnectionId(CONNECTION_ID).withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID).withCatalog(CATALOG)
            .withManual(false).withSchedule(new Schedule().withUnits(1L).withTimeUnit(TimeUnit.MINUTES)));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME);
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME);

    when(sourceService.getSourceDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(sourceDefinition);
    when(destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(destinationDefinition);
    when(sourceService.getStandardSourceDefinition(UUID1))
        .thenReturn(sourceDefinition);
    when(destinationService.getStandardDestinationDefinition(UUID2))
        .thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(CONNECTOR_REPOSITORY)
            .withDockerImageTag(CONNECTOR_VERSION)
            .withSpec(SOURCE_SPEC));
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(CONNECTOR_REPOSITORY)
            .withDockerImageTag(CONNECTOR_VERSION)
            .withSpec(DESTINATION_SPEC));

    jobTracker.trackSyncForInternalFailure(jobId, CONNECTION_ID, attemptNumber, jobState, exception);
    final Map<String, Object> metadata = new LinkedHashMap();
    metadata.put("namespace_definition", NamespaceDefinitionType.SOURCE);
    metadata.put("number_of_streams", 1);
    metadata.put("internal_error_type", exception.getClass().getName());
    metadata.put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME);
    metadata.put("internal_error_cause", exception.getMessage());
    metadata.put(FREQUENCY_KEY, "1 min");
    metadata.put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1);
    metadata.put("workspace_id", WORKSPACE_ID);
    metadata.put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY);
    metadata.put(ATTEMPT_STAGE_KEY, "ENDED");
    metadata.put("attempt_completion_status", jobState);
    metadata.put("connection_id", CONNECTION_ID);
    metadata.put(JOB_ID_KEY, String.valueOf(jobId));
    metadata.put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION);
    metadata.put("connector_destination_version", CONNECTOR_VERSION);
    metadata.put("attempt_id", attemptNumber);
    metadata.put("connector_destination", DESTINATION_DEF_NAME);
    metadata.put("operation_count", 0);
    metadata.put("connector_destination_docker_repository", CONNECTOR_REPOSITORY);
    metadata.put("table_prefix", false);
    metadata.put("workspace_name", WORKSPACE_NAME);
    metadata.put("connector_destination_definition_id", UUID2);
    metadata.put("source_id", SOURCE_ID);
    metadata.put("destination_id", DESTINATION_ID);

    verify(trackingClient).track(WORKSPACE_ID, ScopeType.WORKSPACE, JobTracker.INTERNAL_FAILURE_SYNC_EVENT, metadata);
  }

  @Test
  void testTrackReset() throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronous(ConfigType.RESET_CONNECTION);
  }

  void testAsynchronous(final ConfigType configType)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronous(configType, Collections.emptyMap());
  }

  // todo update with connection-specific test
  void testAsynchronous(final ConfigType configType, final Map<String, Object> additionalExpectedMetadata)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    // for sync the job id is a long not a uuid.
    final long jobId = 10L;
    when(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)).thenReturn(WORKSPACE_ID);

    final Map<String, Object> metadata = getJobMetadata(configType, jobId);
    final Job job = getJobMock(configType, jobId);
    // test when frequency is manual.

    when(connectionService.getStandardSync(CONNECTION_ID))
        .thenReturn(new StandardSync()
            .withConnectionId(CONNECTION_ID)
            .withSourceId(SOURCE_ID)
            .withDestinationId(DESTINATION_ID)
            .withCatalog(CATALOG)
            .withManual(true));
    when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME));
    final Map<String, Object> manualMetadata = MoreMaps.merge(
        metadata,
        Map.of(FREQUENCY_KEY, "manual"),
        additionalExpectedMetadata);
    assertCorrectMessageForEachState((jobState) -> jobTracker.trackSync(job, jobState), manualMetadata);

    // test when frequency is scheduled.
    when(connectionService.getStandardSync(CONNECTION_ID))
        .thenReturn(new StandardSync()
            .withConnectionId(CONNECTION_ID)
            .withSourceId(SOURCE_ID)
            .withDestinationId(DESTINATION_ID)
            .withCatalog(CATALOG)
            .withManual(false)
            .withSchedule(new Schedule().withUnits(1L).withTimeUnit(TimeUnit.MINUTES)));
    final Map<String, Object> scheduledMetadata = MoreMaps.merge(
        metadata,
        Map.of(FREQUENCY_KEY, "1 min"),
        additionalExpectedMetadata);
    assertCorrectMessageForEachState((jobState) -> jobTracker.trackSync(job, jobState), scheduledMetadata);
  }

  @Test
  void testTrackSyncAttempt()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronousAttempt(ConfigType.SYNC, SYNC_CONFIG_METADATA);
  }

  @Test
  void testTrackResetAttempt()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronousAttempt(ConfigType.RESET_CONNECTION);
  }

  @Test
  void testTrackSyncAttemptWithFailures()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronousAttemptWithFailures(ConfigType.SYNC, SYNC_CONFIG_METADATA);
  }

  @Test
  void testConfigToMetadata() throws IOException {
    final String configJson = MoreResources.readResource("example_config.json");
    final JsonNode config = Jsons.deserialize(configJson);

    final String schemaJson = MoreResources.readResource("example_config_schema.json");
    final JsonNode schema = Jsons.deserialize(schemaJson);

    final Map<String, Object> expected = new ImmutableMap.Builder<String, Object>()
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
        .build();

    final Map<String, Object> actual = JobTracker.configToMetadata(config, schema);

    assertEquals(expected, actual);
  }

  @Test
  void testGenerateMetadata() {
    final String jobId = "shouldBeLong";
    final int attemptId = 2;
    final ConfigType configType = ConfigType.REFRESH;
    final Job previousJob = new Job(0, ConfigType.RESET_CONNECTION, null, null, null, null, null, 0L, 0L, true);

    final Map<String, Object> metadata = jobTracker.generateJobMetadata(jobId, configType, attemptId, Optional.of(previousJob));
    assertEquals(jobId, metadata.get("job_id"));
    assertEquals(attemptId, metadata.get("attempt_id"));
    assertEquals(configType, metadata.get("job_type"));
    assertEquals(ConfigType.RESET_CONNECTION, metadata.get("previous_job_type"));
  }

  void testAsynchronousAttempt(final ConfigType configType)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronousAttempt(configType, getJobWithAttemptsMock(configType, LONG_JOB_ID), Collections.emptyMap());
  }

  void testAsynchronousAttempt(final ConfigType configType, final Map<String, Object> additionalExpectedMetadata)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    testAsynchronousAttempt(configType, getJobWithAttemptsMock(configType, LONG_JOB_ID), additionalExpectedMetadata);
  }

  void testAsynchronousAttempt(final ConfigType configType, final Job job, final Map<String, Object> additionalExpectedMetadata)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final Map<String, Object> metadata = getJobMetadata(configType, LONG_JOB_ID);
    // test when frequency is manual.
    when(connectionService.getStandardSync(CONNECTION_ID))
        .thenReturn(new StandardSync()
            .withConnectionId(CONNECTION_ID)
            .withSourceId(SOURCE_ID)
            .withDestinationId(DESTINATION_ID)
            .withManual(true)
            .withCatalog(CATALOG));
    when(workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(LONG_JOB_ID)).thenReturn(WORKSPACE_ID);
    when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true))
        .thenReturn(new StandardWorkspace().withWorkspaceId(WORKSPACE_ID).withName(WORKSPACE_NAME));
    final Map<String, Object> manualMetadata = MoreMaps.merge(
        ATTEMPT_METADATA,
        metadata,
        Map.of(FREQUENCY_KEY, "manual"),
        additionalExpectedMetadata);

    jobTracker.trackSync(job, JobState.SUCCEEDED);
    assertCorrectMessageForSucceededState(JobTracker.SYNC_EVENT, manualMetadata);

    jobTracker.trackSync(job, JobState.FAILED);
    assertCorrectMessageForFailedState(JobTracker.SYNC_EVENT, manualMetadata);
  }

  private JsonNode configFailureJson() {
    final Map<String, Object> linkedHashMap = new LinkedHashMap<>();
    linkedHashMap.put("failureOrigin", "source");
    linkedHashMap.put("failureType", "config_error");
    linkedHashMap.put("internalMessage", "Internal config error error msg");
    linkedHashMap.put("externalMessage", "Config error related msg");
    linkedHashMap.put(METADATA, ImmutableMap.of(SOME, METADATA));
    linkedHashMap.put("retryable", true);
    linkedHashMap.put("timestamp", 1010);
    return Jsons.jsonNode(linkedHashMap);
  }

  private JsonNode systemFailureJson() {
    final Map<String, Object> linkedHashMap1 = new LinkedHashMap<>();
    linkedHashMap1.put("failureOrigin", "replication");
    linkedHashMap1.put("failureType", "system_error");
    linkedHashMap1.put("internalMessage", "Internal system error error msg");
    linkedHashMap1.put("externalMessage", "System error related msg");
    linkedHashMap1.put(METADATA, ImmutableMap.of(SOME, METADATA));
    linkedHashMap1.put("retryable", true);
    linkedHashMap1.put("timestamp", 1100);
    return Jsons.jsonNode(linkedHashMap1);
  }

  private JsonNode unknownFailureJson() {
    final Map<String, Object> linkedHashMap2 = new LinkedHashMap<>();
    linkedHashMap2.put("failureOrigin", null);
    linkedHashMap2.put("failureType", null);
    linkedHashMap2.put("internalMessage", "Internal unknown error error msg");
    linkedHashMap2.put("externalMessage", "Unknown error related msg");
    linkedHashMap2.put(METADATA, ImmutableMap.of(SOME, METADATA));
    linkedHashMap2.put("retryable", true);
    linkedHashMap2.put("timestamp", 1110);
    return Jsons.jsonNode(linkedHashMap2);
  }

  void testAsynchronousAttemptWithFailures(final ConfigType configType, final Map<String, Object> additionalExpectedMetadata)
      throws IOException, JsonValidationException, ConfigNotFoundException {

    final Map<String, Object> failureMetadata = ImmutableMap.of(
        "failure_reasons", Jsons.arrayNode().addAll(Arrays.asList(configFailureJson(), systemFailureJson(), unknownFailureJson())).toString(),
        "main_failure_reason", configFailureJson().toString());
    testAsynchronousAttempt(configType, getJobWithFailuresMock(configType, LONG_JOB_ID),
        MoreMaps.merge(additionalExpectedMetadata, failureMetadata));
  }

  private Job getJobMock(final ConfigType configType, final long jobId)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID1)
        .withName(SOURCE_DEF_NAME);
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID2)
        .withName(DESTINATION_DEF_NAME);

    when(sourceService.getSourceDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(sourceDefinition);
    when(destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(destinationDefinition);
    when(sourceService.getStandardSourceDefinition(UUID1))
        .thenReturn(sourceDefinition);
    when(destinationService.getStandardDestinationDefinition(UUID2))
        .thenReturn(destinationDefinition);

    when(actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, SOURCE_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withSpec(SOURCE_SPEC)
            .withDockerImageTag(CONNECTOR_VERSION)
            .withDockerRepository(CONNECTOR_REPOSITORY));

    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withSpec(DESTINATION_SPEC)
            .withDockerImageTag(CONNECTOR_VERSION)
            .withDockerRepository(CONNECTOR_REPOSITORY));

    final ConfiguredAirbyteCatalog catalog = new ConfiguredAirbyteCatalog().withStreams(List.of(
        new ConfiguredAirbyteStream(
            new AirbyteStream("stream", Jsons.emptyObject(), List.of(SyncMode.FULL_REFRESH)).withNamespace("namespace"),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND)));

    final AttemptSyncConfig attemptSyncConfig = new AttemptSyncConfig()
        .withSourceConfiguration(Jsons.jsonNode(ImmutableMap.of("key", "some_value")))
        .withDestinationConfiguration(Jsons.jsonNode(ImmutableMap.of("key", false)));

    final JobConfig jobConfig = mock(JobConfig.class);
    when(jobConfig.getConfigType()).thenReturn(configType);

    if (configType == ConfigType.SYNC) {
      final JobSyncConfig jobSyncConfig = new JobSyncConfig()
          .withConfiguredAirbyteCatalog(catalog);
      when(jobConfig.getSync()).thenReturn(jobSyncConfig);
    }
    if (configType == ConfigType.REFRESH) {
      final RefreshConfig refreshConfig = new RefreshConfig()
          .withConfiguredAirbyteCatalog(catalog)
          .withStreamsToRefresh(
              catalog.getStreams()
                  .stream()
                  .map(s -> new RefreshStream().withRefreshType(RefreshStream.RefreshType.TRUNCATE)
                      .withStreamDescriptor(new StreamDescriptor().withName(s.getStream().getName()).withNamespace(s.getStream().getNamespace())))
                  .toList());
      when(jobConfig.getRefresh()).thenReturn(refreshConfig);
    }

    final Attempt attempt = mock(Attempt.class);
    when(attempt.getSyncConfig()).thenReturn(Optional.of(attemptSyncConfig));

    final Job job = mock(Job.class);
    when(job.getId()).thenReturn(jobId);
    when(job.getConfig()).thenReturn(jobConfig);
    when(job.getConfigType()).thenReturn(configType);
    when(job.getScope()).thenReturn(CONNECTION_ID.toString());
    when(job.getLastAttempt()).thenReturn(Optional.of(attempt));
    when(job.getAttemptsCount()).thenReturn(700);
    return job;
  }

  private Attempt getAttemptMock() {
    final Attempt attempt = mock(Attempt.class);
    final JobOutput jobOutput = mock(JobOutput.class);
    final StandardSyncOutput syncOutput = mock(StandardSyncOutput.class);
    final StandardSyncSummary syncSummary = mock(StandardSyncSummary.class);
    final SyncStats syncStats = mock(SyncStats.class);

    when(syncSummary.getStartTime()).thenReturn(SYNC_START_TIME);
    when(syncSummary.getEndTime()).thenReturn(SYNC_END_TIME);
    when(syncSummary.getBytesSynced()).thenReturn(SYNC_BYTES_SYNC);
    when(syncSummary.getRecordsSynced()).thenReturn(SYNC_RECORDS_SYNC);
    when(syncOutput.getStandardSyncSummary()).thenReturn(syncSummary);
    when(syncSummary.getTotalStats()).thenReturn(syncStats);
    when(jobOutput.getSync()).thenReturn(syncOutput);
    when(attempt.getOutput()).thenReturn(java.util.Optional.of(jobOutput));
    when(syncStats.getSourceStateMessagesEmitted()).thenReturn(3L);
    when(syncStats.getDestinationStateMessagesEmitted()).thenReturn(1L);
    when(syncStats.getMaxSecondsBeforeSourceStateMessageEmitted()).thenReturn(5L);
    when(syncStats.getMeanSecondsBeforeSourceStateMessageEmitted()).thenReturn(4L);
    when(syncStats.getMaxSecondsBetweenStateMessageEmittedandCommitted()).thenReturn(7L);
    when(syncStats.getMeanSecondsBetweenStateMessageEmittedandCommitted()).thenReturn(6L);
    when(syncStats.getReplicationStartTime()).thenReturn(7L);
    when(syncStats.getReplicationEndTime()).thenReturn(8L);
    when(syncStats.getSourceReadStartTime()).thenReturn(9L);
    when(syncStats.getSourceReadEndTime()).thenReturn(10L);
    when(syncStats.getDestinationWriteStartTime()).thenReturn(11L);
    when(syncStats.getDestinationWriteEndTime()).thenReturn(12L);

    return attempt;
  }

  private Job getJobWithAttemptsMock(final ConfigType configType, final long jobId)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    return getJobWithAttemptsMock(configType, jobId, List.of(getAttemptMock()));
  }

  private Job getJobWithAttemptsMock(final ConfigType configType, final long jobId, final List<Attempt> attempts)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final Job job = getJobMock(configType, jobId);
    when(job.getAttempts()).thenReturn(attempts);
    when(jobPersistence.getJob(jobId)).thenReturn(job);
    return job;
  }

  private FailureReason getConfigFailureReasonMock() {
    return new FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
        .withRetryable(true)
        .withMetadata(new Metadata().withAdditionalProperty(SOME, METADATA))
        .withExternalMessage("Config error related msg")
        .withInternalMessage("Internal config error error msg")
        .withStacktrace("Don't include stacktrace in call to track")
        .withTimestamp(SYNC_START_TIME + 10);
  }

  private FailureReason getSystemFailureReasonMock() {
    return new FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.REPLICATION)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withRetryable(true)
        .withMetadata(new Metadata().withAdditionalProperty(SOME, METADATA))
        .withExternalMessage("System error related msg")
        .withInternalMessage("Internal system error error msg")
        .withStacktrace("Don't include stacktrace in call to track")
        .withTimestamp(SYNC_START_TIME + 100);
  }

  private FailureReason getUnknownFailureReasonMock() {
    return new FailureReason()
        .withRetryable(true)
        .withMetadata(new Metadata().withAdditionalProperty(SOME, METADATA))
        .withExternalMessage("Unknown error related msg")
        .withInternalMessage("Internal unknown error error msg")
        .withStacktrace("Don't include stacktrace in call to track")
        .withTimestamp(SYNC_START_TIME + 110);
  }

  private List<Attempt> getAttemptsWithFailuresMock() {
    final Attempt attemptWithSingleFailure = getAttemptMock();
    final AttemptFailureSummary singleFailureSummary = mock(AttemptFailureSummary.class);
    when(singleFailureSummary.getFailures()).thenReturn(List.of(getConfigFailureReasonMock()));
    when(attemptWithSingleFailure.getFailureSummary()).thenReturn(Optional.of(singleFailureSummary));

    final Attempt attemptWithMultipleFailures = getAttemptMock();
    final AttemptFailureSummary multipleFailuresSummary = mock(AttemptFailureSummary.class);
    when(multipleFailuresSummary.getFailures()).thenReturn(List.of(getSystemFailureReasonMock(), getUnknownFailureReasonMock()));
    when(attemptWithMultipleFailures.getFailureSummary()).thenReturn(Optional.of(multipleFailuresSummary));

    final Attempt attemptWithNoFailures = getAttemptMock();
    when(attemptWithNoFailures.getFailureSummary()).thenReturn(Optional.empty());

    // in non-test cases we shouldn't actually get failures out of order chronologically
    // this is to verify that we are explicitly sorting the results with tracking failure metadata
    return List.of(attemptWithMultipleFailures, attemptWithSingleFailure, attemptWithNoFailures);
  }

  private Job getJobWithFailuresMock(final ConfigType configType, final long jobId)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    return getJobWithAttemptsMock(configType, jobId, getAttemptsWithFailuresMock());
  }

  private Map<String, Object> getJobMetadata(final ConfigType configType, final long jobId) {
    return ImmutableMap.<String, Object>builder()
        .put(JOB_TYPE, configType != ConfigType.RESET_CONNECTION ? configType : ConfigType.CLEAR)
        .put(JOB_ID_KEY, String.valueOf(jobId))
        .put(ATTEMPT_ID, 700)
        .put("connection_id", CONNECTION_ID)
        .put(CONNECTOR_SOURCE_KEY, SOURCE_DEF_NAME)
        .put(CONNECTOR_SOURCE_DEFINITION_ID_KEY, UUID1)
        .put(CONNECTOR_SOURCE_DOCKER_REPOSITORY_KEY, CONNECTOR_REPOSITORY)
        .put(CONNECTOR_SOURCE_VERSION_KEY, CONNECTOR_VERSION)
        .put("connector_destination", DESTINATION_DEF_NAME)
        .put("connector_destination_definition_id", UUID2)
        .put("connector_destination_docker_repository", CONNECTOR_REPOSITORY)
        .put("connector_destination_version", CONNECTOR_VERSION)
        .put("namespace_definition", NamespaceDefinitionType.SOURCE)
        .put("table_prefix", false)
        .put("operation_count", 0)
        .put("number_of_streams", 1)
        .put("source_id", SOURCE_ID)
        .put("destination_id", DESTINATION_ID)
        .build();
  }

  private void assertCheckConnCorrectMessageForEachState(final BiConsumer<JobState, ConnectorJobOutput> jobStateConsumer,
                                                         final ConfigType configType,
                                                         final Map<String, Object> metadata,
                                                         final boolean workspaceSet) {
    reset(trackingClient);

    // Output does not exist when job has started.
    jobStateConsumer.accept(JobState.STARTED, null);

    final var connectionCheckSuccessOutput = new StandardCheckConnectionOutput();
    connectionCheckSuccessOutput.setStatus(Status.SUCCEEDED);
    final var checkConnectionSuccessJobOutput = new ConnectorJobOutput().withCheckConnection(connectionCheckSuccessOutput);
    jobStateConsumer.accept(JobState.SUCCEEDED, checkConnectionSuccessJobOutput);
    final Map<String, Object> checkConnSuccessMetadata = ImmutableMap.of("check_connection_outcome", "succeeded");

    final var connectionCheckFailureOutput = new StandardCheckConnectionOutput();
    connectionCheckFailureOutput.setStatus(Status.FAILED);
    connectionCheckFailureOutput.setMessage("Please check your Personal Access Token.");
    final var checkConnectionFailureJobOutput = new ConnectorJobOutput().withCheckConnection(connectionCheckFailureOutput);
    jobStateConsumer.accept(JobState.SUCCEEDED, checkConnectionFailureJobOutput); // The job still succeeded, only the connection check failed
    final Map<String, Object> checkConnFailureMetadata = ImmutableMap.of(
        "check_connection_outcome", "failed",
        "check_connection_message", "Please check your Personal Access Token.");

    // Failure implies the job threw an exception which almost always meant no output.
    final var failedCheckJobOutput = new ConnectorJobOutput();
    failedCheckJobOutput.setFailureReason(getConfigFailureReasonMock());
    jobStateConsumer.accept(JobState.FAILED, failedCheckJobOutput);
    final Map<String, Object> failedCheckJobMetadata = ImmutableMap.of("failure_reason", configFailureJson().toString());

    if (workspaceSet) {
      assertCorrectMessageForStartedState(
          configType == ConfigType.CHECK_CONNECTION_SOURCE ? JobTracker.CHECK_CONNECTION_SOURCE_EVENT : JobTracker.CHECK_CONNECTION_DESTINATION_EVENT,
          metadata);
      assertCorrectMessageForSucceededState(
          configType == ConfigType.CHECK_CONNECTION_SOURCE ? JobTracker.CHECK_CONNECTION_SOURCE_EVENT : JobTracker.CHECK_CONNECTION_DESTINATION_EVENT,
          MoreMaps.merge(metadata, checkConnSuccessMetadata));
      assertCorrectMessageForSucceededState(
          configType == ConfigType.CHECK_CONNECTION_SOURCE ? JobTracker.CHECK_CONNECTION_SOURCE_EVENT : JobTracker.CHECK_CONNECTION_DESTINATION_EVENT,
          MoreMaps.merge(metadata, checkConnFailureMetadata));
      assertCorrectMessageForFailedState(
          configType == ConfigType.CHECK_CONNECTION_SOURCE ? JobTracker.CHECK_CONNECTION_SOURCE_EVENT : JobTracker.CHECK_CONNECTION_DESTINATION_EVENT,
          MoreMaps.merge(metadata, failedCheckJobMetadata));
    } else {
      verifyNoInteractions(trackingClient);
    }
  }

  private void assertDiscoverCorrectMessageForEachState(final BiConsumer<JobState, ConnectorJobOutput> jobStateConsumer,
                                                        final Map<String, Object> metadata,
                                                        final boolean workspaceSet) {
    reset(trackingClient);

    // Output does not exist when job has started.
    jobStateConsumer.accept(JobState.STARTED, null);

    final UUID discoverCatalogID = UUID.randomUUID();
    final var discoverSuccessJobOutput = new ConnectorJobOutput().withDiscoverCatalogId(discoverCatalogID);
    jobStateConsumer.accept(JobState.SUCCEEDED, discoverSuccessJobOutput);

    // Failure implies the job threw an exception which almost always meant no output.
    final var failedDiscoverOutput = new ConnectorJobOutput();
    failedDiscoverOutput.setFailureReason(getSystemFailureReasonMock());
    jobStateConsumer.accept(JobState.FAILED, failedDiscoverOutput);

    final Map<String, Object> failedDiscoverMetadata = ImmutableMap.of("failure_reason", systemFailureJson().toString());

    if (workspaceSet) {
      assertCorrectMessageForStartedState(JobTracker.DISCOVER_EVENT, metadata);
      assertCorrectMessageForSucceededState(JobTracker.DISCOVER_EVENT, metadata);
      assertCorrectMessageForFailedState(JobTracker.DISCOVER_EVENT, MoreMaps.merge(metadata, failedDiscoverMetadata));
    } else {
      verifyNoInteractions(trackingClient);
    }
  }

  /**
   * Tests that the tracker emits the correct message for when the job starts, succeeds, and fails.
   *
   * @param jobStateConsumer - consumer that takes in a job state and then calls the relevant method
   *        on the job tracker with it. if testing discover, it calls trackDiscover, etc.
   * @param expectedMetadata - expected metadata (except job state).
   */
  private void assertCorrectMessageForEachState(final Consumer<JobState> jobStateConsumer,
                                                final Map<String, Object> expectedMetadata) {
    jobStateConsumer.accept(JobState.STARTED);
    assertCorrectMessageForStartedState(JobTracker.SYNC_EVENT, expectedMetadata);
    jobStateConsumer.accept(JobState.SUCCEEDED);
    assertCorrectMessageForSucceededState(JobTracker.SYNC_EVENT, expectedMetadata);
    jobStateConsumer.accept(JobState.FAILED);
    assertCorrectMessageForFailedState(JobTracker.SYNC_EVENT, expectedMetadata);
  }

  private void assertCorrectMessageForStartedState(final String action, final Map<String, Object> metadata) {
    verify(trackingClient).track(WORKSPACE_ID, ScopeType.WORKSPACE, action, MoreMaps.merge(metadata, STARTED_STATE_METADATA, mockWorkspaceInfo()));
  }

  private void assertCorrectMessageForSucceededState(final String action, final Map<String, Object> metadata) {
    verify(trackingClient).track(WORKSPACE_ID, ScopeType.WORKSPACE, action, MoreMaps.merge(metadata, SUCCEEDED_STATE_METADATA, mockWorkspaceInfo()));
  }

  private void assertCorrectMessageForFailedState(final String action, final Map<String, Object> metadata) {
    verify(trackingClient).track(WORKSPACE_ID, ScopeType.WORKSPACE, action, MoreMaps.merge(metadata, FAILED_STATE_METADATA, mockWorkspaceInfo()));
  }

  private Map<String, Object> mockWorkspaceInfo() {
    final Map<String, Object> map = new HashMap<>();
    map.put("workspace_id", WORKSPACE_ID);
    map.put("workspace_name", WORKSPACE_NAME);
    return map;
  }

}
