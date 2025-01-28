/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter;

import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.Metadata;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.State;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JobErrorReporterTest {

  private static final UUID JOB_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final String CONNECTION_URL = "http://localhost:8000/connection/my_connection";
  private static final String WORKSPACE_URL = "http://localhost:8000/workspace/my_workspace";
  private static final DeploymentMode DEPLOYMENT_MODE = DeploymentMode.OSS;
  private static final String AIRBYTE_VERSION = "0.1.40";
  private static final String DOCKER_IMAGE_TAG = "1.2.3";

  private static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();
  private static final UUID SOURCE_DEFINITION_VERSION_ID = UUID.randomUUID();
  private static final String SOURCE_DEFINITION_NAME = "stripe";
  private static final String SOURCE_DOCKER_REPOSITORY = "airbyte/source-stripe";
  private static final String SOURCE_DOCKER_IMAGE = SOURCE_DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG;
  private static final ReleaseStage SOURCE_RELEASE_STAGE = ReleaseStage.BETA;
  private static final Long SOURCE_INTERNAL_SUPPORT_LEVEL = 200L;
  private static final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
  private static final UUID DESTINATION_DEFINITION_VERSION_ID = UUID.randomUUID();
  private static final String DESTINATION_DEFINITION_NAME = "snowflake";
  private static final String DESTINATION_DOCKER_REPOSITORY = "airbyte/destination-snowflake";
  private static final String DESTINATION_DOCKER_IMAGE = DESTINATION_DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG;
  private static final ReleaseStage DESTINATION_RELEASE_STAGE = ReleaseStage.BETA;
  private static final Long DESTINATION_INTERNAL_SUPPORT_LEVEL = 100L;
  private static final String FROM_TRACE_MESSAGE = "from_trace_message";
  private static final String JOB_ID_KEY = "job_id";
  private static final String WORKSPACE_ID_KEY = "workspace_id";
  private static final String WORKSPACE_URL_KEY = "workspace_url";
  private static final String CONNECTION_ID_KEY = "connection_id";
  private static final String CONNECTION_URL_KEY = "connection_url";
  private static final String DEPLOYMENT_MODE_KEY = "deployment_mode";
  private static final String AIRBYTE_VERSION_KEY = "airbyte_version";
  private static final String FAILURE_ORIGIN_KEY = "failure_origin";
  private static final String SOURCE = "source";
  private static final String FAILURE_TYPE_KEY = "failure_type";
  private static final String SYSTEM_ERROR = "system_error";
  private static final String CONNECTOR_DEFINITION_ID_KEY = "connector_definition_id";
  private static final String CONNECTOR_REPOSITORY_KEY = "connector_repository";
  private static final String CONNECTOR_NAME_KEY = "connector_name";
  private static final String CONNECTOR_RELEASE_STAGE_KEY = "connector_release_stage";
  private static final String CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY = "connector_internal_support_level";
  private static final String CONNECTOR_COMMAND_KEY = "connector_command";
  private static final String CHECK_COMMAND = "check";
  private static final String DISCOVER_COMMAND = "discover";
  private static final String SPEC_COMMAND = "spec";
  private static final String READ_COMMAND = "read";
  private static final String WRITE_COMMAND = "write";

  private ActorDefinitionService actorDefinitionService;
  private SourceService sourceService;
  private DestinationService destinationService;
  private WorkspaceService workspaceService;
  private JobErrorReportingClient jobErrorReportingClient;
  private WebUrlHelper webUrlHelper;
  private JobErrorReporter jobErrorReporter;

  @BeforeEach
  void setup() {
    actorDefinitionService = mock(ActorDefinitionService.class);
    sourceService = mock(SourceService.class);
    destinationService = mock(DestinationService.class);
    workspaceService = mock(WorkspaceService.class);
    jobErrorReportingClient = mock(JobErrorReportingClient.class);
    webUrlHelper = mock(WebUrlHelper.class);
    jobErrorReporter = new JobErrorReporter(
        actorDefinitionService,
        sourceService,
        destinationService,
        workspaceService,
        DEPLOYMENT_MODE,
        AIRBYTE_VERSION,
        webUrlHelper,
        jobErrorReportingClient);

    Mockito.when(webUrlHelper.getConnectionUrl(WORKSPACE_ID, CONNECTION_ID)).thenReturn(CONNECTION_URL);
    Mockito.when(webUrlHelper.getWorkspaceUrl(WORKSPACE_ID)).thenReturn(WORKSPACE_URL);
  }

  @Test
  void testReportSyncJobFailure() throws IOException, ConfigNotFoundException {
    final AttemptFailureSummary mFailureSummary = Mockito.mock(AttemptFailureSummary.class);

    final FailureReason sourceFailureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, READ_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final FailureReason destinationFailureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, WRITE_COMMAND))
        .withFailureOrigin(FailureOrigin.DESTINATION)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final FailureReason nonTraceMessageFailureReason = new FailureReason().withFailureOrigin(FailureOrigin.SOURCE);
    final FailureReason replicationFailureReason = new FailureReason().withFailureOrigin(FailureOrigin.REPLICATION);

    Mockito.when(mFailureSummary.getFailures()).thenReturn(List.of(
        sourceFailureReason, destinationFailureReason, nonTraceMessageFailureReason, replicationFailureReason));

    final long syncJobId = 1L;
    final SyncJobReportingContext jobReportingContext = new SyncJobReportingContext(
        syncJobId,
        SOURCE_DEFINITION_VERSION_ID,
        DESTINATION_DEFINITION_VERSION_ID);

    final ObjectMapper objectMapper = new ObjectMapper();
    final AttemptConfigReportingContext attemptConfig =
        new AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), new State());

    Mockito.when(sourceService.getSourceDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(new StandardSourceDefinition()
            .withSourceDefinitionId(SOURCE_DEFINITION_ID)
            .withName(SOURCE_DEFINITION_NAME));

    Mockito.when(destinationService.getDestinationDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(new StandardDestinationDefinition()
            .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
            .withName(DESTINATION_DEFINITION_NAME));

    Mockito.when(actorDefinitionService.getActorDefinitionVersion(SOURCE_DEFINITION_VERSION_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPOSITORY)
            .withDockerImageTag(DOCKER_IMAGE_TAG)
            .withReleaseStage(SOURCE_RELEASE_STAGE)
            .withInternalSupportLevel(SOURCE_INTERNAL_SUPPORT_LEVEL));

    Mockito.when(actorDefinitionService.getActorDefinitionVersion(DESTINATION_DEFINITION_VERSION_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(DESTINATION_DOCKER_REPOSITORY)
            .withDockerImageTag(DOCKER_IMAGE_TAG)
            .withReleaseStage(DESTINATION_RELEASE_STAGE)
            .withInternalSupportLevel(DESTINATION_INTERNAL_SUPPORT_LEVEL));

    final StandardWorkspace mWorkspace = Mockito.mock(StandardWorkspace.class);
    Mockito.when(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID);
    Mockito.when(workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, true)).thenReturn(mWorkspace);

    jobErrorReporter.reportSyncJobFailure(CONNECTION_ID, mFailureSummary, jobReportingContext, attemptConfig);

    final Map<String, String> expectedSourceMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, String.valueOf(syncJobId)),
        Map.entry(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry(CONNECTION_ID_KEY, CONNECTION_ID.toString()),
        Map.entry(CONNECTION_URL_KEY, CONNECTION_URL),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_COMMAND_KEY, READ_COMMAND),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(SOURCE_INTERNAL_SUPPORT_LEVEL)));

    final Map<String, String> expectedDestinationMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, String.valueOf(syncJobId)),
        Map.entry(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry(CONNECTION_ID_KEY, CONNECTION_ID.toString()),
        Map.entry(CONNECTION_URL_KEY, CONNECTION_URL),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, "destination"),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_COMMAND_KEY, WRITE_COMMAND),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, DESTINATION_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, DESTINATION_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, DESTINATION_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, DESTINATION_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(DESTINATION_INTERNAL_SUPPORT_LEVEL)));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(mWorkspace, sourceFailureReason, SOURCE_DOCKER_IMAGE, expectedSourceMetadata,
        attemptConfig);
    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(mWorkspace, destinationFailureReason, DESTINATION_DOCKER_IMAGE,
        expectedDestinationMetadata, attemptConfig);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportSyncJobFailureDoesNotThrow() throws ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final AttemptFailureSummary mFailureSummary = Mockito.mock(AttemptFailureSummary.class);
    final SyncJobReportingContext jobContext = new SyncJobReportingContext(1L, SOURCE_DEFINITION_VERSION_ID, DESTINATION_DEFINITION_VERSION_ID);

    final FailureReason sourceFailureReason = new FailureReason()
        .withMetadata(new Metadata().withAdditionalProperty(FROM_TRACE_MESSAGE, true))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ObjectMapper objectMapper = new ObjectMapper();
    final AttemptConfigReportingContext attemptConfig =
        new AttemptConfigReportingContext(objectMapper.createObjectNode(), objectMapper.createObjectNode(), new State());

    Mockito.when(mFailureSummary.getFailures()).thenReturn(List.of(sourceFailureReason));

    Mockito.when(sourceService.getSourceDefinitionFromConnection(CONNECTION_ID))
        .thenReturn(new StandardSourceDefinition()
            .withSourceDefinitionId(SOURCE_DEFINITION_ID)
            .withName(SOURCE_DEFINITION_NAME));

    Mockito.when(actorDefinitionService.getActorDefinitionVersion(SOURCE_DEFINITION_VERSION_ID))
        .thenReturn(new ActorDefinitionVersion()
            .withDockerRepository(SOURCE_DOCKER_REPOSITORY)
            .withDockerImageTag(DOCKER_IMAGE_TAG)
            .withReleaseStage(SOURCE_RELEASE_STAGE));

    final StandardWorkspace mWorkspace = Mockito.mock(StandardWorkspace.class);
    Mockito.when(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID);
    Mockito.when(workspaceService.getStandardWorkspaceFromConnection(CONNECTION_ID, true)).thenReturn(mWorkspace);
    Mockito.when(webUrlHelper.getConnectionUrl(WORKSPACE_ID, CONNECTION_ID)).thenReturn(CONNECTION_URL);

    Mockito.doThrow(new RuntimeException("some exception"))
        .when(jobErrorReportingClient)
        .reportJobFailureReason(Mockito.any(), Mockito.eq(sourceFailureReason), Mockito.any(), Mockito.any(), Mockito.any());

    Assertions.assertDoesNotThrow(() -> jobErrorReporter.reportSyncJobFailure(CONNECTION_ID, mFailureSummary, jobContext, attemptConfig));
    Mockito.verify(jobErrorReportingClient, Mockito.times(1))
        .reportJobFailureReason(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  void testReportSourceCheckJobFailureNullWorkspaceId() throws JsonValidationException, ConfigNotFoundException, IOException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    Mockito.when(sourceService.getStandardSourceDefinition(SOURCE_DEFINITION_ID))
        .thenReturn(new StandardSourceDefinition()
            .withSourceDefinitionId(SOURCE_DEFINITION_ID)
            .withName(SOURCE_DEFINITION_NAME));

    jobErrorReporter.reportSourceCheckJobFailure(SOURCE_DEFINITION_ID, null, failureReason, jobContext);

    final Map<String, String> expectedMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(SOURCE_INTERNAL_SUPPORT_LEVEL)),
        Map.entry(CONNECTOR_COMMAND_KEY, CHECK_COMMAND));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testDoNotReportSourceCheckJobFailureFromOtherOrigins() throws JsonValidationException, IOException, ConfigNotFoundException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    jobErrorReporter.reportSourceCheckJobFailure(SOURCE_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext);

    Mockito.verifyNoInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportDestinationCheckJobFailure() throws JsonValidationException, IOException, ConfigNotFoundException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.DESTINATION)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, DESTINATION_DOCKER_IMAGE, DESTINATION_RELEASE_STAGE, DESTINATION_INTERNAL_SUPPORT_LEVEL);

    Mockito.when(destinationService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID))
        .thenReturn(new StandardDestinationDefinition()
            .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
            .withName(DESTINATION_DEFINITION_NAME));

    final StandardWorkspace mWorkspace = Mockito.mock(StandardWorkspace.class);
    Mockito.when(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID);
    Mockito.when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(mWorkspace);

    jobErrorReporter.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext);

    final Map<String, String> expectedMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, "destination"),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, DESTINATION_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, DESTINATION_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, DESTINATION_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, DESTINATION_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(DESTINATION_INTERNAL_SUPPORT_LEVEL)),
        Map.entry(CONNECTOR_COMMAND_KEY, CHECK_COMMAND));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(mWorkspace, failureReason, DESTINATION_DOCKER_IMAGE, expectedMetadata, null);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testDoNotReportDestinationCheckJobFailureFromOtherOrigins() throws JsonValidationException, ConfigNotFoundException, IOException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, DESTINATION_DOCKER_IMAGE, DESTINATION_RELEASE_STAGE, DESTINATION_INTERNAL_SUPPORT_LEVEL);

    jobErrorReporter.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext);

    Mockito.verifyNoInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportDestinationCheckJobFailureNullWorkspaceId() throws JsonValidationException, ConfigNotFoundException, IOException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.DESTINATION)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, DESTINATION_DOCKER_IMAGE, DESTINATION_RELEASE_STAGE, DESTINATION_INTERNAL_SUPPORT_LEVEL);

    Mockito.when(destinationService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID))
        .thenReturn(new StandardDestinationDefinition()
            .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
            .withName(DESTINATION_DEFINITION_NAME));

    jobErrorReporter.reportDestinationCheckJobFailure(DESTINATION_DEFINITION_ID, null, failureReason, jobContext);

    final Map<String, String> expectedMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, "destination"),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, DESTINATION_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, DESTINATION_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, DESTINATION_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, DESTINATION_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(DESTINATION_INTERNAL_SUPPORT_LEVEL)),
        Map.entry(CONNECTOR_COMMAND_KEY, CHECK_COMMAND));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(null, failureReason, DESTINATION_DOCKER_IMAGE, expectedMetadata, null);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportDiscoverJobFailure() throws JsonValidationException, ConfigNotFoundException, IOException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    Mockito.when(sourceService.getStandardSourceDefinition(SOURCE_DEFINITION_ID))
        .thenReturn(new StandardSourceDefinition()
            .withSourceDefinitionId(SOURCE_DEFINITION_ID)
            .withName(SOURCE_DEFINITION_NAME));

    final StandardWorkspace mWorkspace = Mockito.mock(StandardWorkspace.class);
    Mockito.when(mWorkspace.getWorkspaceId()).thenReturn(WORKSPACE_ID);
    Mockito.when(workspaceService.getStandardWorkspaceNoSecrets(WORKSPACE_ID, true)).thenReturn(mWorkspace);

    jobErrorReporter.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext);

    final Map<String, String> expectedMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry(WORKSPACE_ID_KEY, WORKSPACE_ID.toString()),
        Map.entry(WORKSPACE_URL_KEY, WORKSPACE_URL),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(SOURCE_INTERNAL_SUPPORT_LEVEL)),
        Map.entry(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(mWorkspace, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportDiscoverJobFailureNullWorkspaceId() throws JsonValidationException, ConfigNotFoundException, IOException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    Mockito.when(sourceService.getStandardSourceDefinition(SOURCE_DEFINITION_ID))
        .thenReturn(new StandardSourceDefinition()
            .withSourceDefinitionId(SOURCE_DEFINITION_ID)
            .withName(SOURCE_DEFINITION_NAME));

    jobErrorReporter.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, null, failureReason, jobContext);

    final Map<String, String> expectedMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_DEFINITION_ID_KEY, SOURCE_DEFINITION_ID.toString()),
        Map.entry(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_NAME_KEY, SOURCE_DEFINITION_NAME),
        Map.entry(CONNECTOR_RELEASE_STAGE_KEY, SOURCE_RELEASE_STAGE.toString()),
        Map.entry(CONNECTOR_INTERNAL_SUPPORT_LEVEL_KEY, Long.toString(SOURCE_INTERNAL_SUPPORT_LEVEL)),
        Map.entry(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testDoNotReportDiscoverJobFailureFromOtherOrigins() throws JsonValidationException, IOException, ConfigNotFoundException {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    jobErrorReporter.reportDiscoverJobFailure(SOURCE_DEFINITION_ID, WORKSPACE_ID, failureReason, jobContext);

    Mockito.verifyNoInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportSpecJobFailure() {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, SPEC_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext = new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, null, null);

    jobErrorReporter.reportSpecJobFailure(failureReason, jobContext);

    final Map<String, String> expectedMetadata = Map.ofEntries(
        Map.entry(JOB_ID_KEY, JOB_ID.toString()),
        Map.entry(DEPLOYMENT_MODE_KEY, DEPLOYMENT_MODE.name()),
        Map.entry(AIRBYTE_VERSION_KEY, AIRBYTE_VERSION),
        Map.entry(FAILURE_ORIGIN_KEY, SOURCE),
        Map.entry(FAILURE_TYPE_KEY, SYSTEM_ERROR),
        Map.entry(CONNECTOR_REPOSITORY_KEY, SOURCE_DOCKER_REPOSITORY),
        Map.entry(CONNECTOR_COMMAND_KEY, SPEC_COMMAND));

    Mockito.verify(jobErrorReportingClient).reportJobFailureReason(null, failureReason, SOURCE_DOCKER_IMAGE, expectedMetadata, null);
    Mockito.verifyNoMoreInteractions(jobErrorReportingClient);
  }

  @Test
  void testDoNotReportSpecJobFailureFromOtherOrigins() {
    final FailureReason failureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureType.SYSTEM_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    jobErrorReporter.reportSpecJobFailure(failureReason, jobContext);

    Mockito.verifyNoInteractions(jobErrorReportingClient);
  }

  @Test
  void testReportUnsupportedFailureType() {
    final FailureReason readFailureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, READ_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.CONFIG_ERROR);

    final FailureReason discoverFailureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, DISCOVER_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.MANUAL_CANCELLATION);

    final FailureReason checkFailureReason = new FailureReason()
        .withMetadata(new Metadata()
            .withAdditionalProperty(FROM_TRACE_MESSAGE, true)
            .withAdditionalProperty(CONNECTOR_COMMAND_KEY, CHECK_COMMAND))
        .withFailureOrigin(FailureOrigin.SOURCE)
        .withFailureType(FailureType.CONFIG_ERROR);

    final ConnectorJobReportingContext jobContext =
        new ConnectorJobReportingContext(JOB_ID, SOURCE_DOCKER_IMAGE, SOURCE_RELEASE_STAGE, SOURCE_INTERNAL_SUPPORT_LEVEL);

    jobErrorReporter.reportSpecJobFailure(readFailureReason, jobContext);
    jobErrorReporter.reportSpecJobFailure(discoverFailureReason, jobContext);
    jobErrorReporter.reportSpecJobFailure(checkFailureReason, jobContext);

    Mockito.verifyNoInteractions(jobErrorReportingClient);
  }

}
