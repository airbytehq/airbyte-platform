/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.model.generated.CheckInput;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SyncInput;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.ContextBuilder;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.State;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.secrets.ConfigWithSecretReferences;
import io.airbyte.config.secrets.InlinedConfigWithSecretRefsKt;
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate;
import io.airbyte.config.secrets.SecretReferenceConfig;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.domain.services.secrets.SecretReferenceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the JobInputHandler.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class JobInputHandlerTest {

  private static JobPersistence jobPersistence;
  private static ConfigInjector configInjector;
  private static OAuthConfigSupplier oAuthConfigSupplier;
  private static Job job;
  private static FeatureFlagClient featureFlagClient;
  private static ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private static final UUID SECRET_REF_ID = UUID.randomUUID();
  private static final JsonNode SOURCE_CONFIGURATION =
      Jsons.jsonNode(Map.of("source_key", "source_value", "source_secret", Map.of("_secret_reference_id", SECRET_REF_ID)));
  private static final JsonNode SOURCE_CONFIG_WITH_OAUTH =
      Jsons.jsonNode(Map.of("source_key", "source_value", "source_secret", Map.of("_secret_reference_id", SECRET_REF_ID), "oauth", "oauth_value"));
  private static final JsonNode SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG =
      Jsons.jsonNode(Map.of("source_key", "source_value", "source_secret", Map.of("_secret_reference_id", SECRET_REF_ID), "oauth", "oauth_value",
          "injected", "value"));
  private static final ConfigWithSecretReferences SOURCE_CONFIG_WITH_REFS =
      new ConfigWithSecretReferences(SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG,
          Map.of("$.source_secret", new SecretReferenceConfig(new AirbyteManagedSecretCoordinate(), null, null)));
  private static final JsonNode INLINED_SOURCE_CONFIG_WITH_REFS = InlinedConfigWithSecretRefsKt.toInlined(SOURCE_CONFIG_WITH_REFS);
  private static final JsonNode DESTINATION_CONFIGURATION =
      Jsons.jsonNode(Map.of("destination_key", "destination_value", "destination_secret", Map.of("_secret_reference_id", SECRET_REF_ID)));
  private static final JsonNode DESTINATION_CONFIG_WITH_OAUTH =
      Jsons.jsonNode(Map.of("destination_key", "destination_value", "destination_secret", Map.of("_secret_reference_id", SECRET_REF_ID), "oauth",
          "oauth_value"));
  private static final ConfigWithSecretReferences DESTINATION_CONFIG_WITH_REFS = new ConfigWithSecretReferences(DESTINATION_CONFIG_WITH_OAUTH,
      Map.of("$.destination_secret", new SecretReferenceConfig(new AirbyteManagedSecretCoordinate(), null, null)));
  private static final JsonNode INLINED_DESTINATION_CONFIG_WITH_REFS = InlinedConfigWithSecretRefsKt.toInlined(DESTINATION_CONFIG_WITH_REFS);
  private static final State STATE = new State().withState(Jsons.jsonNode(Map.of("state_key", "state_value")));

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final long JOB_ID = 1;
  private static final int ATTEMPT_NUMBER = 1;
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();

  private AttemptHandler attemptHandler;
  private StateHandler stateHandler;
  private JobInputHandler jobInputHandler;
  private ContextBuilder contextBuilder;
  private SourceService sourceService;
  private DestinationService destinatinonService;
  private ConnectionService connectionService;
  private ScopedConfigurationService scopedConfigurationService;
  private SecretReferenceService secretReferenceService;

  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  @BeforeEach
  void init() throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {

    jobPersistence = mock(JobPersistence.class);
    configInjector = mock(ConfigInjector.class);

    oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    job = mock(Job.class);
    featureFlagClient = new TestClient(new HashMap<>());
    attemptHandler = mock(AttemptHandler.class);
    stateHandler = mock(StateHandler.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    contextBuilder = mock(ContextBuilder.class);

    sourceService = mock(SourceService.class);
    destinatinonService = mock(DestinationService.class);
    connectionService = mock(ConnectionService.class);
    scopedConfigurationService = mock(ScopedConfigurationService.class);
    secretReferenceService = mock(SecretReferenceService.class);

    jobInputHandler = new JobInputHandler(jobPersistence,
        featureFlagClient,
        oAuthConfigSupplier,
        configInjector,
        attemptHandler,
        stateHandler,
        actorDefinitionVersionHelper,
        contextBuilder,
        connectionService,
        sourceService,
        destinatinonService,
        apiPojoConverters,
        scopedConfigurationService,
        secretReferenceService);

    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);
    when(configInjector.injectConfig(any(), any())).thenAnswer(i -> i.getArguments()[0]);
    when(secretReferenceService.getConfigWithSecretReferences(any(), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(1), Map.of()));

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withConfiguration(DESTINATION_CONFIGURATION);
    when(destinatinonService.getDestinationConnection(DESTINATION_ID)).thenReturn(destinationConnection);
    when(destinatinonService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID)).thenReturn(mock(StandardDestinationDefinition.class));
    final StandardDestinationDefinition destinationDefinition = mock(StandardDestinationDefinition.class);
    when(destinatinonService.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID)).thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID))
        .thenReturn(mock(ActorDefinitionVersion.class));
    when(oAuthConfigSupplier.injectDestinationOAuthParameters(DESTINATION_DEFINITION_ID, DESTINATION_ID, WORKSPACE_ID, DESTINATION_CONFIGURATION))
        .thenReturn(DESTINATION_CONFIG_WITH_OAUTH);

    final StandardSync standardSync = new StandardSync()
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID);
    when(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(standardSync);
  }

  @Test
  void testGetSyncWorkflowInput() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SyncInput syncInput = new SyncInput().jobId(JOB_ID).attemptNumber(ATTEMPT_NUMBER);

    final UUID sourceDefinitionId = UUID.randomUUID();
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(SOURCE_ID)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(SOURCE_CONFIGURATION);
    when(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(sourceConnection);
    when(oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefinitionId, SOURCE_ID, WORKSPACE_ID, SOURCE_CONFIGURATION))
        .thenReturn(SOURCE_CONFIG_WITH_OAUTH);
    when(configInjector.injectConfig(SOURCE_CONFIG_WITH_OAUTH, sourceDefinitionId))
        .thenReturn(SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG);
    when(
        secretReferenceService.getConfigWithSecretReferences(SOURCE_ID, SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG, WORKSPACE_ID))
            .thenReturn(SOURCE_CONFIG_WITH_REFS);
    when(secretReferenceService.getConfigWithSecretReferences(DESTINATION_ID, DESTINATION_CONFIG_WITH_OAUTH, WORKSPACE_ID))
        .thenReturn(DESTINATION_CONFIG_WITH_REFS);

    when(sourceService.getStandardSourceDefinition(sourceDefinitionId)).thenReturn(mock(StandardSourceDefinition.class));
    when(actorDefinitionVersionHelper.getSourceVersion(any(), any(), any())).thenReturn(mock(ActorDefinitionVersion.class));

    when(stateHandler.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID)))
        .thenReturn(new ConnectionState()
            .stateType(ConnectionStateType.LEGACY)
            .state(STATE.getState())
            .connectionId(CONNECTION_ID));

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDockerImage("destinationDockerImage")
        .withSourceDockerImage("sourceDockerImage")
        .withConfiguredAirbyteCatalog(mock(ConfiguredAirbyteCatalog.class));

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig);

    when(job.getConfig()).thenReturn(jobConfig);
    when(job.getScope()).thenReturn(CONNECTION_ID.toString());

    final StandardSyncInput expectedStandardSyncInput = new StandardSyncInput()
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobSyncConfig.getWorkspaceId())
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withSourceConfiguration(INLINED_SOURCE_CONFIG_WITH_REFS)
        .withDestinationConfiguration(INLINED_DESTINATION_CONFIG_WITH_REFS)
        .withIsReset(false)
        .withUseAsyncReplicate(true)
        .withUseAsyncActivities(true)
        .withIncludesFiles(false);

    final JobRunConfig expectedJobRunConfig = new JobRunConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER);

    final IntegrationLauncherConfig expectedSourceLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER)
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDockerImage(jobSyncConfig.getSourceDockerImage());

    final IntegrationLauncherConfig expectedDestinationLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER)
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobSyncConfig.getWorkspaceId())
        .withDockerImage(jobSyncConfig.getDestinationDockerImage())
        .withAdditionalEnvironmentVariables(Collections.emptyMap());

    final JobInput expectedJobInput = new JobInput(
        expectedJobRunConfig,
        expectedSourceLauncherConfig,
        expectedDestinationLauncherConfig,
        expectedStandardSyncInput);

    final Object generatedJobInput = jobInputHandler.getJobInput(syncInput);
    assertEquals(expectedJobInput, generatedJobInput);

    final AttemptSyncConfig expectedAttemptSyncConfig = new AttemptSyncConfig()
        .withSourceConfiguration(INLINED_SOURCE_CONFIG_WITH_REFS)
        .withDestinationConfiguration(INLINED_DESTINATION_CONFIG_WITH_REFS)
        .withState(STATE);

    verify(oAuthConfigSupplier).injectSourceOAuthParameters(sourceDefinitionId, SOURCE_ID, WORKSPACE_ID, SOURCE_CONFIGURATION);
    verify(oAuthConfigSupplier).injectDestinationOAuthParameters(DESTINATION_DEFINITION_ID, DESTINATION_ID, WORKSPACE_ID, DESTINATION_CONFIGURATION);
    verify(secretReferenceService).getConfigWithSecretReferences(DESTINATION_ID, DESTINATION_CONFIG_WITH_OAUTH, WORKSPACE_ID);
    verify(secretReferenceService).getConfigWithSecretReferences(SOURCE_ID, SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG, WORKSPACE_ID);

    verify(attemptHandler).saveSyncConfig(new SaveAttemptSyncConfigRequestBody()
        .jobId(JOB_ID)
        .attemptNumber(ATTEMPT_NUMBER)
        .syncConfig(apiPojoConverters.attemptSyncConfigToApi(expectedAttemptSyncConfig, CONNECTION_ID)));
  }

  @Test
  void testGetResetSyncWorkflowInput() throws IOException {
    final SyncInput syncInput = new SyncInput().jobId(JOB_ID).attemptNumber(ATTEMPT_NUMBER);

    when(stateHandler.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID)))
        .thenReturn(new ConnectionState()
            .stateType(ConnectionStateType.LEGACY)
            .state(STATE.getState())
            .connectionId(CONNECTION_ID));

    final JobResetConnectionConfig jobResetConfig = new JobResetConnectionConfig()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDockerImage("destinationDockerImage")
        .withConfiguredAirbyteCatalog(mock(ConfiguredAirbyteCatalog.class));

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.RESET_CONNECTION)
        .withResetConnection(jobResetConfig);

    when(job.getConfig()).thenReturn(jobConfig);
    when(job.getScope()).thenReturn(CONNECTION_ID.toString());

    final StandardSyncInput expectedStandardSyncInput = new StandardSyncInput()
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobResetConfig.getWorkspaceId())
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID)
        .withSourceConfiguration(Jsons.emptyObject())
        .withDestinationConfiguration(DESTINATION_CONFIG_WITH_OAUTH)
        .withWebhookOperationConfigs(jobResetConfig.getWebhookOperationConfigs())
        .withIsReset(true)
        .withUseAsyncReplicate(true)
        .withUseAsyncActivities(true)
        .withIncludesFiles(false);

    final JobRunConfig expectedJobRunConfig = new JobRunConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER);

    final IntegrationLauncherConfig expectedSourceLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER)
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobResetConfig.getWorkspaceId())
        .withDockerImage(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB);

    final IntegrationLauncherConfig expectedDestinationLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER)
        .withConnectionId(CONNECTION_ID)
        .withWorkspaceId(jobResetConfig.getWorkspaceId())
        .withDockerImage(jobResetConfig.getDestinationDockerImage())
        .withAdditionalEnvironmentVariables(Collections.emptyMap());

    final JobInput expectedJobInput = new JobInput(
        expectedJobRunConfig,
        expectedSourceLauncherConfig,
        expectedDestinationLauncherConfig,
        expectedStandardSyncInput);

    final Object generatedJobInput = jobInputHandler.getJobInput(syncInput);
    assertEquals(expectedJobInput, generatedJobInput);

    final AttemptSyncConfig expectedAttemptSyncConfig = new AttemptSyncConfig()
        .withSourceConfiguration(Jsons.emptyObject())
        .withDestinationConfiguration(DESTINATION_CONFIG_WITH_OAUTH)
        .withState(STATE);

    verify(oAuthConfigSupplier).injectDestinationOAuthParameters(DESTINATION_DEFINITION_ID, DESTINATION_ID, WORKSPACE_ID, DESTINATION_CONFIGURATION);

    verify(attemptHandler).saveSyncConfig(new SaveAttemptSyncConfigRequestBody()
        .jobId(JOB_ID)
        .attemptNumber(ATTEMPT_NUMBER)
        .syncConfig(apiPojoConverters.attemptSyncConfigToApi(expectedAttemptSyncConfig, CONNECTION_ID)));
  }

  @Test
  void testGetCheckConnectionInputs() throws JsonValidationException, ConfigNotFoundException, IOException {
    final CheckInput syncInput = new CheckInput().jobId(JOB_ID).attemptNumber(ATTEMPT_NUMBER);

    final UUID sourceDefId = UUID.randomUUID();
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(SOURCE_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(sourceDefId)
        .withConfiguration(SOURCE_CONFIGURATION);
    when(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(sourceConnection);
    when(sourceService.getStandardSourceDefinition(sourceDefId)).thenReturn(mock(StandardSourceDefinition.class));
    when(oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefId, SOURCE_ID, WORKSPACE_ID, SOURCE_CONFIGURATION))
        .thenReturn(SOURCE_CONFIG_WITH_OAUTH);
    when(secretReferenceService.getConfigWithSecretReferences(SOURCE_ID, SOURCE_CONFIG_WITH_OAUTH, WORKSPACE_ID))
        .thenReturn(SOURCE_CONFIG_WITH_REFS);
    when(secretReferenceService.getConfigWithSecretReferences(DESTINATION_ID, DESTINATION_CONFIG_WITH_OAUTH, WORKSPACE_ID))
        .thenReturn(DESTINATION_CONFIG_WITH_REFS);

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDockerImage("destinationDockerImage")
        .withSourceDockerImage("sourceDockerImage")
        .withConfiguredAirbyteCatalog(mock(ConfiguredAirbyteCatalog.class));

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(JobConfig.ConfigType.SYNC)
        .withSync(jobSyncConfig);

    when(job.getConfig()).thenReturn(jobConfig);
    when(job.getScope()).thenReturn(CONNECTION_ID.toString());

    final IntegrationLauncherConfig expectedSourceLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER)
        .withWorkspaceId(WORKSPACE_ID)
        .withConnectionId(CONNECTION_ID)
        .withDockerImage(jobSyncConfig.getSourceDockerImage());

    final IntegrationLauncherConfig expectedDestinationLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_NUMBER)
        .withWorkspaceId(WORKSPACE_ID)
        .withConnectionId(CONNECTION_ID)
        .withDockerImage(jobSyncConfig.getDestinationDockerImage())
        .withAdditionalEnvironmentVariables(Collections.emptyMap());

    ActorContext sourceContext = new ActorContext().withActorId(SOURCE_ID);
    when(contextBuilder.fromSource(any())).thenReturn(sourceContext);

    ActorContext destinationContext = new ActorContext().withActorId(DESTINATION_ID);
    when(contextBuilder.fromDestination(any())).thenReturn(destinationContext);

    final StandardCheckConnectionInput expectedDestinationCheckInput = new StandardCheckConnectionInput()
        .withActorId(DESTINATION_ID)
        .withActorType(ActorType.DESTINATION)
        .withConnectionConfiguration(INLINED_DESTINATION_CONFIG_WITH_REFS)
        .withActorContext(destinationContext);

    final StandardCheckConnectionInput expectedSourceCheckInput = new StandardCheckConnectionInput()
        .withActorId(SOURCE_ID)
        .withActorType(ActorType.SOURCE)
        .withConnectionConfiguration(INLINED_SOURCE_CONFIG_WITH_REFS)
        .withActorContext(sourceContext);

    final SyncJobCheckConnectionInputs expectedCheckInputs = new SyncJobCheckConnectionInputs(
        expectedSourceLauncherConfig,
        expectedDestinationLauncherConfig,
        expectedSourceCheckInput,
        expectedDestinationCheckInput);

    final Object checkInputs = jobInputHandler.getCheckJobInput(syncInput);
    Assertions.assertEquals(expectedCheckInputs, checkInputs);
  }

  @Test
  void testIncludesFilesIsTrueIfConnectorsSupportFilesAndFileIsConfigured() {
    final ConfiguredAirbyteStream streamWithFilesEnabled = mock(ConfiguredAirbyteStream.class);
    when(streamWithFilesEnabled.getIncludeFiles()).thenReturn(true);

    final ActorDefinitionVersion sourceAdv = mock(ActorDefinitionVersion.class);
    final ActorDefinitionVersion destinationAdv = mock(ActorDefinitionVersion.class);
    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withConfiguredAirbyteCatalog(new ConfiguredAirbyteCatalog().withStreams(
            List.of(
                mock(ConfiguredAirbyteStream.class),
                streamWithFilesEnabled)));
    assertTrue(jobInputHandler.shouldIncludeFiles(jobSyncConfig, sourceAdv, destinationAdv));
  }

  @Test
  void testIncludesFilesIsFalseIfConnectorsSupportFilesAndFileIsNotConfigured() {
    final ActorDefinitionVersion sourceAdv = mock(ActorDefinitionVersion.class);
    final ActorDefinitionVersion destinationAdv = mock(ActorDefinitionVersion.class);
    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withConfiguredAirbyteCatalog(new ConfiguredAirbyteCatalog().withStreams(
            List.of(
                mock(ConfiguredAirbyteStream.class),
                mock(ConfiguredAirbyteStream.class))));
    assertFalse(jobInputHandler.shouldIncludeFiles(jobSyncConfig, sourceAdv, destinationAdv));
  }

}
