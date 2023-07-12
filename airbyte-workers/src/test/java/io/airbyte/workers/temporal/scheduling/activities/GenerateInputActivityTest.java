/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.State;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity.SyncInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity.SyncJobCheckConnectionInputs;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenerateInputActivityTest {

  private static StateApi stateApi;
  private static AttemptApi attemptApi;
  private static JobPersistence jobPersistence;
  private static ConfigRepository configRepository;
  private static ConfigInjector configInjector;
  private static GenerateInputActivityImpl generateInputActivity;
  private static OAuthConfigSupplier oAuthConfigSupplier;
  private static Job job;
  private static FeatureFlagClient featureFlagClient;
  private static Map<String, Boolean> featureFlagMap;
  private static JobsApi jobsApi;
  private static ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private static final JsonNode SOURCE_CONFIGURATION = Jsons.jsonNode(Map.of("source_key", "source_value"));
  private static final JsonNode SOURCE_CONFIG_WITH_OAUTH = Jsons.jsonNode(Map.of("source_key", "source_value", "oauth", "oauth_value"));
  private static final JsonNode SOURCE_CONFIG_WITH_OAUTH_AND_INJECTED_CONFIG =
      Jsons.jsonNode(Map.of("source_key", "source_value", "oauth", "oauth_value", "injected", "value"));
  private static final JsonNode DESTINATION_CONFIGURATION = Jsons.jsonNode(Map.of("destination_key", "destination_value"));
  private static final JsonNode DESTINATION_CONFIG_WITH_OAUTH =
      Jsons.jsonNode(Map.of("destination_key", "destination_value", "oauth", "oauth_value"));
  private static final State STATE = new State().withState(Jsons.jsonNode(Map.of("state_key", "state_value")));

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final long JOB_ID = 1;
  private static final int ATTEMPT_ID = 1;
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_DEFINITION_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final UUID CONNECTION_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() throws IOException, JsonValidationException, ConfigNotFoundException, ApiException {
    final FeatureFlags featureFlags = mock(FeatureFlags.class);
    featureFlagMap = new HashMap<>();
    featureFlagClient = new TestClient(featureFlagMap);

    oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    stateApi = mock(StateApi.class);
    attemptApi = mock(AttemptApi.class);
    jobPersistence = mock(JobPersistence.class);
    configRepository = mock(ConfigRepository.class);
    configInjector = mock(ConfigInjector.class);
    jobsApi = mock(JobsApi.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    generateInputActivity = new GenerateInputActivityImpl(jobPersistence, configRepository, stateApi, attemptApi, featureFlags,
        featureFlagClient, oAuthConfigSupplier, configInjector, jobsApi, actorDefinitionVersionHelper);

    job = mock(Job.class);

    when(jobPersistence.getJob(JOB_ID)).thenReturn(job);
    when(configInjector.injectConfig(any(), any())).thenAnswer(i -> i.getArguments()[0]);

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(DESTINATION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationDefinitionId(DESTINATION_DEFINITION_ID)
        .withConfiguration(DESTINATION_CONFIGURATION);
    when(configRepository.getDestinationConnection(DESTINATION_ID)).thenReturn(destinationConnection);
    final StandardDestinationDefinition destinationDefinition = mock(StandardDestinationDefinition.class);
    when(configRepository.getStandardDestinationDefinition(DESTINATION_DEFINITION_ID)).thenReturn(destinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, DESTINATION_ID))
        .thenReturn(mock(ActorDefinitionVersion.class));
    when(oAuthConfigSupplier.injectDestinationOAuthParameters(DESTINATION_DEFINITION_ID, DESTINATION_ID, WORKSPACE_ID, DESTINATION_CONFIGURATION))
        .thenReturn(DESTINATION_CONFIG_WITH_OAUTH);

    final StandardSync standardSync = new StandardSync()
        .withSourceId(SOURCE_ID)
        .withDestinationId(DESTINATION_ID);
    when(configRepository.getStandardSync(CONNECTION_ID)).thenReturn(standardSync);
  }

  @Test
  void testGetCheckConnectionInputs() throws JsonValidationException, ConfigNotFoundException, IOException {
    final SyncInputWithAttemptNumber syncInput = new SyncInputWithAttemptNumber(ATTEMPT_ID, JOB_ID);

    final UUID sourceDefId = UUID.randomUUID();
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(SOURCE_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceDefinitionId(sourceDefId)
        .withConfiguration(SOURCE_CONFIGURATION);
    when(configRepository.getSourceConnection(SOURCE_ID)).thenReturn(sourceConnection);
    when(configRepository.getStandardSourceDefinition(sourceDefId)).thenReturn(mock(StandardSourceDefinition.class));
    when(oAuthConfigSupplier.injectSourceOAuthParameters(sourceDefId, SOURCE_ID, WORKSPACE_ID, SOURCE_CONFIGURATION))
        .thenReturn(SOURCE_CONFIG_WITH_OAUTH);

    final JobSyncConfig jobSyncConfig = new JobSyncConfig()
        .withWorkspaceId(UUID.randomUUID())
        .withDestinationDockerImage("destinationDockerImage")
        .withSourceDockerImage("sourceDockerImage")
        .withConfiguredAirbyteCatalog(mock(ConfiguredAirbyteCatalog.class));

    final JobConfig jobConfig = new JobConfig()
        .withConfigType(ConfigType.SYNC)
        .withSync(jobSyncConfig);

    when(job.getConfig()).thenReturn(jobConfig);
    when(job.getScope()).thenReturn(CONNECTION_ID.toString());

    final IntegrationLauncherConfig expectedSourceLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_ID)
        .withDockerImage(jobSyncConfig.getSourceDockerImage());

    final IntegrationLauncherConfig expectedDestinationLauncherConfig = new IntegrationLauncherConfig()
        .withJobId(String.valueOf(JOB_ID))
        .withAttemptId((long) ATTEMPT_ID)
        .withDockerImage(jobSyncConfig.getDestinationDockerImage())
        .withAdditionalEnvironmentVariables(Collections.emptyMap());

    final StandardCheckConnectionInput expectedDestinationCheckInput = new StandardCheckConnectionInput()
        .withActorId(DESTINATION_ID)
        .withActorType(ActorType.DESTINATION)
        .withConnectionConfiguration(DESTINATION_CONFIG_WITH_OAUTH);

    final StandardCheckConnectionInput expectedSourceCheckInput = new StandardCheckConnectionInput()
        .withActorId(SOURCE_ID)
        .withActorType(ActorType.SOURCE)
        .withConnectionConfiguration(SOURCE_CONFIG_WITH_OAUTH);

    final SyncJobCheckConnectionInputs expectedCheckInputs = new SyncJobCheckConnectionInputs(
        expectedSourceLauncherConfig,
        expectedDestinationLauncherConfig,
        expectedSourceCheckInput,
        expectedDestinationCheckInput);

    final SyncJobCheckConnectionInputs checkInputs = generateInputActivity.getCheckConnectionInputs(syncInput);
    assertEquals(expectedCheckInputs, checkInputs);
  }

}
