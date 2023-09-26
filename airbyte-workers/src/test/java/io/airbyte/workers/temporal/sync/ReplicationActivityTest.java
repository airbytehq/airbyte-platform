/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.ResetConfig;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.Configs;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.State;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.RemoveLargeSyncInputs;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.ReplicationActivityInput;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the replication activity specifically.
 */
class ReplicationActivityTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final JsonNode SOURCE_CONFIG = JsonNodeFactory.instance.objectNode();
  private static final JsonNode DESTINATION_CONFIG = JsonNodeFactory.instance.objectNode();

  private static final String TEST_STREAM_NAME = "test-stream-name";
  private static final String TEST_STREAM_NAMESPACE = "test-stream-namespace";
  private static final AirbyteCatalog SYNC_CATALOG = new AirbyteCatalog().addStreamsItem(new AirbyteStreamAndConfiguration()
      .stream(new AirbyteStream().addSupportedSyncModesItem(SyncMode.INCREMENTAL).name(TEST_STREAM_NAME).namespace(TEST_STREAM_NAMESPACE))
      .config(new AirbyteStreamConfiguration()));
  private static final ConnectionState CONNECTION_STATE_RESPONSE = Jsons.deserialize(String
      .format("""
              {
                "stateType": "stream",
                "connectionId": "%s",
                "state": null,
                "streamState": [{
                  "streamDescriptor":  {
                    "name": "id_and_name",
                    "namespace": "public"
                  },
                  "streamState": {"cursor":"6","stream_name":"id_and_name","cursor_field":["id"],"stream_namespace":"public","cursor_record_count":1}
                }],
                "globalState": null
              }
              """, CONNECTION_ID), ConnectionState.class);
  private static final State EXPECTED_STATE = new State().withState(Jsons.deserialize(
      """
      [{
        "type":"STREAM",
        "stream":{
          "stream_descriptor":{
            "name":"id_and_name",
            "namespace":"public"
          },
          "stream_state":{"cursor":"6","stream_name":"id_and_name","cursor_field":["id"],"stream_namespace":"public","cursor_record_count":1}
        }
      }]
      """));
  private static final JobRunConfig JOB_RUN_CONFIG = new JobRunConfig();
  private static final IntegrationLauncherConfig DESTINATION_LAUNCHER_CONFIG = new IntegrationLauncherConfig();
  private static final IntegrationLauncherConfig SOURCE_LAUNCHER_CONFIG = new IntegrationLauncherConfig();
  private static final SyncResourceRequirements SYNC_RESOURCE_REQUIREMENTS = new SyncResourceRequirements();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static SecretsHydrator secretsHydrator;
  private static AirbyteApiClient airbyteApiClient;
  private static ConnectionApi connectionApi;
  private static StateApi stateApi;
  private static JobsApi jobsApi;
  private static FeatureFlagClient featureFlagClient;

  @BeforeEach
  void setup() {
    secretsHydrator = mock(SecretsHydrator.class);
    airbyteApiClient = mock(AirbyteApiClient.class);
    connectionApi = mock(ConnectionApi.class);
    stateApi = mock(StateApi.class);
    jobsApi = mock(JobsApi.class);
    featureFlagClient = mock(TestClient.class);
    when(airbyteApiClient.getConnectionApi()).thenReturn(connectionApi);
    when(airbyteApiClient.getStateApi()).thenReturn(stateApi);
    when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
    when(featureFlagClient.boolVariation(eq(RemoveLargeSyncInputs.INSTANCE), any())).thenReturn(true);
  }

  private ReplicationActivityImpl getDefaultReplicationActivityForTest() {
    return new ReplicationActivityImpl(
        secretsHydrator,
        null, // unused
        Configs.WorkerEnvironment.DOCKER, // unused
        LogConfigs.EMPTY, // unused,
        "airbyte-version-unused",
        null, // unused
        null, // unused
        airbyteApiClient,
        null, // unused
        null, // unused,
        featureFlagClient);
  }

  private ReplicationActivityInput getDefaultReplicationActivityInputForTest() {
    return new ReplicationActivityInput(
        null, // unused
        SOURCE_ID,
        DESTINATION_ID,
        SOURCE_CONFIG,
        DESTINATION_CONFIG,
        JOB_RUN_CONFIG,
        SOURCE_LAUNCHER_CONFIG,
        DESTINATION_LAUNCHER_CONFIG,
        SYNC_RESOURCE_REQUIREMENTS,
        WORKSPACE_ID,
        CONNECTION_ID,
        false,
        "unused",
        false,
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        "unused",
        "unused");
  }

  @Test
  void testGenerateReplicationInputRetrievesInputs() throws Exception {
    // Verify that we get the state and catalog from the API.
    final ReplicationActivityImpl replicationActivity = getDefaultReplicationActivityForTest();
    when(connectionApi.getConnection(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(new ConnectionRead()
        .connectionId(CONNECTION_ID)
        .syncCatalog(SYNC_CATALOG));
    when(stateApi.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(CONNECTION_STATE_RESPONSE);

    final var replicationActivityInput = getDefaultReplicationActivityInputForTest();
    final var replicationInput = replicationActivity.getHydratedReplicationInput(replicationActivityInput);
    assertEquals(EXPECTED_STATE, replicationInput.getState());
    assertEquals(1, replicationInput.getCatalog().getStreams().size());
    assertEquals(TEST_STREAM_NAME, replicationInput.getCatalog().getStreams().get(0).getStream().getName());
  }

  @Test
  void testGenerateReplicationInputHandlesResets() throws Exception {
    // Verify that if the sync is a reset, we retrieve the job info and handle the streams accordingly.
    final ReplicationActivityImpl replicationActivity = getDefaultReplicationActivityForTest();
    final ReplicationActivityInput input = getDefaultReplicationActivityInputForTest();
    input.setIsReset(true);
    when(jobsApi.getLastReplicationJob(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(
        new JobOptionalRead().job(new JobRead().resetConfig(new ResetConfig().streamsToReset(List.of(
            new StreamDescriptor().name(TEST_STREAM_NAME).namespace(TEST_STREAM_NAMESPACE))))));
    when(connectionApi.getConnection(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(new ConnectionRead()
        .connectionId(CONNECTION_ID)
        .syncCatalog(SYNC_CATALOG));
    final var replicationInput = replicationActivity.getHydratedReplicationInput(input);
    assertEquals(1, replicationInput.getCatalog().getStreams().size());
    assertEquals(io.airbyte.protocol.models.SyncMode.FULL_REFRESH, replicationInput.getCatalog().getStreams().get(0).getSyncMode());
  }

}
