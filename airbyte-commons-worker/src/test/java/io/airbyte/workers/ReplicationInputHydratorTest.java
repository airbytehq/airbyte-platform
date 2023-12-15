/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.FieldTransform;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.ResetConfig;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.State;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the replication activity specifically.
 */
class ReplicationInputHydratorTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final JsonNode SOURCE_CONFIG = JsonNodeFactory.instance.objectNode();
  private static final JsonNode DESTINATION_CONFIG = JsonNodeFactory.instance.objectNode();

  private static final String TEST_STREAM_NAME = "test-stream-name";
  private static final String TEST_STREAM_NAMESPACE = "test-stream-namespace";
  private static final AirbyteCatalog SYNC_CATALOG = new AirbyteCatalog().addStreamsItem(new AirbyteStreamAndConfiguration()
      .stream(new AirbyteStream().addSupportedSyncModesItem(SyncMode.INCREMENTAL).name(TEST_STREAM_NAME).namespace(TEST_STREAM_NAMESPACE))
      .config(new AirbyteStreamConfiguration().syncMode(SyncMode.INCREMENTAL)));
  private static final ConnectionState CONNECTION_STATE_RESPONSE = Jsons.deserialize(String
      .format("""
              {
                "stateType": "stream",
                "connectionId": "%s",
                "state": null,
                "streamState": [{
                  "streamDescriptor":  {
                    "name": "%s",
                    "namespace": "%s"
                  },
                  "streamState": {"cursor":"6","stream_name":"%s","cursor_field":["id"],"stream_namespace":"%s","cursor_record_count":1}
                }],
                "globalState": null
              }
              """, CONNECTION_ID, TEST_STREAM_NAME, TEST_STREAM_NAMESPACE, TEST_STREAM_NAME, TEST_STREAM_NAMESPACE), ConnectionState.class);
  private static final State EXPECTED_STATE = new State().withState(Jsons.deserialize(
      """
      [{
        "type":"STREAM",
        "stream":{
          "stream_descriptor":{
            "name":"test-stream-name",
            "namespace":"test-stream-namespace"
          },
          "stream_state":{"cursor":"6","stream_name":"test-stream-name","cursor_field":["id"],
          "stream_namespace":"test-stream-namespace","cursor_record_count":1}
        }
      }]
      """));
  private static final JobRunConfig JOB_RUN_CONFIG = new JobRunConfig();
  private static final IntegrationLauncherConfig DESTINATION_LAUNCHER_CONFIG = new IntegrationLauncherConfig();
  private static final IntegrationLauncherConfig SOURCE_LAUNCHER_CONFIG = new IntegrationLauncherConfig();
  private static final SyncResourceRequirements SYNC_RESOURCE_REQUIREMENTS = new SyncResourceRequirements();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final CatalogDiff CATALOG_DIFF = new CatalogDiff()
      .addTransformsItem(new StreamTransform()
          .streamDescriptor(new StreamDescriptor()
              .name(SYNC_CATALOG.getStreams().get(0).getStream().getName())
              .namespace(SYNC_CATALOG.getStreams().get(0).getStream().getNamespace()))
          .transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
          .addUpdateStreamItem(new FieldTransform()
              .transformType(FieldTransform.TransformTypeEnum.ADD_FIELD)));
  private static SecretsRepositoryReader secretsRepositoryReader;
  private static AirbyteApiClient airbyteApiClient;
  private static ConnectionApi connectionApi;
  private static StateApi stateApi;
  private static JobsApi jobsApi;
  private static FeatureFlagClient featureFlagClient;
  private SecretsPersistenceConfigApi secretsPersistenceConfigApi;

  @BeforeEach
  void setup() throws ApiException {
    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    airbyteApiClient = mock(AirbyteApiClient.class);
    connectionApi = mock(ConnectionApi.class);
    stateApi = mock(StateApi.class);
    jobsApi = mock(JobsApi.class);
    featureFlagClient = mock(TestClient.class);
    secretsPersistenceConfigApi = mock(SecretsPersistenceConfigApi.class);
    when(airbyteApiClient.getConnectionApi()).thenReturn(connectionApi);
    when(airbyteApiClient.getStateApi()).thenReturn(stateApi);
    when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
    when(airbyteApiClient.getSecretPersistenceConfigApi()).thenReturn(secretsPersistenceConfigApi);
    when(connectionApi.getConnection(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(new ConnectionRead()
        .connectionId(CONNECTION_ID)
        .syncCatalog(SYNC_CATALOG));
    when(stateApi.getState(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(CONNECTION_STATE_RESPONSE);
  }

  private ReplicationInputHydrator getReplicationInputHydrator() {
    return new ReplicationInputHydrator(
        airbyteApiClient.getConnectionApi(),
        airbyteApiClient.getJobsApi(),
        airbyteApiClient.getStateApi(),
        secretsPersistenceConfigApi, secretsRepositoryReader,
        featureFlagClient);
  }

  private ReplicationActivityInput getDefaultReplicationActivityInputForTest() {
    return new ReplicationActivityInput(
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
        "unused",
        null, // unused
        new ConnectionContext().withOrganizationId(UUID.randomUUID()),
        false,
        false);
  }

  @Test
  void testGenerateReplicationInputRetrievesInputs() throws Exception {
    // Verify that we get the state and catalog from the API.
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();

    final var replicationActivityInput = getDefaultReplicationActivityInputForTest();
    final var replicationInput = replicationInputHydrator.getHydratedReplicationInput(replicationActivityInput);
    assertEquals(EXPECTED_STATE, replicationInput.getState());
    assertEquals(1, replicationInput.getCatalog().getStreams().size());
    assertEquals(TEST_STREAM_NAME, replicationInput.getCatalog().getStreams().get(0).getStream().getName());
  }

  @Test
  void testGenerateReplicationInputHandlesResets() throws Exception {
    // Verify that if the sync is a reset, we retrieve the job info and handle the streams accordingly.
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final ReplicationActivityInput input = getDefaultReplicationActivityInputForTest();
    input.setIsReset(true);
    when(jobsApi.getLastReplicationJob(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(
        new JobOptionalRead().job(new JobRead().resetConfig(new ResetConfig().streamsToReset(List.of(
            new StreamDescriptor().name(TEST_STREAM_NAME).namespace(TEST_STREAM_NAMESPACE))))));
    when(connectionApi.getConnection(new ConnectionIdRequestBody().connectionId(CONNECTION_ID))).thenReturn(new ConnectionRead()
        .connectionId(CONNECTION_ID)
        .syncCatalog(SYNC_CATALOG));
    final var replicationInput = replicationInputHydrator.getHydratedReplicationInput(input);
    assertEquals(1, replicationInput.getCatalog().getStreams().size());
    assertEquals(io.airbyte.protocol.models.SyncMode.FULL_REFRESH, replicationInput.getCatalog().getStreams().get(0).getSyncMode());
  }

  @Test
  void testGenerateReplicationInputHandlesBackfills() throws Exception {
    // Verify that if we have input from the schema refresh activity, that we clear state accordingly to
    // prepare for backfills.
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final ReplicationActivityInput input = getDefaultReplicationActivityInputForTest();
    input.setSchemaRefreshOutput(new RefreshSchemaActivityOutput(CATALOG_DIFF));
    final var replicationInput = replicationInputHydrator.getHydratedReplicationInput(input);
    final var typedState = StateMessageHelper.getTypedState(replicationInput.getState().getState());
    assertEquals(JsonNodeFactory.instance.nullNode(), typedState.get().getStateMessages().get(0).getStream().getStreamState());
  }

}
