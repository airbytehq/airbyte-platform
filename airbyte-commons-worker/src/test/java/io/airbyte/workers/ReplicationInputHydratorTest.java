/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.ActorDefinitionVersionApi;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.ConnectionApi;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.model.generated.AirbyteCatalog;
import io.airbyte.api.client.model.generated.AirbyteStream;
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.client.model.generated.CatalogDiff;
import io.airbyte.api.client.model.generated.ConnectionAndJobIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.api.client.model.generated.DestinationRead;
import io.airbyte.api.client.model.generated.DestinationSyncMode;
import io.airbyte.api.client.model.generated.FieldTransform;
import io.airbyte.api.client.model.generated.JobConfigType;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.JobRead;
import io.airbyte.api.client.model.generated.JobStatus;
import io.airbyte.api.client.model.generated.ResetConfig;
import io.airbyte.api.client.model.generated.SaveStreamAttemptMetadataRequestBody;
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference;
import io.airbyte.api.client.model.generated.StreamAttemptMetadata;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamTransform;
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream;
import io.airbyte.api.client.model.generated.SyncMode;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.State;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.metrics.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.helper.BackfillHelper;
import io.airbyte.workers.helper.CatalogDiffConverter;
import io.airbyte.workers.helper.MapperSecretHydrationHelper;
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper;
import io.airbyte.workers.hydration.ConnectorSecretsHydrator;
import io.airbyte.workers.input.ReplicationInputMapper;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.api.CollectionAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests for the replication activity specifically.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class ReplicationInputHydratorTest {

  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final UUID DESTINATION_ID = UUID.randomUUID();
  private static final JsonNode SOURCE_CONFIG = JsonNodeFactory.instance.objectNode();
  private static final JsonNode DESTINATION_CONFIG = JsonNodeFactory.instance.objectNode();
  private static final String CONNECTION_NAME = "connection-name";

  private static final String TEST_STREAM_NAME = "test-stream-name";
  private static final String TEST_STREAM_NAMESPACE = "test-stream-namespace";
  private static final AirbyteCatalog SYNC_CATALOG = new AirbyteCatalog(List.of(
      new AirbyteStreamAndConfiguration(
          new AirbyteStream(
              TEST_STREAM_NAME,
              Jsons.emptyObject(),
              List.of(SyncMode.INCREMENTAL),
              null,
              null,
              null,
              TEST_STREAM_NAMESPACE,
              null,
              null),
          new AirbyteStreamConfiguration(
              SyncMode.INCREMENTAL,
              DestinationSyncMode.APPEND,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null))));
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
  private static final Long JOB_ID = 123L;
  private static final Long ATTEMPT_NUMBER = 2L;
  private static final JobRunConfig JOB_RUN_CONFIG = new JobRunConfig().withJobId(JOB_ID.toString()).withAttemptId(ATTEMPT_NUMBER);
  private static final IntegrationLauncherConfig DESTINATION_LAUNCHER_CONFIG =
      new IntegrationLauncherConfig().withDockerImage("dockerimage:dockertag");
  private static final IntegrationLauncherConfig SOURCE_LAUNCHER_CONFIG = new IntegrationLauncherConfig();
  private static final SyncResourceRequirements SYNC_RESOURCE_REQUIREMENTS = new SyncResourceRequirements();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final Boolean useRuntimePersistence = false;
  private static final CatalogDiff CATALOG_DIFF = new CatalogDiff(
      List.of(
          new StreamTransform(
              StreamTransform.TransformType.UPDATE_STREAM,
              new StreamDescriptor(SYNC_CATALOG.getStreams().getFirst().getStream().getName(),
                  SYNC_CATALOG.getStreams().getFirst().getStream().getNamespace()),
              new StreamTransformUpdateStream(List.of(new FieldTransform(
                  FieldTransform.TransformType.ADD_FIELD,
                  List.of(),
                  false,
                  null,
                  null,
                  null)),
                  List.of()))));
  private static final DestinationRead DESTINATION_READ = new DestinationRead(
      UUID.randomUUID(),
      UUID.randomUUID(),
      UUID.randomUUID(),
      Jsons.emptyObject(),
      "name",
      "destinationName",
      1L,
      "icon",
      false,
      true,
      null,
      null,
      null,
      null);

  private static MapperSecretHydrationHelper mapperSecretHydrationHelper;
  private static ConnectorSecretsHydrator connectorSecretsHydrator;
  private static AirbyteApiClient airbyteApiClient;
  private static ConnectionApi connectionApi;
  private static StateApi stateApi;
  private static JobsApi jobsApi;
  private SecretsPersistenceConfigApi secretsPersistenceConfigApi;
  private ActorDefinitionVersionApi actorDefinitionVersionApi;
  private AttemptApi attemptApi;
  private DestinationApi destinationApi;
  private ResumableFullRefreshStatsHelper resumableFullRefreshStatsHelper;
  private BackfillHelper backfillHelper;
  private CatalogClientConverters catalogClientConverters;
  private MetricClient metricClient;

  @BeforeEach
  void setup() throws IOException {
    mapperSecretHydrationHelper = mock(MapperSecretHydrationHelper.class);
    connectorSecretsHydrator = mock(ConnectorSecretsHydrator.class);
    airbyteApiClient = mock(AirbyteApiClient.class);
    attemptApi = mock(AttemptApi.class);
    connectionApi = mock(ConnectionApi.class);
    stateApi = mock(StateApi.class);
    jobsApi = mock(JobsApi.class);
    secretsPersistenceConfigApi = mock(SecretsPersistenceConfigApi.class);
    actorDefinitionVersionApi = mock(ActorDefinitionVersionApi.class);
    destinationApi = mock(DestinationApi.class);
    resumableFullRefreshStatsHelper = mock(ResumableFullRefreshStatsHelper.class);
    catalogClientConverters = new CatalogClientConverters(new FieldGenerator());
    backfillHelper = new BackfillHelper(catalogClientConverters);
    metricClient = mock(MetricClient.class);
    when(destinationApi.getBaseUrl()).thenReturn("http://localhost:8001/api");
    when(destinationApi.getDestination(any())).thenReturn(DESTINATION_READ);
    when(airbyteApiClient.getAttemptApi()).thenReturn(attemptApi);
    when(airbyteApiClient.getConnectionApi()).thenReturn(connectionApi);
    when(airbyteApiClient.getDestinationApi()).thenReturn(destinationApi);
    when(airbyteApiClient.getStateApi()).thenReturn(stateApi);
    when(airbyteApiClient.getJobsApi()).thenReturn(jobsApi);
    when(airbyteApiClient.getSecretPersistenceConfigApi()).thenReturn(secretsPersistenceConfigApi);
    when(airbyteApiClient.getActorDefinitionVersionApi()).thenReturn(actorDefinitionVersionApi);
    when(airbyteApiClient.getDestinationApi()).thenReturn(destinationApi);
    when(stateApi.getState(new ConnectionIdRequestBody(CONNECTION_ID))).thenReturn(CONNECTION_STATE_RESPONSE);
    when(mapperSecretHydrationHelper.hydrateMapperSecrets(any(), anyBoolean(), any())).thenAnswer(invocation -> invocation.getArgument(0));
  }

  private ReplicationInputHydrator getReplicationInputHydrator() {
    return new ReplicationInputHydrator(
        airbyteApiClient,
        resumableFullRefreshStatsHelper,
        mapperSecretHydrationHelper,
        backfillHelper,
        catalogClientConverters,
        new ReplicationInputMapper(),
        metricClient,
        connectorSecretsHydrator,
        useRuntimePersistence);
  }

  private ReplicationActivityInput getDefaultReplicationActivityInputForTest(final boolean supportsRefresh) {
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
        "unused",
        false,
        JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT,
        "unused",
        "unused",
        null, // unused
        new ConnectionContext().withWorkspaceId(UUID.randomUUID()).withOrganizationId(UUID.randomUUID()),
        null,
        List.of(),
        false,
        false,
        Map.of(),
        null,
        supportsRefresh,
        null,
        null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGenerateReplicationInputRetrievesInputs(final boolean withRefresh) throws Exception {
    if (withRefresh) {
      mockRefresh();
    } else {
      mockNonRefresh();
    }
    // Verify that we get the state and catalog from the API.
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();

    final var replicationActivityInput = getDefaultReplicationActivityInputForTest(withRefresh);
    final var replicationInput = replicationInputHydrator.getHydratedReplicationInput(replicationActivityInput);
    assertEquals(EXPECTED_STATE, replicationInput.getState());
    assertEquals(1, replicationInput.getCatalog().getStreams().size());
    assertEquals(TEST_STREAM_NAME, replicationInput.getCatalog().getStreams().get(0).getStream().getName());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGenerateReplicationInputHandlesResets(final boolean withRefresh) throws Exception {
    if (withRefresh) {
      mockRefresh();
    } else {
      mockNonRefresh();
    }
    // Verify that if the sync is a reset, we retrieve the job info and handle the streams accordingly.
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final ReplicationActivityInput input = getDefaultReplicationActivityInputForTest(withRefresh);
    input.setReset(true);
    when(jobsApi.getLastReplicationJob(new ConnectionIdRequestBody(CONNECTION_ID))).thenReturn(
        new JobOptionalRead(new JobRead(
            JOB_ID,
            JobConfigType.SYNC,
            CONNECTION_ID.toString(),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            JobStatus.CANCELLED,
            null,
            null,
            new ResetConfig(List.of(new StreamDescriptor(TEST_STREAM_NAME, TEST_STREAM_NAMESPACE))),
            null,
            null,
            null)));
    final var replicationInput = replicationInputHydrator.getHydratedReplicationInput(input);
    assertEquals(1, replicationInput.getCatalog().getStreams().size());
    assertEquals(io.airbyte.config.SyncMode.FULL_REFRESH, replicationInput.getCatalog().getStreams().getFirst().getSyncMode());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGenerateReplicationInputHandlesBackfills(final boolean withRefresh) throws Exception {
    if (withRefresh) {
      mockRefresh();
    } else {
      mockNonRefresh();
    }
    // Verify that if backfill is enabled, and we have an appropriate diff, then we clear the state for
    // the affected streams.
    mockEnableBackfillForConnection(withRefresh);
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final ReplicationActivityInput input = getDefaultReplicationActivityInputForTest(withRefresh);

    input.setSchemaRefreshOutput(new RefreshSchemaActivityOutput(CatalogDiffConverter.toDomain(CATALOG_DIFF)));
    final var replicationInput = replicationInputHydrator.getHydratedReplicationInput(input);
    final var typedState = StateMessageHelper.getTypedState(replicationInput.getState().getState());
    assertEquals(JsonNodeFactory.instance.nullNode(), typedState.get().getStateMessages().get(0).getStream().getStreamState());
  }

  @Test
  void testTrackBackfillAndResume() throws IOException {
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final io.airbyte.config.StreamDescriptor stream1 = new io.airbyte.config.StreamDescriptor().withName("s1").withNamespace("ns1");
    final io.airbyte.config.StreamDescriptor stream2 = new io.airbyte.config.StreamDescriptor().withName("s1");
    final io.airbyte.config.StreamDescriptor stream3 = new io.airbyte.config.StreamDescriptor().withName("s1").withNamespace("ns2");
    final io.airbyte.config.StreamDescriptor stream4 = new io.airbyte.config.StreamDescriptor().withName("s2");
    replicationInputHydrator.trackBackfillAndResume(
        1L,
        2L,
        List.of(stream1, stream2, stream4),
        List.of(stream1, stream3, stream4));

    final SaveStreamAttemptMetadataRequestBody expectedRequest = new SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        List.of(
            new StreamAttemptMetadata("s1", true, true, "ns1"),
            new StreamAttemptMetadata("s1", false, true, null),
            new StreamAttemptMetadata("s1", true, false, "ns2"),
            new StreamAttemptMetadata("s2", true, true, null)));

    final ArgumentCaptor<SaveStreamAttemptMetadataRequestBody> captor = ArgumentCaptor.forClass(SaveStreamAttemptMetadataRequestBody.class);
    verify(attemptApi).saveStreamMetadata(captor.capture());
    assertEquals(expectedRequest.getJobId(), captor.getValue().getJobId());
    assertEquals(expectedRequest.getAttemptNumber(), captor.getValue().getAttemptNumber());
    CollectionAssert.assertThatCollection(captor.getValue().getStreamMetadata())
        .containsExactlyInAnyOrderElementsOf(expectedRequest.getStreamMetadata());
  }

  @Test
  void testTrackBackfillAndResumeWithoutBackfill() throws IOException {
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final io.airbyte.config.StreamDescriptor stream1 = new io.airbyte.config.StreamDescriptor().withName("s1").withNamespace("ns1");
    final io.airbyte.config.StreamDescriptor stream2 = new io.airbyte.config.StreamDescriptor().withName("s1");
    final io.airbyte.config.StreamDescriptor stream4 = new io.airbyte.config.StreamDescriptor().withName("s2");
    replicationInputHydrator.trackBackfillAndResume(
        1L,
        2L,
        List.of(stream1, stream2, stream4),
        null);

    final SaveStreamAttemptMetadataRequestBody expectedRequest = new SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        List.of(
            new StreamAttemptMetadata("s1", false, true, "ns1"),
            new StreamAttemptMetadata("s1", false, true, null),
            new StreamAttemptMetadata("s2", false, true, null)));

    final ArgumentCaptor<SaveStreamAttemptMetadataRequestBody> captor = ArgumentCaptor.forClass(SaveStreamAttemptMetadataRequestBody.class);
    verify(attemptApi).saveStreamMetadata(captor.capture());
    assertEquals(expectedRequest.getJobId(), captor.getValue().getJobId());
    assertEquals(expectedRequest.getAttemptNumber(), captor.getValue().getAttemptNumber());
    CollectionAssert.assertThatCollection(captor.getValue().getStreamMetadata())
        .containsExactlyInAnyOrderElementsOf(expectedRequest.getStreamMetadata());
  }

  @Test
  void testTrackBackfillAndResumeWithoutResume() throws IOException {
    final ReplicationInputHydrator replicationInputHydrator = getReplicationInputHydrator();
    final io.airbyte.config.StreamDescriptor stream1 = new io.airbyte.config.StreamDescriptor().withName("s1").withNamespace("ns1");
    final io.airbyte.config.StreamDescriptor stream3 = new io.airbyte.config.StreamDescriptor().withName("s1").withNamespace("ns2");
    final io.airbyte.config.StreamDescriptor stream4 = new io.airbyte.config.StreamDescriptor().withName("s2");
    replicationInputHydrator.trackBackfillAndResume(
        1L,
        2L,
        null,
        List.of(stream1, stream3, stream4));

    final SaveStreamAttemptMetadataRequestBody expectedRequest = new SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        List.of(
            new StreamAttemptMetadata("s1", true, false, "ns1"),
            new StreamAttemptMetadata("s1", true, false, "ns2"),
            new StreamAttemptMetadata("s2", true, false, null)));

    final ArgumentCaptor<SaveStreamAttemptMetadataRequestBody> captor = ArgumentCaptor.forClass(SaveStreamAttemptMetadataRequestBody.class);
    verify(attemptApi).saveStreamMetadata(captor.capture());
    assertEquals(expectedRequest.getJobId(), captor.getValue().getJobId());
    assertEquals(expectedRequest.getAttemptNumber(), captor.getValue().getAttemptNumber());
    CollectionAssert.assertThatCollection(captor.getValue().getStreamMetadata())
        .containsExactlyInAnyOrderElementsOf(expectedRequest.getStreamMetadata());
  }

  private void mockEnableBackfillForConnection(final boolean withRefresh) throws IOException {
    if (withRefresh) {
      when(connectionApi.getConnectionForJob(new ConnectionAndJobIdRequestBody(CONNECTION_ID, JOB_ID)))
          .thenReturn(new ConnectionRead(CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, SYNC_CATALOG, ConnectionStatus.ACTIVE, false,
              null, null, null, null, null, null, null, null, null, null, null, null, null, null, SchemaChangeBackfillPreference.ENABLED, null, null,
              null));
    } else {
      when(connectionApi.getConnection(new ConnectionIdRequestBody(CONNECTION_ID)))
          .thenReturn(new ConnectionRead(CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, SYNC_CATALOG, ConnectionStatus.ACTIVE, false,
              null, null, null, null, null, null, null, null, null, null, null, null, null, null, SchemaChangeBackfillPreference.ENABLED, null,
              null, null));
    }
  }

  private void mockRefresh() throws IOException {
    when(connectionApi.getConnectionForJob(new ConnectionAndJobIdRequestBody(CONNECTION_ID, JOB_ID)))
        .thenReturn(new ConnectionRead(CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, SYNC_CATALOG, ConnectionStatus.ACTIVE, false, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));
  }

  private void mockNonRefresh() throws IOException {
    when(connectionApi.getConnection(new ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(new ConnectionRead(CONNECTION_ID, CONNECTION_NAME, SOURCE_ID, DESTINATION_ID, SYNC_CATALOG, ConnectionStatus.ACTIVE, false, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));
  }

}
