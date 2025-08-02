/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ActorDefinitionVersionApi
import io.airbyte.api.client.generated.AttemptApi
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.DestinationApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.generated.StateApi
import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.CatalogDiff
import io.airbyte.api.client.model.generated.ConnectionAndJobIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionState
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.FieldTransform
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobOptionalRead
import io.airbyte.api.client.model.generated.JobRead
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.ResetConfig
import io.airbyte.api.client.model.generated.SaveStreamAttemptMetadataRequestBody
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.client.model.generated.StreamAttemptMetadata
import io.airbyte.api.client.model.generated.StreamAttributeTransform
import io.airbyte.api.client.model.generated.StreamDescriptor
import io.airbyte.api.client.model.generated.StreamTransform
import io.airbyte.api.client.model.generated.StreamTransformUpdateStream
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.State
import io.airbyte.config.StateWrapper
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.config.helpers.StateMessageHelper.getTypedState
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.helper.BackfillHelper
import io.airbyte.workers.helper.CatalogDiffConverter.toDomain
import io.airbyte.workers.helper.MapperSecretHydrationHelper
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import org.assertj.core.api.CollectionAssert
import org.junit.Assert
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentCaptor
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.any
import org.mockito.stubbing.Answer
import java.io.IOException
import java.util.List
import java.util.Map
import java.util.Optional
import java.util.UUID

/**
 * Tests for the replication activity specifically.
 */
internal class ReplicationInputHydratorTest {
  private var secretsPersistenceConfigApi: SecretsPersistenceConfigApi? = null
  private var actorDefinitionVersionApi: ActorDefinitionVersionApi? = null
  private var attemptApi: AttemptApi? = null
  private var destinationApi: DestinationApi? = null
  private var resumableFullRefreshStatsHelper: ResumableFullRefreshStatsHelper? = null
  private var backfillHelper: BackfillHelper? = null
  private var catalogClientConverters: CatalogClientConverters? = null
  private var metricClient: MetricClient? = null

  @BeforeEach
  @Throws(IOException::class)
  fun setup() {
    mapperSecretHydrationHelper = Mockito.mock<MapperSecretHydrationHelper>(MapperSecretHydrationHelper::class.java)
    connectorSecretsHydrator = Mockito.mock<ConnectorSecretsHydrator>(ConnectorSecretsHydrator::class.java)
    airbyteApiClient = Mockito.mock<AirbyteApiClient>(AirbyteApiClient::class.java)
    attemptApi = Mockito.mock<AttemptApi?>(AttemptApi::class.java)
    connectionApi = Mockito.mock<ConnectionApi>(ConnectionApi::class.java)
    stateApi = Mockito.mock<StateApi>(StateApi::class.java)
    jobsApi = Mockito.mock<JobsApi>(JobsApi::class.java)
    secretsPersistenceConfigApi = Mockito.mock<SecretsPersistenceConfigApi?>(SecretsPersistenceConfigApi::class.java)
    actorDefinitionVersionApi = Mockito.mock<ActorDefinitionVersionApi?>(ActorDefinitionVersionApi::class.java)
    destinationApi = Mockito.mock<DestinationApi>(DestinationApi::class.java)
    resumableFullRefreshStatsHelper = Mockito.mock(ResumableFullRefreshStatsHelper::class.java)
    catalogClientConverters = CatalogClientConverters(FieldGenerator())
    backfillHelper = BackfillHelper(catalogClientConverters!!)
    metricClient = Mockito.mock(MetricClient::class.java)
    Mockito.`when`<String?>(destinationApi!!.baseUrl).thenReturn("http://localhost:8001/api")
    Mockito.`when`<DestinationRead?>(destinationApi!!.getDestination(any<DestinationIdRequestBody>())).thenReturn(
      DESTINATION_READ,
    )
    Mockito.`when`<AttemptApi?>(airbyteApiClient.attemptApi).thenReturn(attemptApi)
    Mockito.`when`<ConnectionApi?>(airbyteApiClient.connectionApi).thenReturn(connectionApi)
    Mockito.`when`<DestinationApi?>(airbyteApiClient.destinationApi).thenReturn(destinationApi)
    Mockito.`when`<StateApi?>(airbyteApiClient.stateApi).thenReturn(stateApi)
    Mockito.`when`<JobsApi?>(airbyteApiClient.jobsApi).thenReturn(jobsApi)
    Mockito.`when`<SecretsPersistenceConfigApi?>(airbyteApiClient.secretPersistenceConfigApi).thenReturn(secretsPersistenceConfigApi)
    Mockito.`when`<ActorDefinitionVersionApi?>(airbyteApiClient.actorDefinitionVersionApi).thenReturn(actorDefinitionVersionApi)
    Mockito.`when`<DestinationApi?>(airbyteApiClient.destinationApi).thenReturn(destinationApi)
    Mockito.`when`<ConnectionState?>(stateApi.getState(ConnectionIdRequestBody(CONNECTION_ID))).thenReturn(CONNECTION_STATE_RESPONSE)
    Mockito
      .`when`<ConfiguredAirbyteCatalog?>(
        mapperSecretHydrationHelper.hydrateMapperSecrets(
          any<ConfiguredAirbyteCatalog>(),
          any<Boolean>(),
          any<UUID>(),
        ),
      ).thenAnswer(
        Answer { invocation: InvocationOnMock? -> invocation!!.getArgument<Any?>(0) },
      )
  }

  private val replicationInputHydrator: ReplicationInputHydrator
    get() =
      ReplicationInputHydrator(
        airbyteApiClient,
        resumableFullRefreshStatsHelper!!,
        mapperSecretHydrationHelper,
        backfillHelper!!,
        catalogClientConverters!!,
        ReplicationInputMapper(),
        metricClient!!,
        connectorSecretsHydrator,
        USE_RUNTIME_PERSISTENCE,
      )

  private fun getDefaultReplicationActivityInputForTest(supportsRefresh: Boolean): ReplicationActivityInput =
    ReplicationActivityInput(
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
      ConnectionContext().withWorkspaceId(UUID.randomUUID()).withOrganizationId(UUID.randomUUID()),
      null,
      mutableListOf<String>(),
      false,
      false,
      Map.of<String, Any?>(),
      null,
      supportsRefresh,
      null,
      null,
    )

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(Exception::class)
  fun testGenerateReplicationInputRetrievesInputs(withRefresh: Boolean) {
    if (withRefresh) {
      mockRefresh()
    } else {
      mockNonRefresh()
    }
    // Verify that we get the state and catalog from the API.
    val replicationInputHydrator = this.replicationInputHydrator

    val replicationActivityInput = getDefaultReplicationActivityInputForTest(withRefresh)
    val replicationInput = replicationInputHydrator.getHydratedReplicationInput(replicationActivityInput)
    Assert.assertEquals(EXPECTED_STATE, replicationInput.getState())
    Assert.assertEquals(
      1,
      replicationInput
        .getCatalog()
        .streams.size
        .toLong(),
    )
    Assert.assertEquals(
      TEST_STREAM_NAME,
      replicationInput
        .getCatalog()
        .streams
        .get(0)
        .stream.name,
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(Exception::class)
  fun testGenerateReplicationInputHandlesResets(withRefresh: Boolean) {
    if (withRefresh) {
      mockRefresh()
    } else {
      mockNonRefresh()
    }
    // Verify that if the sync is a reset, we retrieve the job info and handle the streams accordingly.
    val replicationInputHydrator = this.replicationInputHydrator
    val input = getDefaultReplicationActivityInputForTest(withRefresh)
    input.isReset = true
    Mockito.`when`<JobOptionalRead?>(jobsApi.getLastReplicationJob(ConnectionIdRequestBody(CONNECTION_ID))).thenReturn(
      JobOptionalRead(
        JobRead(
          JOB_ID,
          JobConfigType.SYNC,
          CONNECTION_ID.toString(),
          System.currentTimeMillis(),
          System.currentTimeMillis(),
          JobStatus.CANCELLED,
          null,
          null,
          ResetConfig(List.of<StreamDescriptor>(StreamDescriptor(TEST_STREAM_NAME, TEST_STREAM_NAMESPACE))),
          null,
          null,
          null,
        ),
      ),
    )
    val replicationInput = replicationInputHydrator.getHydratedReplicationInput(input)
    Assert.assertEquals(
      1,
      replicationInput
        .getCatalog()
        .streams.size
        .toLong(),
    )
    Assert.assertEquals(SyncMode.FULL_REFRESH, replicationInput.getCatalog().streams[0].syncMode)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  @Throws(Exception::class)
  fun testGenerateReplicationInputHandlesBackfills(withRefresh: Boolean) {
    if (withRefresh) {
      mockRefresh()
    } else {
      mockNonRefresh()
    }
    // Verify that if backfill is enabled, and we have an appropriate diff, then we clear the state for
    // the affected streams.
    mockEnableBackfillForConnection(withRefresh)
    val replicationInputHydrator = this.replicationInputHydrator
    val input = getDefaultReplicationActivityInputForTest(withRefresh)

    input.schemaRefreshOutput = RefreshSchemaActivityOutput(toDomain(CATALOG_DIFF))
    val replicationInput = replicationInputHydrator.getHydratedReplicationInput(input)
    val typedState: Optional<StateWrapper> = getTypedState(replicationInput.getState().getState())
    Assert.assertEquals(
      JsonNodeFactory.instance.nullNode(),
      typedState
        .get()
        .getStateMessages()
        .get(0)
        .getStream()
        .getStreamState(),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testTrackBackfillAndResume() {
    val replicationInputHydrator = this.replicationInputHydrator
    val stream1 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
        .withNamespace("ns1")
    val stream2 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
    val stream3 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
        .withNamespace("ns2")
    val stream4 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s2")
    replicationInputHydrator.trackBackfillAndResume(
      1L,
      2L,
      List.of<io.airbyte.config.StreamDescriptor?>(stream1, stream2, stream4),
      List.of<io.airbyte.config.StreamDescriptor?>(stream1, stream3, stream4),
    )

    val expectedRequest =
      SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        List.of<StreamAttemptMetadata>(
          StreamAttemptMetadata("s1", true, true, "ns1"),
          StreamAttemptMetadata("s1", false, true, null),
          StreamAttemptMetadata("s1", true, false, "ns2"),
          StreamAttemptMetadata("s2", true, true, null),
        ),
      )

    val captor =
      ArgumentCaptor.forClass<SaveStreamAttemptMetadataRequestBody?, SaveStreamAttemptMetadataRequestBody?>(
        SaveStreamAttemptMetadataRequestBody::class.java,
      )
    Mockito.verify<AttemptApi?>(attemptApi).saveStreamMetadata(captor.capture())
    Assert.assertEquals(expectedRequest.jobId, captor.getValue()!!.jobId)
    Assert.assertEquals(expectedRequest.attemptNumber.toLong(), captor.getValue()!!.attemptNumber.toLong())
    CollectionAssert
      .assertThatCollection<StreamAttemptMetadata>(captor.getValue()!!.streamMetadata)
      .containsExactlyInAnyOrderElementsOf(expectedRequest.streamMetadata)
  }

  @Test
  @Throws(IOException::class)
  fun testTrackBackfillAndResumeWithoutBackfill() {
    val replicationInputHydrator = this.replicationInputHydrator
    val stream1 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
        .withNamespace("ns1")
    val stream2 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
    val stream4 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s2")
    replicationInputHydrator.trackBackfillAndResume(
      1L,
      2L,
      List.of<io.airbyte.config.StreamDescriptor?>(stream1, stream2, stream4),
      null,
    )

    val expectedRequest =
      SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        List.of<StreamAttemptMetadata>(
          StreamAttemptMetadata("s1", false, true, "ns1"),
          StreamAttemptMetadata("s1", false, true, null),
          StreamAttemptMetadata("s2", false, true, null),
        ),
      )

    val captor =
      ArgumentCaptor.forClass<SaveStreamAttemptMetadataRequestBody?, SaveStreamAttemptMetadataRequestBody?>(
        SaveStreamAttemptMetadataRequestBody::class.java,
      )
    Mockito.verify<AttemptApi?>(attemptApi).saveStreamMetadata(captor.capture())
    Assert.assertEquals(expectedRequest.jobId, captor.getValue()!!.jobId)
    Assert.assertEquals(expectedRequest.attemptNumber.toLong(), captor.getValue()!!.attemptNumber.toLong())
    CollectionAssert
      .assertThatCollection<StreamAttemptMetadata>(captor.getValue()!!.streamMetadata)
      .containsExactlyInAnyOrderElementsOf(expectedRequest.streamMetadata)
  }

  @Test
  @Throws(IOException::class)
  fun testTrackBackfillAndResumeWithoutResume() {
    val replicationInputHydrator = this.replicationInputHydrator
    val stream1 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
        .withNamespace("ns1")
    val stream3 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s1")
        .withNamespace("ns2")
    val stream4 =
      io.airbyte.config
        .StreamDescriptor()
        .withName("s2")
    replicationInputHydrator.trackBackfillAndResume(
      1L,
      2L,
      null,
      List.of<io.airbyte.config.StreamDescriptor?>(stream1, stream3, stream4),
    )

    val expectedRequest =
      SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        List.of<StreamAttemptMetadata>(
          StreamAttemptMetadata("s1", true, false, "ns1"),
          StreamAttemptMetadata("s1", true, false, "ns2"),
          StreamAttemptMetadata("s2", true, false, null),
        ),
      )

    val captor =
      ArgumentCaptor.forClass<SaveStreamAttemptMetadataRequestBody?, SaveStreamAttemptMetadataRequestBody?>(
        SaveStreamAttemptMetadataRequestBody::class.java,
      )
    Mockito.verify<AttemptApi?>(attemptApi).saveStreamMetadata(captor.capture())
    Assert.assertEquals(expectedRequest.jobId, captor.getValue()!!.jobId)
    Assert.assertEquals(expectedRequest.attemptNumber.toLong(), captor.getValue()!!.attemptNumber.toLong())
    CollectionAssert
      .assertThatCollection<StreamAttemptMetadata>(captor.getValue()!!.streamMetadata)
      .containsExactlyInAnyOrderElementsOf(expectedRequest.streamMetadata)
  }

  @Throws(IOException::class)
  private fun mockEnableBackfillForConnection(withRefresh: Boolean) {
    if (withRefresh) {
      Mockito
        .`when`<ConnectionRead?>(connectionApi.getConnectionForJob(ConnectionAndJobIdRequestBody(CONNECTION_ID, JOB_ID)))
        .thenReturn(
          ConnectionRead(
            CONNECTION_ID,
            CONNECTION_NAME,
            SOURCE_ID,
            DESTINATION_ID,
            SYNC_CATALOG,
            ConnectionStatus.ACTIVE,
            false,
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
            SchemaChangeBackfillPreference.ENABLED,
            null,
            null,
            null,
          ),
        )
    } else {
      Mockito
        .`when`<ConnectionRead?>(connectionApi.getConnection(ConnectionIdRequestBody(CONNECTION_ID)))
        .thenReturn(
          ConnectionRead(
            CONNECTION_ID,
            CONNECTION_NAME,
            SOURCE_ID,
            DESTINATION_ID,
            SYNC_CATALOG,
            ConnectionStatus.ACTIVE,
            false,
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
            SchemaChangeBackfillPreference.ENABLED,
            null,
            null,
            null,
          ),
        )
    }
  }

  @Throws(IOException::class)
  private fun mockRefresh() {
    Mockito
      .`when`<ConnectionRead?>(connectionApi.getConnectionForJob(ConnectionAndJobIdRequestBody(CONNECTION_ID, JOB_ID)))
      .thenReturn(
        ConnectionRead(
          CONNECTION_ID,
          CONNECTION_NAME,
          SOURCE_ID,
          DESTINATION_ID,
          SYNC_CATALOG,
          ConnectionStatus.ACTIVE,
          false,
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
          null,
          null,
          null,
          null,
        ),
      )
  }

  @Throws(IOException::class)
  private fun mockNonRefresh() {
    Mockito
      .`when`<ConnectionRead?>(connectionApi.getConnection(ConnectionIdRequestBody(CONNECTION_ID)))
      .thenReturn(
        ConnectionRead(
          CONNECTION_ID,
          CONNECTION_NAME,
          SOURCE_ID,
          DESTINATION_ID,
          SYNC_CATALOG,
          ConnectionStatus.ACTIVE,
          false,
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
          null,
          null,
          null,
          null,
        ),
      )
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DESTINATION_ID: UUID = UUID.randomUUID()
    private val SOURCE_CONFIG: JsonNode? = JsonNodeFactory.instance.objectNode()
    private val DESTINATION_CONFIG: JsonNode? = JsonNodeFactory.instance.objectNode()
    private const val CONNECTION_NAME = "connection-name"

    private const val TEST_STREAM_NAME = "test-stream-name"
    private const val TEST_STREAM_NAMESPACE = "test-stream-namespace"
    private val SYNC_CATALOG =
      AirbyteCatalog(
        List.of<AirbyteStreamAndConfiguration>(
          AirbyteStreamAndConfiguration(
            AirbyteStream(
              TEST_STREAM_NAME,
              emptyObject(),
              List.of<io.airbyte.api.client.model.generated.SyncMode?>(io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL),
              null,
              null,
              null,
              TEST_STREAM_NAMESPACE,
              null,
              null,
            ),
            AirbyteStreamConfiguration(
              io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL,
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
              null,
            ),
          ),
        ),
      )
    private val CONNECTION_STATE_RESPONSE: ConnectionState =
      Jsons.deserialize<ConnectionState>(
        String.format(
          """
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
          
          """.trimIndent(),
          CONNECTION_ID,
          TEST_STREAM_NAME,
          TEST_STREAM_NAMESPACE,
          TEST_STREAM_NAME,
          TEST_STREAM_NAMESPACE,
        ),
        ConnectionState::class.java,
      )
    private val EXPECTED_STATE: State? =
      State().withState(
        Jsons.deserialize(
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
          
          """.trimIndent(),
        ),
      )
    private const val JOB_ID = 123L
    private const val ATTEMPT_NUMBER = 2L
    private val JOB_RUN_CONFIG: JobRunConfig? = JobRunConfig().withJobId(JOB_ID.toString()).withAttemptId(ATTEMPT_NUMBER)
    private val DESTINATION_LAUNCHER_CONFIG: IntegrationLauncherConfig? = IntegrationLauncherConfig().withDockerImage("dockerimage:dockertag")
    private val SOURCE_LAUNCHER_CONFIG = IntegrationLauncherConfig()
    private val SYNC_RESOURCE_REQUIREMENTS = SyncResourceRequirements()
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private const val USE_RUNTIME_PERSISTENCE = false
    private val CATALOG_DIFF =
      CatalogDiff(
        List.of<StreamTransform>(
          StreamTransform(
            StreamTransform.TransformType.UPDATE_STREAM,
            StreamDescriptor(
              SYNC_CATALOG.streams[0].stream!!.name,
              SYNC_CATALOG.streams[0].stream!!.namespace,
            ),
            StreamTransformUpdateStream(
              List.of<FieldTransform>(
                FieldTransform(
                  FieldTransform.TransformType.ADD_FIELD,
                  mutableListOf<String>(),
                  false,
                  null,
                  null,
                  null,
                ),
              ),
              mutableListOf<StreamAttributeTransform>(),
            ),
          ),
        ),
      )
    private val DESTINATION_READ =
      DestinationRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        emptyObject(),
        "name",
        "destinationName",
        1L,
        "icon",
        false,
        true,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )

    private lateinit var mapperSecretHydrationHelper: MapperSecretHydrationHelper
    private lateinit var connectorSecretsHydrator: ConnectorSecretsHydrator
    private lateinit var airbyteApiClient: AirbyteApiClient
    private lateinit var connectionApi: ConnectionApi
    private lateinit var stateApi: StateApi
    private lateinit var jobsApi: JobsApi
  }
}
