/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.airbyte.commons.version.Version
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.State
import io.airbyte.config.StateWrapper
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.WorkloadPriority
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
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.CollectionAssert
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional
import java.util.UUID

/**
 * Tests for the replication activity specifically.
 */
internal class ReplicationInputHydratorTest {
  private lateinit var secretsPersistenceConfigApi: SecretsPersistenceConfigApi
  private lateinit var actorDefinitionVersionApi: ActorDefinitionVersionApi
  private lateinit var attemptApi: AttemptApi
  private lateinit var destinationApi: DestinationApi
  private lateinit var resumableFullRefreshStatsHelper: ResumableFullRefreshStatsHelper
  private lateinit var backfillHelper: BackfillHelper
  private lateinit var catalogClientConverters: CatalogClientConverters
  private lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    mapperSecretHydrationHelper = mockk(relaxed = true)
    connectorSecretsHydrator = mockk(relaxed = true)
    airbyteApiClient = mockk(relaxed = true)
    attemptApi = mockk(relaxed = true)
    connectionApi = mockk(relaxed = true)
    stateApi = mockk(relaxed = true)
    jobsApi = mockk(relaxed = true)
    secretsPersistenceConfigApi = mockk(relaxed = true)
    actorDefinitionVersionApi = mockk(relaxed = true)
    destinationApi = mockk(relaxed = true)
    resumableFullRefreshStatsHelper = mockk(relaxed = true)
    catalogClientConverters = CatalogClientConverters(FieldGenerator())
    backfillHelper = BackfillHelper(catalogClientConverters)
    metricClient = mockk(relaxed = true)
    every { destinationApi.baseUrl } returns "http://localhost:8001/api"
    every { destinationApi.getDestination(any()) } returns DESTINATION_READ
    every { airbyteApiClient.attemptApi } returns attemptApi
    every { airbyteApiClient.connectionApi } returns connectionApi
    every { airbyteApiClient.destinationApi } returns destinationApi
    every { airbyteApiClient.stateApi } returns stateApi
    every { airbyteApiClient.jobsApi } returns jobsApi
    every { airbyteApiClient.secretPersistenceConfigApi } returns secretsPersistenceConfigApi
    every { airbyteApiClient.actorDefinitionVersionApi } returns actorDefinitionVersionApi
    every { airbyteApiClient.destinationApi } returns destinationApi
    every { stateApi.getState(ConnectionIdRequestBody(CONNECTION_ID)) } returns CONNECTION_STATE_RESPONSE
    every {
      mapperSecretHydrationHelper.hydrateMapperSecrets(
        any<ConfiguredAirbyteCatalog>(),
        any<Boolean>(),
        any<UUID>(),
      )
    } answers { firstArg() }
  }

  private val replicationInputHydrator: ReplicationInputHydrator
    get() =
      ReplicationInputHydrator(
        airbyteApiClient,
        resumableFullRefreshStatsHelper,
        mapperSecretHydrationHelper,
        backfillHelper,
        catalogClientConverters,
        ReplicationInputMapper(),
        metricClient,
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
      listOf(),
      includesFiles = false,
      omitFileTransferEnvVar = false,
      featureFlags = emptyMap(),
      heartbeatMaxSecondsBetweenMessages = null,
      supportsRefreshes = supportsRefresh,
      sourceIPCOptions = null,
      destinationIPCOptions = null,
    )

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGenerateReplicationInputRetrievesInputs(withRefresh: Boolean) {
    every { jobsApi.getJobInput(any()) } returns
      mockk<JobInput> {
        every { destinationLauncherConfig } returns
          mockk<IntegrationLauncherConfig>(relaxed = true) {
            every { priority } returns WorkloadPriority.DEFAULT
            every { protocolVersion } returns Version("0.1.0")
          }
        every { sourceLauncherConfig } returns
          mockk<IntegrationLauncherConfig>(relaxed = true) {
            every { priority } returns WorkloadPriority.DEFAULT
            every { protocolVersion } returns Version("0.1.0")
          }
        every { syncInput } returns
          mockk<StandardSyncInput>(relaxed = true) {
            every { namespaceDefinition } returns JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT
            every { sourceConfiguration } returns Jsons.jsonNode(emptyMap<String, Any>())
            every { destinationConfiguration } returns Jsons.jsonNode(emptyMap<String, Any>())
          }
        every { jobRunConfig } returns mockk<JobRunConfig>(relaxed = true)
      }

    if (withRefresh) {
      mockRefresh()
    } else {
      mockNonRefresh()
    }
    // Verify that we get the state and catalog from the API.
    val replicationInputHydrator = this.replicationInputHydrator

    val replicationActivityInput = getDefaultReplicationActivityInputForTest(withRefresh)
    val replicationInput = replicationInputHydrator.getHydratedReplicationInput(replicationActivityInput)
    assertEquals(EXPECTED_STATE, replicationInput.state)
    assertEquals(
      1,
      replicationInput
        .catalog
        .streams.size
        .toLong(),
    )
    assertEquals(
      TEST_STREAM_NAME,
      replicationInput
        .catalog
        .streams[0]
        .stream.name,
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGenerateReplicationInputHandlesResets(withRefresh: Boolean) {
    every { jobsApi.getJobInput(any()) } returns
      mockk<JobInput> {
        every { destinationLauncherConfig } returns
          mockk<IntegrationLauncherConfig>(relaxed = true) {
            every { priority } returns WorkloadPriority.DEFAULT
            every { protocolVersion } returns Version("0.1.0")
          }
        every { sourceLauncherConfig } returns
          mockk<IntegrationLauncherConfig>(relaxed = true) {
            every { priority } returns WorkloadPriority.DEFAULT
            every { protocolVersion } returns Version("0.1.0")
          }
        every { syncInput } returns
          mockk<StandardSyncInput>(relaxed = true) {
            every { namespaceDefinition } returns JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT
            every { sourceConfiguration } returns Jsons.jsonNode(emptyMap<String, Any>())
            every { destinationConfiguration } returns Jsons.jsonNode(emptyMap<String, Any>())
          }
        every { jobRunConfig } returns mockk<JobRunConfig>(relaxed = true)
      }

    if (withRefresh) {
      mockRefresh()
    } else {
      mockNonRefresh()
    }
    // Verify that if the sync is a reset, we retrieve the job info and handle the streams accordingly.
    val replicationInputHydrator = this.replicationInputHydrator
    val input = getDefaultReplicationActivityInputForTest(withRefresh)
    input.isReset = true
    every { jobsApi.getLastReplicationJob(ConnectionIdRequestBody(CONNECTION_ID)) } returns
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
          ResetConfig(listOf(StreamDescriptor(TEST_STREAM_NAME, TEST_STREAM_NAMESPACE))),
          null,
          null,
          null,
        ),
      )
    val replicationInput = replicationInputHydrator.getHydratedReplicationInput(input)
    assertEquals(
      1,
      replicationInput
        .catalog
        .streams.size
        .toLong(),
    )
    assertEquals(SyncMode.FULL_REFRESH, replicationInput.catalog.streams[0].syncMode)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGenerateReplicationInputHandlesBackfills(withRefresh: Boolean) {
    every { jobsApi.getJobInput(any()) } returns
      mockk<JobInput> {
        every { destinationLauncherConfig } returns
          mockk<IntegrationLauncherConfig>(relaxed = true) {
            every { priority } returns WorkloadPriority.DEFAULT
            every { protocolVersion } returns Version("0.1.0")
          }
        every { sourceLauncherConfig } returns
          mockk<IntegrationLauncherConfig>(relaxed = true) {
            every { priority } returns WorkloadPriority.DEFAULT
            every { protocolVersion } returns Version("0.1.0")
          }
        every { syncInput } returns
          mockk<StandardSyncInput>(relaxed = true) {
            every { namespaceDefinition } returns JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT
            every { sourceConfiguration } returns Jsons.jsonNode(emptyMap<String, Any>())
            every { destinationConfiguration } returns Jsons.jsonNode(emptyMap<String, Any>())
          }
        every { jobRunConfig } returns mockk<JobRunConfig>(relaxed = true)
      }

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
    val typedState: Optional<StateWrapper> = getTypedState(replicationInput.state.state)
    assertEquals(
      JsonNodeFactory.instance.nullNode(),
      typedState
        .get()
        .stateMessages[0]
        .stream
        .streamState,
    )
  }

  @Test
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
      listOf(stream1, stream2, stream4),
      listOf(stream1, stream3, stream4),
    )

    val expectedRequest =
      SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        listOf(
          StreamAttemptMetadata("s1", wasBackfilled = true, wasResumed = true, streamNamespace = "ns1"),
          StreamAttemptMetadata("s1", wasBackfilled = false, wasResumed = true, streamNamespace = null),
          StreamAttemptMetadata("s1", wasBackfilled = true, wasResumed = false, streamNamespace = "ns2"),
          StreamAttemptMetadata("s2", true, wasResumed = true, streamNamespace = null),
        ),
      )

    val captor = slot<SaveStreamAttemptMetadataRequestBody>()
    verify { attemptApi.saveStreamMetadata(capture(captor)) }
    assertEquals(expectedRequest.jobId, captor.captured.jobId)
    assertEquals(expectedRequest.attemptNumber.toLong(), captor.captured.attemptNumber.toLong())
    CollectionAssert
      .assertThatCollection(captor.captured.streamMetadata)
      .containsExactlyInAnyOrderElementsOf(expectedRequest.streamMetadata)
  }

  @Test
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
      listOf(stream1, stream2, stream4),
      null,
    )

    val expectedRequest =
      SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        listOf(
          StreamAttemptMetadata("s1", wasBackfilled = false, wasResumed = true, streamNamespace = "ns1"),
          StreamAttemptMetadata("s1", wasBackfilled = false, wasResumed = true, streamNamespace = null),
          StreamAttemptMetadata("s2", wasBackfilled = false, wasResumed = true, streamNamespace = null),
        ),
      )

    val captor = slot<SaveStreamAttemptMetadataRequestBody>()
    verify { attemptApi.saveStreamMetadata(capture(captor)) }
    assertEquals(expectedRequest.jobId, captor.captured.jobId)
    assertEquals(expectedRequest.attemptNumber.toLong(), captor.captured.attemptNumber.toLong())
    CollectionAssert
      .assertThatCollection(captor.captured.streamMetadata)
      .containsExactlyInAnyOrderElementsOf(expectedRequest.streamMetadata)
  }

  @Test
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
      listOf(stream1, stream3, stream4),
    )

    val expectedRequest =
      SaveStreamAttemptMetadataRequestBody(
        1,
        2,
        listOf(
          StreamAttemptMetadata("s1", wasBackfilled = true, wasResumed = false, streamNamespace = "ns1"),
          StreamAttemptMetadata("s1", wasBackfilled = true, wasResumed = false, streamNamespace = "ns2"),
          StreamAttemptMetadata("s2", wasBackfilled = true, wasResumed = false, streamNamespace = null),
        ),
      )

    val captor = slot<SaveStreamAttemptMetadataRequestBody>()
    verify { attemptApi.saveStreamMetadata(capture(captor)) }
    assertEquals(expectedRequest.jobId, captor.captured.jobId)
    assertEquals(expectedRequest.attemptNumber.toLong(), captor.captured.attemptNumber.toLong())
    CollectionAssert
      .assertThatCollection(captor.captured.streamMetadata)
      .containsExactlyInAnyOrderElementsOf(expectedRequest.streamMetadata)
  }

  private fun mockEnableBackfillForConnection(withRefresh: Boolean) {
    if (withRefresh) {
      every {
        connectionApi.getConnectionForJob(ConnectionAndJobIdRequestBody(CONNECTION_ID, JOB_ID))
      } returns
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
          SchemaChangeBackfillPreference.ENABLED,
          null,
          null,
          null,
        )
    } else {
      every {
        connectionApi.getConnection(ConnectionIdRequestBody(CONNECTION_ID))
      } returns
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
          SchemaChangeBackfillPreference.ENABLED,
          null,
          null,
          null,
        )
    }
  }

  private fun mockRefresh() {
    every {
      connectionApi.getConnectionForJob(ConnectionAndJobIdRequestBody(CONNECTION_ID, JOB_ID))
    } returns
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
      )
  }

  private fun mockNonRefresh() {
    every {
      connectionApi.getConnection(ConnectionIdRequestBody(CONNECTION_ID))
    } returns
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
        listOf(
          AirbyteStreamAndConfiguration(
            AirbyteStream(
              TEST_STREAM_NAME,
              emptyObject(),
              listOf(io.airbyte.api.client.model.generated.SyncMode.INCREMENTAL),
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
        listOf(
          StreamTransform(
            StreamTransform.TransformType.UPDATE_STREAM,
            StreamDescriptor(
              SYNC_CATALOG.streams[0].stream!!.name,
              SYNC_CATALOG.streams[0].stream!!.namespace,
            ),
            StreamTransformUpdateStream(
              listOf(
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
        isVersionOverrideApplied = false,
        isEntitled = true,
        breakingChanges = null,
        supportState = null,
        status = null,
        resourceAllocation = null,
        numConnections = null,
        lastSync = null,
        connectionJobStatuses = null,
      )

    private lateinit var mapperSecretHydrationHelper: MapperSecretHydrationHelper
    private lateinit var connectorSecretsHydrator: ConnectorSecretsHydrator
    private lateinit var airbyteApiClient: AirbyteApiClient
    private lateinit var connectionApi: ConnectionApi
    private lateinit var stateApi: StateApi
    private lateinit var jobsApi: JobsApi
  }
}
