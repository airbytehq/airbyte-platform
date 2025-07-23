/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.InternalOperationResult
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.MdcScope
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConnectionContext
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.MapperConfig
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.State
import io.airbyte.config.SyncMode
import io.airbyte.config.WorkerDestinationConfig
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageTracker
import io.airbyte.container.orchestrator.bookkeeping.ParallelStreamStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.StateCheckSumCountEventHandler
import io.airbyte.container.orchestrator.bookkeeping.SyncStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.airbyte.container.orchestrator.bookkeeping.state.DefaultStateAggregator
import io.airbyte.container.orchestrator.bookkeeping.state.SingleStateAggregator
import io.airbyte.container.orchestrator.bookkeeping.state.StreamStateAggregator
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusTracker
import io.airbyte.container.orchestrator.config.CommonBeanFactory
import io.airbyte.container.orchestrator.config.OrchestratorBeanFactory
import io.airbyte.container.orchestrator.persistence.SyncPersistenceImpl
import io.airbyte.container.orchestrator.tracker.AnalyticsMessageTracker
import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.tracker.ThreadedTimeTracker
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.filter.FieldSelector
import io.airbyte.container.orchestrator.worker.fixtures.EmptyAirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.io.EmptyAirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.container.orchestrator.worker.util.ReplicationMetricReporter
import io.airbyte.mappers.application.RecordMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.mappers.transformations.Mapper
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.internal.AirbyteMapper
import io.airbyte.workers.internal.NamespacingMapper
import io.airbyte.workers.models.ArchitectureConstants
import io.mockk.Runs
import io.mockk.coEvery
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

/**
 * This class is mostly just a demo of [ReplicationWorkerIntegrationTestUtil].
 */
class ReplicationWorkerIntegrationTest {
  @Test
  fun emptySource() =
    runTest {
      val catalog =
        ConfiguredAirbyteCatalog().withStreams(
          listOf(
            ConfiguredAirbyteStream(
              stream =
                AirbyteStream(
                  name = "test_stream",
                  jsonSchema = Jsons.deserialize("""{"properties": {}}"""),
                  supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
                ),
            ),
          ),
        )
      val source = EmptyAirbyteSource(hasCustomNamespace = true)
      val destination = CapturingDestination()

      val replicationWorkerInfo =
        ReplicationWorkerIntegrationTestUtil.runSync(
          catalog,
          source,
          destination,
          // set a custom namespace + stream name prefix
          airbyteMapper = NamespacingMapper(JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT, "foo_namespace", "foo_prefix_"),
        )

      assertEquals(listOf(AirbyteMessage.Type.STATE), destination.getMessages().map { it.type })
      // Destination should receive message with mapped stream name+namespace
      assertEquals(
        StreamDescriptor().withName("foo_prefix_test_stream").withNamespace("foo_namespace"),
        destination
          .getMessages()
          .first()
          .state.stream.streamDescriptor,
      )
      // Stats should be tracked internally using the source stream name+namespace
      assertEquals(
        0,
        replicationWorkerInfo.syncStatsTracker.getStats()[AirbyteStreamNameNamespacePair("test_stream", null)]!!.bytesEmitted,
      )
    }

  /**
   * Mostly identical to [emptySource], but uses a [StaticMessageListSource].
   * This allows you to specify an exact list of messages to send to the orchestrator.
   */
  @Test
  fun staticSource() =
    runTest {
      val catalog =
        ConfiguredAirbyteCatalog().withStreams(
          listOf(
            ConfiguredAirbyteStream(
              stream =
                AirbyteStream(
                  name = "test_stream",
                  jsonSchema =
                    Jsons.deserialize(
                      """
                      {
                        "properties": {
                          "id": {"type": "integer"}
                        }
                      }
                      """.trimIndent(),
                    ),
                  supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
                ),
            ),
          ),
        )
      val source =
        StaticMessageListSource(
          sequenceOf(
            AirbyteMessage()
              .withType(AirbyteMessage.Type.RECORD)
              .withRecord(
                AirbyteRecordMessage()
                  .withStream("test_stream")
                  .withEmittedAt(1234)
                  .withData(Jsons.deserialize("""{"id": 42, "undeclared": "blah"}""")),
              ),
            AirbyteMessage()
              .withType(AirbyteMessage.Type.STATE)
              .withState(
                AirbyteStateMessage()
                  .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                  .withStream(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("test_stream"))
                      .withStreamState(Jsons.deserialize("""{"cursor": "whatever"}""")),
                  ),
              ),
          ),
        )
      val destination = CapturingDestination()

      val replicationWorkerInfo = ReplicationWorkerIntegrationTestUtil.runSync(catalog, source, destination)

      assertEquals(
        listOf(AirbyteMessage.Type.RECORD, AirbyteMessage.Type.STATE),
        destination.getMessages().map { it.type },
      )
      assertEquals(
        // the undeclared field is filtered out, and we minify the json
        // so we end up emitting {"id":42}, which is 9 bytes long
        9,
        replicationWorkerInfo.syncStatsTracker.getStats()[AirbyteStreamNameNamespacePair("test_stream", null)]!!.bytesEmitted,
      )
    }
}

/**
 * Utility class for running a source->orchestrator->destination sync, without actually needing to
 * spin up a full platform instance + configure a real source/destination. See [ReplicationWorkerIntegrationTest]
 * for example usages. TL;DR call [runSync], probably using a [StaticMessageListSource] and [CapturingDestination].
 */
object ReplicationWorkerIntegrationTestUtil {
  suspend fun runSync(
    catalog: ConfiguredAirbyteCatalog,
    source: AirbyteSource,
    destination: AirbyteDestination,
    featureFlags: Map<String, Any?> = emptyMap(),
    mappers: List<Mapper<out MapperConfig>> = emptyList(),
    // Default to a noop mapper
    airbyteMapper: AirbyteMapper =
      NamespacingMapper(
        JobSyncConfig.NamespaceDefinitionType.SOURCE,
        namespaceFormat = null,
        streamPrefix = null,
      ),
  ): ReplicationWorkerInfo {
    val jobRoot = getJobRoot()
    val replicationWorkerDispatcher = Executors.newSingleThreadExecutor()
    val stateFlushExecutorService = Executors.newSingleThreadScheduledExecutor()

    val replicationWorkerInfo =
      try {
        getReplicationWorkerInfo(
          source,
          catalog,
          replicationWorkerDispatcher = replicationWorkerDispatcher,
          stateFlushExecutorService = stateFlushExecutorService,
          jobRoot,
          destination = destination,
          featureFlags,
          mappers,
          airbyteMapper,
        ).also {
          it.replicationWorker.run(jobRoot)
        }
      } finally {
        replicationWorkerDispatcher.shutdown()
        stateFlushExecutorService.shutdown()
      }

    // Return the worker info so the caller can inspect e.g. the sync stats
    return replicationWorkerInfo
  }

  private fun getJobRoot(): Path =
    Files
      .createTempDirectory("replication-worker-integration-test-jobRoot")
      .also { it.toFile().deleteOnExit() }

  // TODO some things probably should be smarter mocks (e.g. the AirbyteApi instance)
  //   so that it's easier to verify we did the right things.
  private fun getReplicationWorkerInfo(
    source: AirbyteSource,
    catalog: ConfiguredAirbyteCatalog,
    replicationWorkerDispatcher: ExecutorService,
    stateFlushExecutorService: ScheduledExecutorService,
    jobRoot: Path,
    destination: AirbyteDestination,
    featureFlags: Map<String, Any?>,
    mappers: List<Mapper<out MapperConfig>>,
    airbyteMapper: AirbyteMapper,
  ): ReplicationWorkerInfo {
    val bufferConfiguration = BufferConfiguration()
    val streamStatusCompletionTracker = StreamStatusCompletionTracker(Clock.systemUTC())
    val replicationWorkerState = ReplicationWorkerState()
    val connectionId = UUID.randomUUID()
    val replicationInput =
      ReplicationInput()
        .withWorkspaceId(UUID.randomUUID())
        .withConnectionId(connectionId)
        .withSourceId(UUID.randomUUID())
        .withDestinationId(UUID.randomUUID())
        .withConnectionContext(
          ConnectionContext()
            .withSourceDefinitionId(UUID.randomUUID())
            .withDestinationDefinitionId(UUID.randomUUID()),
        ).withCatalog(catalog)
        .withSourceLauncherConfig(IntegrationLauncherConfig().withDockerImage("fake/source-image:with-version"))
        .withDestinationLauncherConfig(IntegrationLauncherConfig().withDockerImage("fake/destination-image:with-version"))
        .withFeatureFlags(featureFlags)
        .apply {
          if (source is EmptyAirbyteSource) {
            // The empty airbyte source needs some extra stuff to behave usefully.
            // TODO eventually we should probably accept these as function params,
            //   but for now let's just hardcode it
            sourceConfiguration = Jsons.jsonNode(ResetSourceConfiguration().withStreamsToReset(catalog.streams.map { it.streamDescriptor }))
            state =
              State().withState(
                Jsons.jsonNode(
                  catalog.streams.map {
                    AirbyteStateMessage()
                      .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                      .withStream(
                        AirbyteStreamState()
                          .withStreamDescriptor(it.streamDescriptor.toProtocol())
                          .withStreamState(Jsons.deserialize("""{"foo": "bar"}""")),
                      )
                  },
                ),
              )
          }
        }
    val jobId = 1234L
    val attempt = 5678
    // TODO in principle we might want to capture the metrics / state checksum / attemptApi calls
    //   but for now, meh
    val metricClient = mockk<MetricClient>(relaxed = true)
    val platformMode = ArchitectureConstants.ORCHESTRATOR
    val syncStatsTracker =
      ParallelStreamStatsTracker(
        metricClient = metricClient,
        stateCheckSumEventHandler =
          mockk<StateCheckSumCountEventHandler> {
            every { validateStateChecksum(any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
            every { close(any()) } just Runs
          },
        platformMode = platformMode,
      )
    val syncPersistence =
      SyncPersistenceImpl(
        airbyteApiClient =
          mockk<AirbyteApiClient> {
            every { attemptApi.saveStats(any()) } returns InternalOperationResult(succeeded = true)
          },
        stateBuffer = DefaultStateAggregator(StreamStateAggregator(), SingleStateAggregator()),
        stateFlushExecutorService = stateFlushExecutorService,
        stateFlushPeriodInSeconds = 1,
        metricClient = metricClient,
        syncStatsTracker = syncStatsTracker,
        connectionId = connectionId,
        jobId = jobId,
        attemptNumber = attempt,
      )
    val replicationInputFeatureFlagReader = ReplicationInputFeatureFlagReader(replicationInput)
    val replicationWorkerHelper =
      ReplicationWorkerHelper(
        fieldSelector =
          FieldSelector(
            mockk<RecordSchemaValidator> {
              every { validateSchema(any(), any(), any()) } just Runs
            },
            mockk<ReplicationMetricReporter>(),
            replicationInput,
            replicationInputFeatureFlagReader,
          ),
        mapper = airbyteMapper,
        AirbyteMessageTracker(
          replicationInputFeatureFlagReader,
          replicationInput,
          syncPersistence,
          platformMode = platformMode,
        ),
        mockk<ReplicationAirbyteMessageEventPublishingHelper>(),
        ThreadedTimeTracker(),
        mockk<AnalyticsMessageTracker> {
          every { ctx = any() } just Runs
          every { flush() } just Runs
        },
        streamStatusCompletionTracker,
        syncStatsTracker,
        mockk<StreamStatusTracker> {
          every { track(any()) } just Runs
        },
        RecordMapper(mappers),
        replicationWorkerState,
        ReplicationContextProvider(attempt = attempt, jobId = jobId).provideContext(replicationInput),
        DestinationCatalogGenerator(mappers = mappers, MoreMappers.initMapper()),
        metricClient,
      )
    val replicationWorkerContext =
      ReplicationWorkerContext(
        jobId = jobId,
        attempt = attempt,
        bufferConfiguration,
        replicationWorkerHelper,
        replicationWorkerState,
        streamStatusCompletionTracker,
      )
    val startReplicationJobs =
      CommonBeanFactory().startReplicationJobs(
        destination,
        jobRoot,
        replicationInput,
        replicationWorkerContext,
        source,
      )
    val syncReplicationJobs =
      OrchestratorBeanFactory().syncReplicationJobs(
        destination,
        ClosableChannelQueue(bufferConfiguration.destinationMaxBufferSize),
        replicationWorkerHelper,
        replicationWorkerState,
        source,
        ClosableChannelQueue(bufferConfiguration.sourceMaxBufferSize),
        streamStatusCompletionTracker,
      )
    return ReplicationWorkerInfo(
      ReplicationWorker(
        source,
        destination,
        syncPersistence,
        onReplicationRunning = {},
        mockk<WorkloadHeartbeatSender> {
          coEvery { sendHeartbeat() } just Runs
        },
        recordSchemaValidator = null,
        replicationWorkerContext,
        startReplicationJobs = startReplicationJobs,
        syncReplicationJobs = syncReplicationJobs,
        replicationWorkerDispatcher,
        MdcScope.Builder(),
      ),
      syncStatsTracker,
    )
  }

  // Simple wrapper for useful things.
  // Probably eventually has more stuff added to it, depending on what we need to investigate.
  data class ReplicationWorkerInfo(
    val replicationWorker: ReplicationWorker,
    val syncStatsTracker: SyncStatsTracker,
  )
}

/**
 * Simple [AirbyteSource] implementation that just takes a hardcoded sequence of messages,
 * and emits those messages.
 *
 * You can use `StaticMessageListSource(sequence { yield(...); ... })` to programmatically generate
 * the messages.
 */
class StaticMessageListSource(
  messages: Sequence<AirbyteMessage>,
) : AirbyteSource {
  private val messageIterator = messages.iterator()

  override fun start(
    sourceConfig: WorkerSourceConfig,
    jobRoot: Path?,
    connectionId: UUID?,
  ) {
  }

  override val isFinished: Boolean
    get() = !messageIterator.hasNext()
  override val exitValue = 0

  override fun attemptRead(): Optional<AirbyteMessage> =
    if (messageIterator.hasNext()) {
      Optional.of(messageIterator.next())
    } else {
      Optional.empty()
    }

  override fun close() {
  }

  override fun cancel() {
  }
}

/**
 * Basically the same as [EmptyAirbyteDestination], but keeps a list of messages that it received.
 */
class CapturingDestination : AirbyteDestination {
  // Capture messages as JSON.
  // This helps to investigate where a single message object (or subobject)
  // is shared between calls, and then mutated in weird ways.
  private val messagesReceived: MutableList<String> = mutableListOf()
  private var finished = false

  override fun start(
    destinationConfig: WorkerDestinationConfig,
    jobRoot: Path,
  ) {
  }

  override fun accept(message: AirbyteMessage) {
    check(!finished)
    messagesReceived.add(Jsons.serialize(message))
  }

  override fun notifyEndOfInput() {
    finished = true
  }

  override val isFinished: Boolean
    get() = finished
  override val exitValue = 0

  override fun attemptRead(): Optional<AirbyteMessage> = Optional.empty()

  override fun close() {
    finished = true
  }

  override fun cancel() {
    finished = true
  }

  fun getMessages(): List<AirbyteMessage> = messagesReceived.map { Jsons.deserialize(it, AirbyteMessage::class.java) }
}
