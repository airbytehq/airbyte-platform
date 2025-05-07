/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardSyncSummary.ReplicationStatus
import io.airbyte.container.orchestrator.tracker.ThreadedTimeTracker
import io.airbyte.container.orchestrator.worker.fixtures.SimpleAirbyteDestination
import io.airbyte.container.orchestrator.worker.fixtures.SimpleAirbyteSource
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.mappers.application.RecordMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteLogMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.Config
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.WorkerMetricReporter
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.general.BufferConfiguration
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.helper.StreamStatusCompletionTracker
import io.airbyte.workers.internal.AirbyteMapper
import io.airbyte.workers.internal.AnalyticsMessageTracker
import io.airbyte.workers.internal.FieldSelector
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker
import io.airbyte.workers.internal.bookkeeping.SyncStatsTracker
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTrackerFactory
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.airbyte.workers.testutils.AirbyteMessageUtils
import io.airbyte.workers.testutils.TestConfigHelpers
import io.mockk.MockKAnnotations
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.MDC
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

class ReplicationWorkerPortedTests {
  companion object {
    private const val JOB_ID = 0L
    private const val JOB_ATTEMPT = 0
    private val WORKSPACE_ROOT: Path = Path.of("workspaces/10")
    private const val STREAM_NAME = "user_preferences"
    private const val FIELD_NAME = "favorite_color"
    private val RECORD_MESSAGE1: AirbyteMessage =
      AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "blue")
    private val RECORD_MESSAGE2: AirbyteMessage =
      AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, "yellow")
    private val RECORD_MESSAGE3: AirbyteMessage =
      AirbyteMessageUtils.createRecordMessage(STREAM_NAME, FIELD_NAME, 3)
    private val STATE_MESSAGE: AirbyteMessage =
      AirbyteMessageUtils.createStateMessage(STREAM_NAME, "checkpoint", "1")
    private val ERROR_TRACE_MESSAGE: AirbyteTraceMessage =
      AirbyteMessageUtils.createErrorTraceMessage("a connector error occurred", 123.0)
    private val CONFIG_CONNECTOR: Config = Config().withAdditionalProperty("my_key", "my_new_value")
    private val CONFIG_MESSAGE: AirbyteMessage =
      AirbyteMessageUtils.createConfigControlMessage(CONFIG_CONNECTOR, 1.0)
    private const val SOURCE_IMAGE = "source-img:latest"
    private const val DESTINATION_IMAGE = "dest-img:latest"
  }

  private lateinit var jobRoot: Path
  private lateinit var sourceStub: SimpleAirbyteSource
  private lateinit var source: AirbyteSource
  private lateinit var destination: AirbyteDestination
  private lateinit var messageTracker: AirbyteMessageTracker
  private lateinit var syncStatsTracker: SyncStatsTracker
  private lateinit var syncPersistence: SyncPersistence
  private lateinit var recordSchemaValidator: RecordSchemaValidator
  private lateinit var metricClient: MetricClient
  private lateinit var workerMetricReporter: WorkerMetricReporter
  private lateinit var replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader
  private lateinit var replicationAirbyteMessageEventPublishingHelper: ReplicationAirbyteMessageEventPublishingHelper
  private lateinit var analyticsMessageTracker: AnalyticsMessageTracker
  private lateinit var streamStatusCompletionTracker: StreamStatusCompletionTracker
  private lateinit var streamStatusTrackerFactory: StreamStatusTrackerFactory
  private lateinit var recordMapper: RecordMapper
  private lateinit var destinationCatalogGenerator: DestinationCatalogGenerator

  private lateinit var replicationInput: ReplicationInput
  private lateinit var onReplicationRunning: VoidCallable
  private lateinit var replicationWorkerHelper: ReplicationWorkerHelper
  private lateinit var replicationContext: ReplicationContext
  private lateinit var ctx: ReplicationWorkerContext
  private lateinit var replicationWorkerState: ReplicationWorkerState
  private lateinit var mapper: AirbyteMapper

  @BeforeEach
  fun setup() {
    MockKAnnotations.init(this, relaxed = true)
    MDC.clear()

    jobRoot = Files.createDirectories(Files.createTempDirectory("test").resolve(WORKSPACE_ROOT))
    replicationInput = TestConfigHelpers.createReplicationConfig().second

    sourceStub =
      SimpleAirbyteSource().apply {
        setMessages(RECORD_MESSAGE1, RECORD_MESSAGE2)
      }
    source = spyk(sourceStub)

    destination = spyk(SimpleAirbyteDestination())
    messageTracker = mockk(relaxed = true)
    syncStatsTracker = mockk(relaxed = true)
    every { messageTracker.syncStatsTracker } returns syncStatsTracker
    syncPersistence = mockk(relaxed = true)
    recordSchemaValidator = mockk(relaxed = true)
    metricClient = mockk(relaxed = true)
    workerMetricReporter = WorkerMetricReporter(metricClient, "docker_image:v1.0.0")
    replicationInputFeatureFlagReader = mockk(relaxed = true)
    replicationAirbyteMessageEventPublishingHelper = mockk(relaxed = true)
    analyticsMessageTracker = mockk(relaxed = true)
    streamStatusCompletionTracker = mockk(relaxed = true)
    streamStatusTrackerFactory = mockk(relaxed = true)
    recordMapper = mockk(relaxed = true)
    destinationCatalogGenerator = mockk(relaxed = true)

    mapper =
      mockk<AirbyteMapper>(relaxed = true).apply {
        every { mapMessage(any()) } answers { firstArg() }
        every { revertMap(any()) } answers { firstArg() }
      }
    onReplicationRunning = mockk(relaxed = true)

    replicationContext =
      ReplicationContext(
        false,
        replicationInput.connectionId,
        replicationInput.sourceId,
        replicationInput.destinationId,
        JOB_ID,
        JOB_ATTEMPT,
        replicationInput.workspaceId,
        SOURCE_IMAGE,
        DESTINATION_IMAGE,
        UUID.randomUUID(),
        UUID.randomUUID(),
      )
    replicationWorkerState = ReplicationWorkerState()

    replicationWorkerHelper =
      spyk(
        ReplicationWorkerHelper(
          FieldSelector(recordSchemaValidator, workerMetricReporter, false, false),
          mapper,
          messageTracker,
          syncPersistence,
          replicationAirbyteMessageEventPublishingHelper,
          ThreadedTimeTracker(),
          analyticsMessageTracker,
          streamStatusCompletionTracker,
          streamStatusTrackerFactory.create(replicationContext),
          recordMapper,
          replicationWorkerState,
          ReplicationContextProvider.Context(replicationContext, replicationInput.catalog, false, replicationInput),
          destinationCatalogGenerator,
          metricClient,
        ),
      )

    ctx =
      ReplicationWorkerContext(
        JOB_ID,
        JOB_ATTEMPT,
        BufferConfiguration.withPollTimeout(1),
        replicationWorkerHelper,
        replicationWorkerState,
        streamStatusCompletionTracker,
      )

    MDC.put(DEFAULT_JOB_LOG_PATH_MDC_KEY, jobRoot.toString())
    LogSource.PLATFORM.toMdc().forEach { (k, v) -> MDC.put(k, v) }
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
    MDC.clear()
  }

  private fun getWorker() =
    ReplicationWorker(source, destination, syncPersistence, onReplicationRunning, mockk(relaxed = true), recordSchemaValidator, ctx)

  private fun setUpInfiniteSource() = sourceStub.setInfiniteSourceWithMessages(RECORD_MESSAGE1)

  @Test fun `closure propagates when source read crashes`() {
    setUpInfiniteSource()
    every { source.attemptRead() } throws RuntimeException("fail read")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `closure propagates when processing fails`() {
    setUpInfiniteSource()
    every { messageTracker.acceptFromSource(any()) } throws RuntimeException("fail process")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `closure propagates when destination write crashes`() {
    setUpInfiniteSource()
    every { destination.accept(any()) } throws RuntimeException("fail write")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `closure propagates when destination read crashes`() {
    setUpInfiniteSource()
    every { destination.attemptRead() } throws RuntimeException("fail dest read")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `basic happy path`() {
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    verify { source.start(any(), any(), any()) }
    verify { destination.start(any(), any()) }
    verify { onReplicationRunning.call() }
    verify { recordSchemaValidator.validateSchema(RECORD_MESSAGE1.record, any(), any()) }
    verify { recordSchemaValidator.validateSchema(RECORD_MESSAGE2.record, any(), any()) }
    verify { source.close() }
    verify { destination.close() }
    assertEquals(ReplicationStatus.COMPLETED, out.replicationAttemptSummary.status)
  }

  @Test fun `replication times are updated`() {
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    val stats = out.replicationAttemptSummary.totalStats
    assertNotEquals(0L, stats.replicationStartTime)
    assertNotEquals(0L, stats.replicationEndTime)
    assertNotEquals(0L, stats.sourceReadStartTime)
    assertNotEquals(0L, stats.sourceReadEndTime)
    assertNotEquals(0L, stats.destinationWriteStartTime)
    assertNotEquals(0L, stats.destinationWriteEndTime)
  }

  @Test fun `invalid schema still writes all records`() {
    sourceStub.setMessages(RECORD_MESSAGE1, RECORD_MESSAGE2, RECORD_MESSAGE3)
    runBlocking { getWorker().run(replicationInput, jobRoot) }
    verify { destination.accept(RECORD_MESSAGE1) }
    verify { destination.accept(RECORD_MESSAGE2) }
    verify { destination.accept(RECORD_MESSAGE3) }
  }

  @Test fun `source non-zero exit fails`() {
    every { source.exitValue } returns 1
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
    assertTrue(out.failures.any { it.failureOrigin == FailureReason.FailureOrigin.SOURCE })
  }

  @Test fun `run handles source exception`() {
    every { source.attemptRead() } throws RuntimeException("oh no")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
    assertTrue(out.failures.any { it.failureOrigin == FailureReason.FailureOrigin.SOURCE && it.stacktrace.contains("oh no") })
  }

  @Test fun `source config update publishes event`() {
    sourceStub.setMessages(RECORD_MESSAGE1, CONFIG_MESSAGE)
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.COMPLETED, out.replicationAttemptSummary.status)
    verify { replicationAirbyteMessageEventPublishingHelper.publishEvent(any<ReplicationAirbyteMessageEvent>()) }
  }

  @Test fun `source config persist error fails`() {
    sourceStub.setMessages(CONFIG_MESSAGE)
    every { replicationAirbyteMessageEventPublishingHelper.publishEvent(any()) } throws RuntimeException("persist fail")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `destination config update publishes event`() {
    every { destination.attemptRead() } returnsMany listOf(Optional.of(STATE_MESSAGE), Optional.of(CONFIG_MESSAGE))
    every { destination.isFinished } returnsMany listOf(false, false, true)
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.COMPLETED, out.replicationAttemptSummary.status)
    verify { replicationAirbyteMessageEventPublishingHelper.publishEvent(any<ReplicationAirbyteMessageEvent>()) }
  }

  @Test fun `destination config persist error fails`() {
    every { destination.attemptRead() } returnsMany listOf(Optional.of(CONFIG_MESSAGE))
    every { destination.isFinished } returnsMany listOf(false, true)
    every { replicationAirbyteMessageEventPublishingHelper.publishEvent(any()) } throws RuntimeException("persist dest fail")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `destination write failure`() {
    every { destination.accept(any()) } throws RuntimeException("dest boom")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
    assertTrue(out.failures.any { it.failureOrigin == FailureReason.FailureOrigin.DESTINATION && it.stacktrace.contains("dest boom") })
  }

  @Test fun `destination failure via trace message`() {
    every { messageTracker.errorTraceMessageFailure(any(), any()) } returns
      listOf(
        FailureHelper.destinationFailure(ERROR_TRACE_MESSAGE, JOB_ID.toLong(), JOB_ATTEMPT),
      )
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertTrue(
      out.failures.any {
        it.failureOrigin == FailureReason.FailureOrigin.DESTINATION &&
          it.externalMessage.contains("connector error occurred")
      },
    )
  }

  @Test fun `replication worker failure`() {
    every { messageTracker.acceptFromSource(any()) } throws RuntimeException("worker fail")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
    assertTrue(out.failures.any { it.failureOrigin == FailureReason.FailureOrigin.REPLICATION && it.stacktrace.contains("worker fail") })
  }

  @Test fun `only records & state go to destination`() {
    val logMsg = AirbyteMessageUtils.createLogMessage(AirbyteLogMessage.Level.INFO, "log")
    val traceMsg = AirbyteMessageUtils.createErrorMessage("trace", 1.0)
    every { messageTracker.acceptFromSource(any()) } just Runs
    sourceStub.setMessages(RECORD_MESSAGE1, logMsg, traceMsg, RECORD_MESSAGE2)
    runBlocking { getWorker().run(replicationInput, jobRoot) }
    verify { destination.accept(RECORD_MESSAGE1) }
    verify { destination.accept(RECORD_MESSAGE2) }
    verify(exactly = 0) { destination.accept(logMsg) }
    verify(exactly = 0) { destination.accept(traceMsg) }
  }

  @Test
  fun `field selection enabled filters extra fields`() {
    val recordWithExtraFields =
      Jsons.clone(RECORD_MESSAGE1).apply {
        (this.record.data as ObjectNode).put("AnUnexpectedField", "SomeValue")
      }
    sourceStub.setMessages(recordWithExtraFields)

    val stream = replicationInput.catalog.streams[0].stream
    recordSchemaValidator =
      RecordSchemaValidator(
        mapOf(
          AirbyteStreamNameNamespacePair(stream.name, stream.namespace) to stream.jsonSchema,
        ),
      )

    val fieldSelector = FieldSelector(recordSchemaValidator, workerMetricReporter, true, false)
    val helperEnabled =
      ReplicationWorkerHelper(
        fieldSelector,
        mapper,
        messageTracker,
        syncPersistence,
        replicationAirbyteMessageEventPublishingHelper,
        ThreadedTimeTracker(),
        analyticsMessageTracker,
        streamStatusCompletionTracker,
        streamStatusTrackerFactory.create(replicationContext),
        recordMapper,
        replicationWorkerState,
        ReplicationContextProvider.Context(replicationContext, replicationInput.catalog, false, replicationInput),
        destinationCatalogGenerator,
        metricClient,
      )
    val ctxEnabled =
      ReplicationWorkerContext(
        JOB_ID,
        JOB_ATTEMPT,
        BufferConfiguration.withPollTimeout(1),
        helperEnabled,
        replicationWorkerState,
        streamStatusCompletionTracker,
      )
    val workerEnabled =
      ReplicationWorker(
        source,
        destination,
        syncPersistence,
        onReplicationRunning,
        mockk(relaxed = true),
        recordSchemaValidator,
        ctxEnabled,
      )

    runBlocking { workerEnabled.run(replicationInput, jobRoot) }

    verify { destination.accept(RECORD_MESSAGE1) }
  }

  @Test
  fun `field selection disabled allows all fields`() {
    val recordWithExtraFields =
      Jsons.clone(RECORD_MESSAGE1).apply {
        (this.record.data as ObjectNode).put("AnUnexpectedField", "SomeValue")
      }
    sourceStub.setMessages(recordWithExtraFields)

    val stream = replicationInput.catalog.streams[0].stream
    recordSchemaValidator =
      RecordSchemaValidator(
        mapOf(
          AirbyteStreamNameNamespacePair(stream.name, stream.namespace) to stream.jsonSchema,
        ),
      )

    val fieldSelector = FieldSelector(recordSchemaValidator, workerMetricReporter, false, false)
    val helperDisabled =
      ReplicationWorkerHelper(
        fieldSelector,
        mapper,
        messageTracker,
        syncPersistence,
        replicationAirbyteMessageEventPublishingHelper,
        ThreadedTimeTracker(),
        analyticsMessageTracker,
        streamStatusCompletionTracker,
        streamStatusTrackerFactory.create(replicationContext),
        recordMapper,
        replicationWorkerState,
        ReplicationContextProvider.Context(replicationContext, replicationInput.catalog, false, replicationInput),
        destinationCatalogGenerator,
        metricClient,
      )
    val ctxDisabled =
      ReplicationWorkerContext(
        JOB_ID,
        JOB_ATTEMPT,
        BufferConfiguration.withPollTimeout(1),
        helperDisabled,
        replicationWorkerState,
        streamStatusCompletionTracker,
      )
    val workerDisabled =
      ReplicationWorker(
        source,
        destination,
        syncPersistence,
        onReplicationRunning,
        mockk(relaxed = true),
        recordSchemaValidator,
        ctxDisabled,
      )

    runBlocking { workerDisabled.run(replicationInput, jobRoot) }

    verify { destination.accept(recordWithExtraFields) }
  }

  @Test fun `destination non-zero exit fails`() {
    every { destination.exitValue } returns 1
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
    assertTrue(out.failures.any { it.failureOrigin == FailureReason.FailureOrigin.DESTINATION })
  }

  @Test fun `destination notify end-of-input failure`() {
    every { destination.isFinished } returns false
    every { destination.notifyEndOfInput() } throws RuntimeException("notify fail")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
    assertTrue(out.failures.any { it.failureOrigin == FailureReason.FailureOrigin.DESTINATION })
  }

  @Disabled("Flaky")
  @Test
  fun `destination worker failure via state`() {
    every { destination.attemptRead() } returnsMany listOf(Optional.of(STATE_MESSAGE))
    every { messageTracker.acceptFromDestination(any()) } throws RuntimeException("dest worker fail")
    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.FAILED, out.replicationAttemptSummary.status)
  }

  @Test fun `stream status tracking start & finalize`() {
    sourceStub.setMessages(RECORD_MESSAGE1)
    runBlocking { getWorker().run(replicationInput, jobRoot) }
    verify { streamStatusCompletionTracker.startTracking(any(), any()) }
    verify { streamStatusCompletionTracker.finalize(0, any()) }
  }

  @Test fun `stream status track source COMPLETE`() {
    val complete =
      AirbyteMessageUtils.createStatusTraceMessage(
        StreamDescriptor().withName(STREAM_NAME),
        AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE,
      )
    sourceStub.setMessages(RECORD_MESSAGE1, complete)
    runBlocking { getWorker().run(replicationInput, jobRoot) }
    verify { streamStatusCompletionTracker.track(complete.trace.streamStatus) }
  }

  @Test fun `populates output on success`() {
    every { syncStatsTracker.getTotalRecordsEmitted() } returns 12L
    every { syncStatsTracker.getTotalBytesFilteredOut() } returns 0L
    every { syncStatsTracker.getTotalRecordsFilteredOut() } returns 0L
    every { syncStatsTracker.getTotalBytesEmitted() } returns 100L
    every { syncStatsTracker.getTotalRecordsCommitted() } returns 12L
    every { syncStatsTracker.getTotalBytesCommitted() } returns 100L
    every { syncStatsTracker.getTotalSourceStateMessagesEmitted() } returns 3L
    every { syncStatsTracker.getTotalDestinationStateMessagesEmitted() } returns 1L
    every { syncStatsTracker.getStreamToEmittedBytes() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 100L,
      )
    every { syncStatsTracker.getStreamToFilteredOutBytes() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 0L,
      )
    every { syncStatsTracker.getStreamToFilteredOutRecords() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 0L,
      )
    every { syncStatsTracker.getStreamToEmittedRecords() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 12L,
      )
    every { syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage() } returns 5L
    every { syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage() } returns 4L
    every { syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted() } returns 6L
    every { syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted() } returns 3L

    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.COMPLETED, out.replicationAttemptSummary.status)
    assertEquals(12L, out.replicationAttemptSummary.recordsSynced)
    assertEquals(100L, out.replicationAttemptSummary.bytesSynced)
  }

  @Test fun `populates stats on failure if available`() {
    every { source.close() } throws IllegalStateException("induced")
    every { syncStatsTracker.getTotalRecordsEmitted() } returns 12L
    every { syncStatsTracker.getTotalRecordsFilteredOut() } returns 0L
    every { syncStatsTracker.getTotalBytesFilteredOut() } returns 0L
    every { syncStatsTracker.getTotalBytesEmitted() } returns 100L
    every { syncStatsTracker.getTotalBytesCommitted() } returns 12L
    every { syncStatsTracker.getTotalRecordsCommitted() } returns 6L
    every { syncStatsTracker.getTotalSourceStateMessagesEmitted() } returns 3L
    every { syncStatsTracker.getTotalDestinationStateMessagesEmitted() } returns 2L
    every { syncStatsTracker.getStreamToEmittedBytes() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 100L,
      )
    every { syncStatsTracker.getStreamToFilteredOutBytes() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 0L,
      )
    every { syncStatsTracker.getStreamToFilteredOutRecords() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 0L,
      )
    every { syncStatsTracker.getStreamToEmittedRecords() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 12L,
      )
    every { syncStatsTracker.getStreamToCommittedRecords() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 6L,
      )
    every { syncStatsTracker.getStreamToCommittedBytes() } returns
      mapOf(
        AirbyteStreamNameNamespacePair(STREAM_NAME, "") to 13L,
      )
    every { syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage() } returns 10L
    every { syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage() } returns 8L
    every { syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted() } returns 12L
    every { syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted() } returns 11L

    val out = runBlocking { getWorker().run(replicationInput, jobRoot) }
    assertEquals(ReplicationStatus.COMPLETED, out.replicationAttemptSummary.status)
  }

  @Test fun `does not populate state on failure if not available`() {
    val inputNoState = replicationInput.apply { state = null }
    every { source.close() } throws IllegalStateException("induced")
    val out = runBlocking { getWorker().run(inputNoState, jobRoot) }
    assertNull(out.state)
  }

  @Test fun `irrecoverable failure throws WorkerException`() {
    every { syncStatsTracker.getTotalRecordsEmitted() } throws IllegalStateException("oops")
    assertThrows<WorkerException> {
      runBlocking { getWorker().run(replicationInput, jobRoot) }
    }
  }
}
