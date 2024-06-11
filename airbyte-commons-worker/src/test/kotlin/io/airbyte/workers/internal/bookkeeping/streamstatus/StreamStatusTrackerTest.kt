package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStreamState
import io.airbyte.protocol.models.AirbyteStreamStatusRateLimitedReason
import io.airbyte.protocol.models.AirbyteStreamStatusReason
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.general.CachingFeatureFlagClient
import io.airbyte.workers.helper.AirbyteMessageDataExtractor
import io.airbyte.workers.internal.bookkeeping.events.StreamStatusUpdateEvent
import io.micronaut.context.event.ApplicationEventPublisher
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus as ProtocolEnum

class StreamStatusTrackerTest {
  private lateinit var tracker: StreamStatusTracker

  private lateinit var dataExtractor: AirbyteMessageDataExtractor
  private lateinit var store: StreamStatusStateStore
  private lateinit var eventPublisher: ApplicationEventPublisher<StreamStatusUpdateEvent>
  private lateinit var ffClient: CachingFeatureFlagClient
  private lateinit var apiCache: MutableMap<StreamStatusKey, StreamStatusRead>

  @BeforeEach
  fun setup() {
    dataExtractor = mockk()
    store = mockk()
    eventPublisher = mockk()
    ffClient = mockk()
    apiCache = HashMap()

    tracker = StreamStatusTracker(dataExtractor, store, eventPublisher, ffClient, Fixtures.ctx, apiCache)

    every { ffClient.boolVariation(any(), any()) } returns true
    every { dataExtractor.getStreamFromMessage(any()) } returns Fixtures.streamDescriptor
    every { store.get(any()) } returns null
  }

  @Test
  fun noopsIfStreamDescriptorNotExtractable() {
    every { dataExtractor.getStreamFromMessage(any()) } returns null

    tracker.track(Fixtures.traceMsg())

    verify { store wasNot Called }
    verify { eventPublisher wasNot Called }
  }

  @Test
  fun ignoresNonStreamStatusTraceMessages() {
    val spy = spyk(tracker)

    spy.track(Fixtures.traceMsg(AirbyteTraceMessage.Type.ERROR))
    spy.track(Fixtures.traceMsg(AirbyteTraceMessage.Type.ESTIMATE))
    spy.track(Fixtures.traceMsg(AirbyteTraceMessage.Type.ANALYTICS))

    verify(exactly = 0) { spy.trackEvent(any(), any()) }
  }

  @Test
  fun ignoresNonStreamStateMessages() {
    val spy = spyk(tracker)

    spy.track(Fixtures.stateMsg(AirbyteStateMessage.AirbyteStateType.GLOBAL))
    spy.track(Fixtures.stateMsg(AirbyteStateMessage.AirbyteStateType.LEGACY))
    spy.track(Fixtures.stateMsg(AirbyteStateMessage.AirbyteStateType.GLOBAL))

    verify(exactly = 0) { spy.trackState(any(), any()) }
  }

  @Test
  fun ignoresNonRecordStateStatusMessages() {
    val spy = spyk(tracker)

    spy.track(Fixtures.msg(AirbyteMessage.Type.LOG))
    spy.track(Fixtures.msg(AirbyteMessage.Type.SPEC))
    spy.track(Fixtures.msg(AirbyteMessage.Type.CONNECTION_STATUS))
    spy.track(Fixtures.msg(AirbyteMessage.Type.CATALOG))
    spy.track(Fixtures.msg(AirbyteMessage.Type.CONTROL))

    verify(exactly = 0) { spy.trackState(any(), any()) }
  }

  @Test
  fun setsRunStateToRunningForStarted() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.STARTED))

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.RUNNING) }
  }

  @Test
  fun setsRunStateToRunningForRunning() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.RUNNING))

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.RUNNING) }
  }

  @Test
  fun setsRunStateToIncompleteForIncomplete() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.INCOMPLETE))

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.INCOMPLETE) }
  }

  @Test
  fun setsRunStateToRunningForRecord() {
    every { store.isRateLimited(any()) } returns false
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.setMetadata(any(), any()) } returns StreamStatusValue()
    every { store.markStreamNotEmpty(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.recordMsg())

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.RUNNING) }
  }

  @Test
  fun marksSourceCompleteForComplete() {
    every { store.markSourceComplete(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.COMPLETE))

    verify(exactly = 1) { store.markSourceComplete(Fixtures.key) }
  }

  @Test
  fun marksStreamNotEmptyOnRecord() {
    every { store.isRateLimited(any()) } returns false
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.markStreamNotEmpty(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.recordMsg())

    verify(exactly = 1) { store.markStreamNotEmpty(Fixtures.key) }
  }

  @Test
  fun setsMetadataToNullIfRateLimited() {
    every { store.isRateLimited(any()) } returns true
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.setMetadata(any(), any()) } returns StreamStatusValue()
    every { store.markStreamNotEmpty(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.recordMsg())

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.RUNNING) }
    verify(exactly = 1) { store.setMetadata(Fixtures.key, null) }
  }

  @Test
  fun setsLatestStateIdOnStateIfDestNotComplete() {
    every { store.isDestComplete(any(), any()) } returns false
    every { store.setLatestStateId(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.stateMsg(id = 54321))

    verify(exactly = 1) { store.setLatestStateId(Fixtures.key, 54321) }
  }

  @Test
  fun setsMetadataForRateLimited() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.setMetadata(any(), any()) } returns StreamStatusValue()

    val quotaReset = 456L
    tracker.track(Fixtures.rateLimitedMsg(quotaReset))

    val expected = StreamStatusRateLimitedMetadata(quotaReset = quotaReset)

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.RATE_LIMITED) }
    verify(exactly = 1) { store.setMetadata(Fixtures.key, eq(expected)) }
  }

  @Test
  fun setsRunStateToCompleteOnStateIfDestComplete() {
    every { store.isDestComplete(any(), any()) } returns true
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.stateMsg(id = 54321))

    verify(exactly = 1) { store.setRunState(Fixtures.key, ApiEnum.COMPLETE) }
  }

  @ParameterizedTest
  @MethodSource("runStateTransitionMatrix")
  fun publishesEventIfRunStateChanges(
    startingRunState: ApiEnum?,
    updatedRunState: ApiEnum,
  ) {
    every { store.get(any()) } returns StreamStatusValue(runState = startingRunState)
    every { store.setRunState(any(), any()) } returns StreamStatusValue(runState = updatedRunState)
    every { eventPublisher.publishEvent(any()) } returns Unit

    tracker.track(Fixtures.traceMsg())

    verify(exactly = 1) {
      eventPublisher.publishEvent(
        eq(
          StreamStatusUpdateEvent(
            key = Fixtures.key,
            runState = updatedRunState,
            ctx = Fixtures.ctx,
            cache = apiCache,
          ),
        ),
      )
    }
  }

  @ParameterizedTest
  @MethodSource("runStateValues")
  fun doesNotPublishEventIfRunStateDoesNotChange(runState: ApiEnum?) {
    every { store.get(any()) } returns StreamStatusValue(runState = runState)
    every { store.setRunState(any(), any()) } returns StreamStatusValue(runState = runState)

    tracker.track(Fixtures.traceMsg())

    verify(exactly = 0) { eventPublisher.publishEvent(any()) }
  }

  companion object {
    @JvmStatic
    fun runStateTransitionMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(null, ApiEnum.RUNNING),
        Arguments.of(null, ApiEnum.RATE_LIMITED),
        Arguments.of(null, ApiEnum.COMPLETE),
        Arguments.of(null, ApiEnum.INCOMPLETE),
        Arguments.of(ApiEnum.RATE_LIMITED, ApiEnum.RUNNING),
        Arguments.of(ApiEnum.RATE_LIMITED, ApiEnum.COMPLETE),
        Arguments.of(ApiEnum.RATE_LIMITED, ApiEnum.INCOMPLETE),
        Arguments.of(ApiEnum.RUNNING, ApiEnum.RATE_LIMITED),
        Arguments.of(ApiEnum.RUNNING, ApiEnum.COMPLETE),
        Arguments.of(ApiEnum.RUNNING, ApiEnum.INCOMPLETE),
      )
    }

    @JvmStatic
    fun runStateValues(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(null),
        Arguments.of(ApiEnum.RUNNING),
        Arguments.of(ApiEnum.RATE_LIMITED),
        Arguments.of(ApiEnum.COMPLETE),
        Arguments.of(ApiEnum.INCOMPLETE),
      )
    }
  }

  object Fixtures {
    private const val STREAM_NAME = "test-stream-name"
    private const val STREAM_NAMESPACE = "test-stream-namespace"

    private const val STATE_ID = 123

    val streamDescriptor: StreamDescriptor =
      StreamDescriptor()
        .withName(STREAM_NAME)
        .withNamespace(STREAM_NAMESPACE)

    val key = StreamStatusKey.fromProtocol(streamDescriptor)

    fun traceMsg(
      type: AirbyteTraceMessage.Type = AirbyteTraceMessage.Type.STREAM_STATUS,
      status: ProtocolEnum = ProtocolEnum.RUNNING,
    ): AirbyteMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.TRACE)
        .withTrace(
          AirbyteTraceMessage()
            .withType(type)
            .withEmittedAt(1.0)
            .withStreamStatus(
              AirbyteStreamStatusTraceMessage()
                .withStatus(status)
                .withStreamDescriptor(
                  streamDescriptor,
                ),
            ),
        )

    fun stateMsg(
      type: AirbyteStateMessage.AirbyteStateType = AirbyteStateMessage.AirbyteStateType.STREAM,
      id: Int = STATE_ID,
    ): AirbyteMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(
          AirbyteStateMessage()
            .withAdditionalProperty("id", id)
            .withType(type)
            .withStream(
              AirbyteStreamState()
                .withStreamDescriptor(streamDescriptor),
            ),
        )

    fun recordMsg(): AirbyteMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(
          AirbyteRecordMessage()
            .withStream(STREAM_NAME)
            .withNamespace(STREAM_NAMESPACE),
        )

    fun rateLimitedMsg(quotaReset: Long): AirbyteMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.TRACE)
        .withTrace(
          AirbyteTraceMessage()
            .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
            .withEmittedAt(1.0)
            .withStreamStatus(
              AirbyteStreamStatusTraceMessage()
                .withStatus(AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.RUNNING)
                .withStreamDescriptor(
                  streamDescriptor,
                )
                .withReasons(
                  listOf(
                    AirbyteStreamStatusReason()
                      .withType(AirbyteStreamStatusReason.AirbyteStreamStatusReasonType.RATE_LIMITED)
                      .withRateLimited(
                        AirbyteStreamStatusRateLimitedReason().withQuotaReset(quotaReset),
                      ),
                  ),
                ),
            ),
        )

    fun msg(type: AirbyteMessage.Type): AirbyteMessage =
      AirbyteMessage()
        .withType(type)

    val ctx =
      ReplicationContext(
        false,
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID(),
        12L,
        34,
        UUID.randomUUID(),
        "source-image",
        "dest-image",
        UUID.randomUUID(),
        UUID.randomUUID(),
      )
  }
}
