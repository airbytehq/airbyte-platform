/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.container.orchestrator.RateLimitedMessageHelper
import io.airbyte.container.orchestrator.bookkeeping.events.StreamStatusUpdateEvent
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.container.orchestrator.worker.util.AirbyteMessageDataExtractor
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusRateLimitedReason
import io.airbyte.protocol.models.v0.AirbyteStreamStatusReason
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.micronaut.context.event.ApplicationEventPublisher
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
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus as ProtocolEnum

class StreamStatusTrackerTest {
  private lateinit var tracker: StreamStatusTracker

  private lateinit var dataExtractor: AirbyteMessageDataExtractor
  private lateinit var store: StreamStatusStateStore
  private lateinit var eventPublisher: ApplicationEventPublisher<StreamStatusUpdateEvent>
  private lateinit var rateLimitedMessageHelper: RateLimitedMessageHelper

  @BeforeEach
  fun setup() {
    dataExtractor = mockk()
    store = mockk()
    eventPublisher = mockk()
    rateLimitedMessageHelper = mockk(relaxed = true)

    val context =
      ReplicationContextProvider.Context(
        configuredCatalog = mockk(),
        replicationContext = Fixtures.ctx,
        supportRefreshes = false,
        replicationInput = mockk(),
      )
    tracker = StreamStatusTracker(dataExtractor, store, eventPublisher, context, rateLimitedMessageHelper)

    every { dataExtractor.getStreamFromMessage(any()) } returns Fixtures.streamDescriptor1
    every { store.get(any()) } returns null
    every { store.isGlobalComplete(any()) } returns false
  }

  @Test
  fun callsTrackStreamIfStreamExtractable() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    val spy = spyk(tracker)
    val msg = Fixtures.traceMsg()

    spy.track(msg)

    verify(exactly = 1) { spy.trackStream(Fixtures.streamDescriptor1, msg) }
  }

  @Test
  fun callsTrackGlobalIfStreamNotExtractable() {
    every { dataExtractor.getStreamFromMessage(any()) } returns null

    val spy = spyk(tracker)
    val msg = Fixtures.traceMsg()

    spy.track(msg)

    verify(exactly = 1) { spy.trackGlobal(msg) }
  }

  @Test
  fun trackGlobalTracksGlobalStateMsgs() {
    every { store.setLatestGlobalStateId(any()) } returns 1

    val spy = spyk(tracker)

    spy.trackGlobal(Fixtures.stateMsg(AirbyteStateMessage.AirbyteStateType.GLOBAL))

    verify(exactly = 1) { spy.trackGlobalState(any()) }
  }

  @Test
  fun trackGlobalNoopsForNonGlobalStateMsgs() {
    val spy = spyk(tracker)

    spy.trackGlobal(Fixtures.stateMsg(AirbyteStateMessage.AirbyteStateType.STREAM))
    spy.trackGlobal(Fixtures.stateMsg(AirbyteStateMessage.AirbyteStateType.LEGACY))
    spy.trackGlobal(Fixtures.traceMsg(AirbyteTraceMessage.Type.ERROR))
    spy.trackGlobal(Fixtures.traceMsg(AirbyteTraceMessage.Type.ESTIMATE))
    spy.trackGlobal(Fixtures.traceMsg(AirbyteTraceMessage.Type.ANALYTICS))
    spy.trackGlobal(Fixtures.msg(AirbyteMessage.Type.LOG))
    spy.trackGlobal(Fixtures.msg(AirbyteMessage.Type.SPEC))
    spy.trackGlobal(Fixtures.msg(AirbyteMessage.Type.CONNECTION_STATUS))
    spy.trackGlobal(Fixtures.msg(AirbyteMessage.Type.CATALOG))
    spy.trackGlobal(Fixtures.msg(AirbyteMessage.Type.CONTROL))
    spy.trackGlobal(Fixtures.recordMsg())

    verify(exactly = 0) { spy.trackGlobalState(any()) }
  }

  @Test
  fun trackGlobalStateUpdatesLatestGlobalStateId() {
    val id = 12
    every { store.setLatestGlobalStateId(any()) } returns id
    every { store.isGlobalComplete(id) } returns false
    every { store.entries() } returns
      mapOf(
        Fixtures.key1 to Fixtures.completeValue(),
        Fixtures.key2 to Fixtures.completeValue(),
        Fixtures.key3 to Fixtures.completeValue(),
        Fixtures.key4 to Fixtures.completeValue(),
      ).entries

    val msg =
      Fixtures.stateMsg(
        type = AirbyteStateMessage.AirbyteStateType.GLOBAL,
        id = id,
      )

    tracker.trackGlobalState(msg.state)

    verify(exactly = 1) { store.setLatestGlobalStateId(id) }
    verify(exactly = 0) { eventPublisher.publishEvent(any()) }
  }

  @Test
  fun trackGlobalStatePublishesCompletesWhenGloballyComplete() {
    val id = 12

    every { store.setLatestGlobalStateId(any()) } returns id
    every { store.isGlobalComplete(id) } returns true
    every { store.entries() } returns
      mapOf(
        Fixtures.key1 to Fixtures.completeValue(),
        Fixtures.key2 to Fixtures.completeValue(),
        Fixtures.key3 to Fixtures.completeValue(),
        Fixtures.key4 to Fixtures.runningValue(),
      ).entries
    every { eventPublisher.publishEvent(any()) } returns Unit

    val msg =
      Fixtures.stateMsg(
        type = AirbyteStateMessage.AirbyteStateType.GLOBAL,
        id = id,
      )

    tracker.trackGlobalState(msg.state)

    verify(exactly = 1) { store.setLatestGlobalStateId(id) }
    // verify one message published per event
    verify(exactly = 1) {
      eventPublisher.publishEvent(
        eq(
          StreamStatusUpdateEvent(
            key = Fixtures.key1,
            runState = ApiEnum.COMPLETE,
            ctx = Fixtures.ctx,
            cache = tracker.getResponseCache(),
          ),
        ),
      )
    }
    verify(exactly = 1) {
      eventPublisher.publishEvent(
        eq(
          StreamStatusUpdateEvent(
            key = Fixtures.key2,
            runState = ApiEnum.COMPLETE,
            ctx = Fixtures.ctx,
            cache = tracker.getResponseCache(),
          ),
        ),
      )
    }
    verify(exactly = 1) {
      eventPublisher.publishEvent(
        eq(
          StreamStatusUpdateEvent(
            key = Fixtures.key3,
            runState = ApiEnum.COMPLETE,
            ctx = Fixtures.ctx,
            cache = tracker.getResponseCache(),
          ),
        ),
      )
    }
    // this one wasn't source complete, so we don't send an update
    // this case shouldn't happen in practice, but we program defensively
    verify(exactly = 0) {
      eventPublisher.publishEvent(
        eq(
          StreamStatusUpdateEvent(
            key = Fixtures.key4,
            runState = ApiEnum.COMPLETE,
            ctx = Fixtures.ctx,
            cache = tracker.getResponseCache(),
          ),
        ),
      )
    }
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

    verify(exactly = 0) { spy.trackStreamState(any(), any()) }
  }

  @Test
  fun ignoresNonRecordStateStatusMessages() {
    val spy = spyk(tracker)

    spy.track(Fixtures.msg(AirbyteMessage.Type.LOG))
    spy.track(Fixtures.msg(AirbyteMessage.Type.SPEC))
    spy.track(Fixtures.msg(AirbyteMessage.Type.CONNECTION_STATUS))
    spy.track(Fixtures.msg(AirbyteMessage.Type.CATALOG))
    spy.track(Fixtures.msg(AirbyteMessage.Type.CONTROL))

    verify(exactly = 0) { spy.trackStreamState(any(), any()) }
  }

  @Test
  fun setsRunStateToRunningForStarted() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.STARTED))

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.RUNNING) }
  }

  @Test
  fun setsRunStateToRunningForRunning() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.RUNNING))

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.RUNNING) }
  }

  @Test
  fun setsRunStateToIncompleteForIncomplete() {
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.INCOMPLETE))

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.INCOMPLETE) }
  }

  @Test
  fun setsRunStateToRunningForRecord() {
    every { store.isRateLimited(any()) } returns false
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.setMetadata(any(), any()) } returns StreamStatusValue()
    every { store.markStreamNotEmpty(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.recordMsg())

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.RUNNING) }
  }

  @Test
  fun marksSourceCompleteForComplete() {
    every { store.markSourceComplete(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.traceMsg(status = ProtocolEnum.COMPLETE))

    verify(exactly = 1) { store.markSourceComplete(Fixtures.key1) }
  }

  @Test
  fun marksStreamNotEmptyOnRecord() {
    every { store.isRateLimited(any()) } returns false
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.markStreamNotEmpty(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.recordMsg())

    verify(exactly = 1) { store.markStreamNotEmpty(Fixtures.key1) }
  }

  @Test
  fun setsMetadataToNullIfRateLimited() {
    every { store.isRateLimited(any()) } returns true
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.setMetadata(any(), any()) } returns StreamStatusValue()
    every { store.markStreamNotEmpty(any()) } returns StreamStatusValue()

    tracker.track(Fixtures.recordMsg())

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.RUNNING) }
    verify(exactly = 1) { store.setMetadata(Fixtures.key1, null) }
  }

  @Test
  fun setsLatestStateIdOnStateIfDestNotComplete() {
    every { store.isStreamComplete(any(), any()) } returns false
    every { store.setLatestStateId(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.stateMsg(id = 54321))

    verify(exactly = 1) { store.setLatestStateId(Fixtures.key1, 54321) }
  }

  @Test
  fun setsMetadataForRateLimited() {
    val quotaReset = 456L
    val expected = StreamStatusRateLimitedMetadata(quotaReset = quotaReset)

    every { rateLimitedMessageHelper.apiFromProtocol(any()) } returns expected
    every { rateLimitedMessageHelper.isStreamStatusRateLimitedMessage(any()) } returns true
    every { store.setRunState(any(), any()) } returns StreamStatusValue()
    every { store.setMetadata(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.rateLimitedMsg(quotaReset))

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.RATE_LIMITED) }
    verify(exactly = 1) { store.setMetadata(Fixtures.key1, eq(expected)) }
  }

  @Test
  fun setsRunStateToCompleteOnStateIfDestComplete() {
    every { store.isStreamComplete(any(), any()) } returns true
    every { store.setRunState(any(), any()) } returns StreamStatusValue()

    tracker.track(Fixtures.stateMsg(id = 54321))

    verify(exactly = 1) { store.setRunState(Fixtures.key1, ApiEnum.COMPLETE) }
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
            key = Fixtures.key1,
            runState = updatedRunState,
            ctx = Fixtures.ctx,
            cache = tracker.getResponseCache(),
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
    fun runStateTransitionMatrix(): Stream<Arguments> =
      Stream.of(
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

    @JvmStatic
    fun runStateValues(): Stream<Arguments> =
      Stream.of(
        Arguments.of(null),
        Arguments.of(ApiEnum.RUNNING),
        Arguments.of(ApiEnum.RATE_LIMITED),
        Arguments.of(ApiEnum.COMPLETE),
        Arguments.of(ApiEnum.INCOMPLETE),
      )
  }

  object Fixtures {
    private const val STREAM_NAME_1 = "test-stream-name-1"
    private const val STREAM_NAMESPACE_1 = "test-stream-namespace-1"
    private const val STREAM_NAME_2 = "test-stream-name-1"
    private const val STREAM_NAMESPACE_2 = "test-stream-namespace-1"

    private const val STATE_ID = 123

    val streamDescriptor1: StreamDescriptor =
      StreamDescriptor()
        .withName(STREAM_NAME_1)
        .withNamespace(STREAM_NAMESPACE_1)

    private val streamDescriptor2: StreamDescriptor =
      StreamDescriptor()
        .withName(STREAM_NAME_2)
        .withNamespace(STREAM_NAMESPACE_2)

    private val streamDescriptor3: StreamDescriptor =
      StreamDescriptor()
        .withName(STREAM_NAME_2)
        .withNamespace(STREAM_NAMESPACE_1)

    private val streamDescriptor4: StreamDescriptor =
      StreamDescriptor()
        .withName(STREAM_NAME_1)
        .withNamespace(null)

    val key1 = StreamStatusKey.fromProtocol(streamDescriptor1)
    val key2 = StreamStatusKey.fromProtocol(streamDescriptor2)
    val key3 = StreamStatusKey.fromProtocol(streamDescriptor3)
    val key4 = StreamStatusKey.fromProtocol(streamDescriptor4)

    fun runningValue() = StreamStatusValue(ApiEnum.RUNNING, 0, sourceComplete = false, streamEmpty = false)

    fun completeValue() = StreamStatusValue(ApiEnum.COMPLETE, 124, sourceComplete = true, streamEmpty = false)

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
                  streamDescriptor1,
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
                .withStreamDescriptor(streamDescriptor1),
            ),
        )

    fun recordMsg(): AirbyteMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.RECORD)
        .withRecord(
          AirbyteRecordMessage()
            .withStream(STREAM_NAME_1)
            .withNamespace(STREAM_NAMESPACE_1),
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
                .withStatus(ProtocolEnum.RUNNING)
                .withStreamDescriptor(
                  streamDescriptor1,
                ).withReasons(
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
