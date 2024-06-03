package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.completeValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.incompleteValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key1
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key2
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key3
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key4
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.runningValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.tenStateIdValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.value
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.zeroStateIdValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

class StreamStatusStateStoreTest {
  private lateinit var store: StreamStatusStateStore

  @BeforeEach
  fun setup() {
    store = StreamStatusStateStore()
  }

  @Test
  fun gettersSettersForAKey() {
    store.set(key1, runningValue())
    Assertions.assertEquals(store.get(key1), runningValue())

    store.set(key1, completeValue())
    Assertions.assertEquals(store.get(key1), completeValue())

    store.set(key1, incompleteValue())
    Assertions.assertEquals(store.get(key1), incompleteValue())
  }

  @Test
  fun setRunStateHandlesNoValue() {
    Assertions.assertNull(store.get(key1))

    val result1 = store.setRunState(key1, StreamStatusRunState.RUNNING)

    Assertions.assertEquals(StreamStatusRunState.RUNNING, result1.runState)
    Assertions.assertEquals(StreamStatusRunState.RUNNING, store.get(key1)!!.runState)
  }

  @Test
  fun setRunStateHandlesNoRunState() {
    val noRunState = StreamStatusValue(null, 0, sourceComplete = false, streamEmpty = false)
    store.set(key1, noRunState)
    Assertions.assertNull(store.get(key1)!!.runState)

    val result1 = store.setRunState(key1, StreamStatusRunState.RUNNING)

    Assertions.assertEquals(StreamStatusRunState.RUNNING, result1.runState)
    Assertions.assertEquals(StreamStatusRunState.RUNNING, store.get(key1)!!.runState)
  }

  @ParameterizedTest
  @MethodSource("runStateValidTransitionMatrix")
  fun setRunStateAllowsValidStateTransitions(
    currentRunState: ApiEnum?,
    incomingRunState: ApiEnum,
  ) {
    store.set(key1, value(currentRunState))

    val transitioned = store.setRunState(key1, incomingRunState)

    Assertions.assertEquals(incomingRunState, transitioned.runState)
    Assertions.assertEquals(incomingRunState, store.get(key1)!!.runState)
  }

  @ParameterizedTest
  @MethodSource("runStateInvalidTransitionMatrix")
  fun setRunStateIgnoresInvalidStateTransitions(
    currentRunState: ApiEnum,
    incomingRunState: ApiEnum,
  ) {
    store.set(key1, value(currentRunState))

    val transitioned = store.setRunState(key1, incomingRunState)

    Assertions.assertEquals(currentRunState, transitioned.runState)
    Assertions.assertEquals(currentRunState, store.get(key1)!!.runState)
  }

  @Test
  fun setLatestStateIdHandlesNoValue() {
    Assertions.assertNull(store.get(key1))

    val result1 = store.setLatestStateId(key1, 0)

    Assertions.assertEquals(0, result1.latestStateId)
    Assertions.assertEquals(0, store.get(key1)!!.latestStateId)
  }

  @Test
  fun setLatestStateIdHandlesNoStateId() {
    val noStateId = StreamStatusValue(null, null, sourceComplete = false, streamEmpty = false)
    store.set(key1, noStateId)
    Assertions.assertNull(store.get(key1)!!.latestStateId)

    val result1 = store.setLatestStateId(key1, 0)

    Assertions.assertEquals(0, result1.latestStateId)
    Assertions.assertEquals(0, store.get(key1)!!.latestStateId)
  }

  @Test
  fun setLatestStateIdAllowsMonotonicUpdates() {
    store.set(key1, zeroStateIdValue())
    store.set(key2, zeroStateIdValue())
    store.set(key3, tenStateIdValue())
    store.set(key4, tenStateIdValue())

    val zeroToOne = store.setLatestStateId(key1, 1)
    val zeroToTwenty = store.setLatestStateId(key2, 20)
    val tenToEleven = store.setLatestStateId(key3, 11)
    val tenTo124124 = store.setLatestStateId(key4, 124124)

    Assertions.assertEquals(1, zeroToOne.latestStateId)
    Assertions.assertEquals(1, store.get(key1)!!.latestStateId)

    Assertions.assertEquals(20, zeroToTwenty.latestStateId)
    Assertions.assertEquals(20, store.get(key2)!!.latestStateId)

    Assertions.assertEquals(11, tenToEleven.latestStateId)
    Assertions.assertEquals(11, store.get(key3)!!.latestStateId)

    Assertions.assertEquals(124124, tenTo124124.latestStateId)
    Assertions.assertEquals(124124, store.get(key4)!!.latestStateId)
  }

  @Test
  fun setLatestStateIdIgnoresNonmonotonicUpdates() {
    store.set(key1, tenStateIdValue())
    store.set(key2, tenStateIdValue())

    val tenToZero = store.setLatestStateId(key1, 0)
    val tenToNine = store.setLatestStateId(key2, 9)

    Assertions.assertEquals(10, tenToZero.latestStateId)
    Assertions.assertEquals(10, store.get(key1)!!.latestStateId)

    Assertions.assertEquals(10, tenToNine.latestStateId)
    Assertions.assertEquals(10, store.get(key2)!!.latestStateId)

    val tenToEight = store.setLatestStateId(key1, 8)
    val tenToOne = store.setLatestStateId(key2, 9)

    Assertions.assertEquals(10, tenToEight.latestStateId)
    Assertions.assertEquals(10, store.get(key1)!!.latestStateId)

    Assertions.assertEquals(10, tenToOne.latestStateId)
    Assertions.assertEquals(10, store.get(key2)!!.latestStateId)
  }

  @Test
  fun setMetadataHandlesNoValue() {
    Assertions.assertNull(store.get(key1))

    val metadata = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val result = store.setMetadata(key1, metadata)

    Assertions.assertEquals(metadata, result.metadata)
    Assertions.assertEquals(metadata, store.get(key1)!!.metadata)
  }

  @Test
  fun setMetadataHandlesNullMetadata() {
    val noMetadata = StreamStatusValue(null, 0, sourceComplete = false, streamEmpty = false, metadata = null)
    store.set(key1, noMetadata)
    Assertions.assertNull(store.get(key1)!!.metadata)

    val metadata = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val result = store.setMetadata(key1, metadata)

    Assertions.assertEquals(metadata, result.metadata)
    Assertions.assertEquals(metadata, store.get(key1)!!.metadata)
  }

  @Test
  fun setMetadataReplacesMetadata() {
    val metadata1 = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val value = StreamStatusValue(null, 0, sourceComplete = false, streamEmpty = false, metadata = metadata1)
    store.set(key1, value)
    Assertions.assertEquals(metadata1, store.get(key1)!!.metadata)

    val metadata2 = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val result = store.setMetadata(key1, metadata2)

    Assertions.assertEquals(metadata2, result.metadata)
    Assertions.assertEquals(metadata2, store.get(key1)!!.metadata)
  }

  @Test
  fun markSourceCompleteHandlesNoValue() {
    Assertions.assertNull(store.get(key1))

    val result1 = store.markSourceComplete(key1)

    Assertions.assertTrue(result1.sourceComplete)
    Assertions.assertTrue(store.get(key1)!!.sourceComplete)
  }

  @Test
  fun markSourceCompleteIsIdempotentAndIsolated() {
    store.set(key1, runningValue())
    store.set(key2, runningValue())
    Assertions.assertFalse(store.get(key1)!!.sourceComplete)
    Assertions.assertFalse(store.get(key2)!!.sourceComplete)

    store.markSourceComplete(key1)
    Assertions.assertTrue(store.get(key1)!!.sourceComplete)
    Assertions.assertFalse(store.get(key2)!!.sourceComplete)

    store.markSourceComplete(key1)
    Assertions.assertTrue(store.get(key1)!!.sourceComplete)
    Assertions.assertFalse(store.get(key2)!!.sourceComplete)
  }

  @Test
  fun markStreamNotEmptyHandlesNoValue() {
    Assertions.assertNull(store.get(key1))

    val result1 = store.markStreamNotEmpty(key1)

    Assertions.assertFalse(result1.streamEmpty)
    Assertions.assertFalse(store.get(key1)!!.streamEmpty)
  }

  @Test
  fun markStreamNotEmptyIsIdempotentAndIsolated() {
    store.set(key1, runningValue())
    store.set(key2, runningValue())
    Assertions.assertTrue(store.get(key1)!!.streamEmpty)
    Assertions.assertTrue(store.get(key2)!!.streamEmpty)

    store.markStreamNotEmpty(key1)
    Assertions.assertFalse(store.get(key1)!!.streamEmpty)
    Assertions.assertTrue(store.get(key2)!!.streamEmpty)

    store.markStreamNotEmpty(key1)
    Assertions.assertFalse(store.get(key1)!!.streamEmpty)
    Assertions.assertTrue(store.get(key2)!!.streamEmpty)
  }

  @Test
  fun isDestCompleteHandlesNoValue() {
    Assertions.assertNull(store.get(key1))

    val result1 = store.isDestComplete(key1, 0)

    Assertions.assertFalse(result1)
  }

  @Test
  fun isDestCompleteMatchesStateIds() {
    store.set(key1, tenStateIdValue())
    Assertions.assertFalse(store.get(key1)!!.sourceComplete)

    val result1 = store.isDestComplete(key1, 8)
    Assertions.assertFalse(result1)

    // this case shouldn't happen but documenting it here
    val result2 = store.isDestComplete(key1, 11)
    Assertions.assertFalse(result2)

    val result3 = store.isDestComplete(key1, 10)
    Assertions.assertFalse(result3)

    store.markSourceComplete(key1)
    Assertions.assertTrue(store.get(key1)!!.sourceComplete)

    val result4 = store.isDestComplete(key1, 8)
    Assertions.assertFalse(result4)

    // this case shouldn't happen but documenting it here
    val result5 = store.isDestComplete(key1, 11)
    Assertions.assertFalse(result5)

    val result6 = store.isDestComplete(key1, 10)
    Assertions.assertTrue(result6)
  }

  companion object {
    @JvmStatic
    fun runStateValidTransitionMatrix(): Stream<Arguments> {
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
    fun runStateInvalidTransitionMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(ApiEnum.COMPLETE, ApiEnum.RUNNING),
        Arguments.of(ApiEnum.INCOMPLETE, ApiEnum.RUNNING),
        Arguments.of(ApiEnum.COMPLETE, ApiEnum.RATE_LIMITED),
        Arguments.of(ApiEnum.INCOMPLETE, ApiEnum.RATE_LIMITED),
      )
    }
  }

  object Fixtures {
    val key1 = StreamStatusKey(streamName = "test-stream-1", streamNamespace = null)
    val key2 = StreamStatusKey(streamName = "test-stream-2", streamNamespace = "test-namespace-1")
    val key3 = StreamStatusKey(streamName = "test-stream-3", streamNamespace = null)
    val key4 = StreamStatusKey(streamName = "test-stream-4", streamNamespace = "test-namespace-2")

    fun value(runState: StreamStatusRunState?) = StreamStatusValue(runState, 0, sourceComplete = false, streamEmpty = true)

    fun runningValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 0, sourceComplete = false, streamEmpty = true)

    fun completeValue() = StreamStatusValue(StreamStatusRunState.COMPLETE, 124, sourceComplete = false, streamEmpty = true)

    fun incompleteValue() = StreamStatusValue(StreamStatusRunState.INCOMPLETE, 246, sourceComplete = true, streamEmpty = true)

    fun zeroStateIdValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 0, sourceComplete = false, streamEmpty = false)

    fun tenStateIdValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 10, sourceComplete = false, streamEmpty = false)
  }
}
