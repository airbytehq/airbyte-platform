package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.completeValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.incompleteValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key1
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key2
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key3
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key4
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.runningValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.tenStateIdValue
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.zeroStateIdValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

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

  @Test
  fun setRunStateAllowsValidStateTransitions() {
    store.set(key1, runningValue())
    store.set(key2, runningValue())

    val runningToComplete = store.setRunState(key1, StreamStatusRunState.COMPLETE)
    val runningToIncomplete = store.setRunState(key2, StreamStatusRunState.INCOMPLETE)
    val nullToComplete = store.setRunState(key3, StreamStatusRunState.COMPLETE)
    val nullToIncomplete = store.setRunState(key4, StreamStatusRunState.INCOMPLETE)

    Assertions.assertEquals(StreamStatusRunState.COMPLETE, runningToComplete.runState)
    Assertions.assertEquals(StreamStatusRunState.COMPLETE, store.get(key1)!!.runState)

    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, runningToIncomplete.runState)
    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, store.get(key2)!!.runState)

    Assertions.assertEquals(StreamStatusRunState.COMPLETE, nullToComplete.runState)
    Assertions.assertEquals(StreamStatusRunState.COMPLETE, store.get(key3)!!.runState)

    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, nullToIncomplete.runState)
    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, store.get(key4)!!.runState)
  }

  @Test
  fun setRunStateIgnoresInvalidStateTransitions() {
    store.set(key1, completeValue())
    store.set(key2, incompleteValue())
    store.set(key3, completeValue())
    store.set(key4, incompleteValue())

    val completeToRunning = store.setRunState(key1, StreamStatusRunState.RUNNING)
    val incompleteToRunning = store.setRunState(key2, StreamStatusRunState.RUNNING)
    val completeToIncomplete = store.setRunState(key3, StreamStatusRunState.INCOMPLETE)
    val incompleteToComplete = store.setRunState(key4, StreamStatusRunState.COMPLETE)

    Assertions.assertEquals(StreamStatusRunState.COMPLETE, completeToRunning.runState)
    Assertions.assertEquals(StreamStatusRunState.COMPLETE, store.get(key1)!!.runState)

    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, incompleteToRunning.runState)
    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, store.get(key2)!!.runState)

    Assertions.assertEquals(StreamStatusRunState.COMPLETE, completeToIncomplete.runState)
    Assertions.assertEquals(StreamStatusRunState.COMPLETE, store.get(key3)!!.runState)

    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, incompleteToComplete.runState)
    Assertions.assertEquals(StreamStatusRunState.INCOMPLETE, store.get(key4)!!.runState)
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

  object Fixtures {
    val key1 = StreamStatusKey(streamName = "test-stream-1", streamNamespace = null)
    val key2 = StreamStatusKey(streamName = "test-stream-2", streamNamespace = "test-namespace-1")
    val key3 = StreamStatusKey(streamName = "test-stream-3", streamNamespace = null)
    val key4 = StreamStatusKey(streamName = "test-stream-4", streamNamespace = "test-namespace-2")

    fun runningValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 0, sourceComplete = false, streamEmpty = true)

    fun completeValue() = StreamStatusValue(StreamStatusRunState.COMPLETE, 124, sourceComplete = false, streamEmpty = true)

    fun incompleteValue() = StreamStatusValue(StreamStatusRunState.INCOMPLETE, 246, sourceComplete = true, streamEmpty = true)

    fun zeroStateIdValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 0, sourceComplete = false, streamEmpty = false)

    fun tenStateIdValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 10, sourceComplete = false, streamEmpty = false)
  }
}
