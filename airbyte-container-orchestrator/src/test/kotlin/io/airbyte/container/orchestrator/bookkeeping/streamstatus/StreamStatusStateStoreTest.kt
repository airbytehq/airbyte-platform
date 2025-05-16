/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.completeValue
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.incompleteValue
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key1
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key2
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key3
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.key4
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.runningValue
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.tenStateIdValue
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.value
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusStateStoreTest.Fixtures.zeroStateIdValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.AbstractMap.SimpleEntry
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
    assertEquals(store.get(key1), runningValue())

    store.set(key1, completeValue())
    assertEquals(store.get(key1), completeValue())

    store.set(key1, incompleteValue())
    assertEquals(store.get(key1), incompleteValue())
  }

  @Test
  fun entriesReturnsEntries() {
    val store1 = StreamStatusStateStore()
    val store2 = StreamStatusStateStore()

    val value1 = runningValue()
    val value2 = runningValue()
    val value3 = runningValue()
    val value4 = runningValue()
    val value5 = runningValue()
    val value6 = runningValue()

    store1.set(key1, value1)
    store1.set(key2, value2)
    store1.set(key3, value3)
    store1.set(key4, value4)

    store2.set(key2, value5)
    store2.set(key4, value6)

    val expected1 = setOf(SimpleEntry(key1, value1), SimpleEntry(key2, value2), SimpleEntry(key3, value3), SimpleEntry(key4, value4))
    val expected2 = setOf(SimpleEntry(key2, value5), SimpleEntry(key4, value6))

    assertEquals(expected1, store1.entries())
    assertEquals(expected2, store2.entries())
  }

  @Test
  fun setRunStateHandlesNoValue() {
    assertNull(store.get(key1))

    val result1 = store.setRunState(key1, StreamStatusRunState.RUNNING)

    assertEquals(StreamStatusRunState.RUNNING, result1.runState)
    assertEquals(StreamStatusRunState.RUNNING, store.get(key1)!!.runState)
  }

  @Test
  fun setRunStateHandlesNoRunState() {
    val noRunState = StreamStatusValue(null, 0, sourceComplete = false, streamEmpty = false)
    store.set(key1, noRunState)
    assertNull(store.get(key1)!!.runState)

    val result1 = store.setRunState(key1, StreamStatusRunState.RUNNING)

    assertEquals(StreamStatusRunState.RUNNING, result1.runState)
    assertEquals(StreamStatusRunState.RUNNING, store.get(key1)!!.runState)
  }

  @ParameterizedTest
  @MethodSource("runStateValidTransitionMatrix")
  fun setRunStateAllowsValidStateTransitions(
    currentRunState: ApiEnum?,
    incomingRunState: ApiEnum,
  ) {
    store.set(key1, value(currentRunState))

    val transitioned = store.setRunState(key1, incomingRunState)

    assertEquals(incomingRunState, transitioned.runState)
    assertEquals(incomingRunState, store.get(key1)!!.runState)
  }

  @ParameterizedTest
  @MethodSource("runStateInvalidTransitionMatrix")
  fun setRunStateIgnoresInvalidStateTransitions(
    currentRunState: ApiEnum,
    incomingRunState: ApiEnum,
  ) {
    store.set(key1, value(currentRunState))

    val transitioned = store.setRunState(key1, incomingRunState)

    assertEquals(currentRunState, transitioned.runState)
    assertEquals(currentRunState, store.get(key1)!!.runState)
  }

  @Test
  fun setLatestStateIdHandlesNoValue() {
    assertNull(store.get(key1))

    val result1 = store.setLatestStateId(key1, 0)

    assertEquals(0, result1.latestStateId)
    assertEquals(0, store.get(key1)!!.latestStateId)
  }

  @Test
  fun setLatestStateIdHandlesNoStateId() {
    val noStateId = StreamStatusValue(null, null, sourceComplete = false, streamEmpty = false)
    store.set(key1, noStateId)
    assertNull(store.get(key1)!!.latestStateId)

    val result1 = store.setLatestStateId(key1, 0)

    assertEquals(0, result1.latestStateId)
    assertEquals(0, store.get(key1)!!.latestStateId)
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

    assertEquals(1, zeroToOne.latestStateId)
    assertEquals(1, store.get(key1)!!.latestStateId)

    assertEquals(20, zeroToTwenty.latestStateId)
    assertEquals(20, store.get(key2)!!.latestStateId)

    assertEquals(11, tenToEleven.latestStateId)
    assertEquals(11, store.get(key3)!!.latestStateId)

    assertEquals(124124, tenTo124124.latestStateId)
    assertEquals(124124, store.get(key4)!!.latestStateId)
  }

  @Test
  fun setLatestStateIdIgnoresNonmonotonicUpdates() {
    store.set(key1, tenStateIdValue())
    store.set(key2, tenStateIdValue())

    val tenToZero = store.setLatestStateId(key1, 0)
    val tenToNine = store.setLatestStateId(key2, 9)

    assertEquals(10, tenToZero.latestStateId)
    assertEquals(10, store.get(key1)!!.latestStateId)

    assertEquals(10, tenToNine.latestStateId)
    assertEquals(10, store.get(key2)!!.latestStateId)

    val tenToEight = store.setLatestStateId(key1, 8)
    val tenToOne = store.setLatestStateId(key2, 9)

    assertEquals(10, tenToEight.latestStateId)
    assertEquals(10, store.get(key1)!!.latestStateId)

    assertEquals(10, tenToOne.latestStateId)
    assertEquals(10, store.get(key2)!!.latestStateId)
  }

  @Test
  fun setLatestGlobalStateIdEnforcesMonotonicUpdates() {
    store.setLatestGlobalStateId(0)
    assertEquals(0, store.getLatestGlobalStateId())

    store.setLatestGlobalStateId(1)
    assertEquals(1, store.getLatestGlobalStateId())
    store.setLatestGlobalStateId(20)
    assertEquals(20, store.getLatestGlobalStateId())
    store.setLatestGlobalStateId(11)
    assertEquals(20, store.getLatestGlobalStateId())
    store.setLatestGlobalStateId(124124)
    assertEquals(124124, store.getLatestGlobalStateId())
    store.setLatestGlobalStateId(124123)
    assertEquals(124124, store.getLatestGlobalStateId())
    store.setLatestGlobalStateId(0)
    store.setLatestGlobalStateId(124123)
  }

  @Test
  fun setMetadataHandlesNoValue() {
    assertNull(store.get(key1))

    val metadata = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val result = store.setMetadata(key1, metadata)

    assertEquals(metadata, result.metadata)
    assertEquals(metadata, store.get(key1)!!.metadata)
  }

  @Test
  fun setMetadataHandlesNullMetadata() {
    val noMetadata = StreamStatusValue(null, 0, sourceComplete = false, streamEmpty = false, metadata = null)
    store.set(key1, noMetadata)
    assertNull(store.get(key1)!!.metadata)

    val metadata = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val result = store.setMetadata(key1, metadata)

    assertEquals(metadata, result.metadata)
    assertEquals(metadata, store.get(key1)!!.metadata)
  }

  @Test
  fun setMetadataReplacesMetadata() {
    val metadata1 = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val value = StreamStatusValue(null, 0, sourceComplete = false, streamEmpty = false, metadata = metadata1)
    store.set(key1, value)
    assertEquals(metadata1, store.get(key1)!!.metadata)

    val metadata2 = StreamStatusRateLimitedMetadata(quotaReset = 123)
    val result = store.setMetadata(key1, metadata2)

    assertEquals(metadata2, result.metadata)
    assertEquals(metadata2, store.get(key1)!!.metadata)
  }

  @Test
  fun markSourceCompleteHandlesNoValue() {
    assertNull(store.get(key1))

    val result1 = store.markSourceComplete(key1)

    assertTrue(result1.sourceComplete)
    assertTrue(store.get(key1)!!.sourceComplete)
  }

  @Test
  fun markSourceCompleteIsIdempotentAndIsolated() {
    store.set(key1, runningValue())
    store.set(key2, runningValue())
    assertFalse(store.get(key1)!!.sourceComplete)
    assertFalse(store.get(key2)!!.sourceComplete)

    store.markSourceComplete(key1)
    assertTrue(store.get(key1)!!.sourceComplete)
    assertFalse(store.get(key2)!!.sourceComplete)

    store.markSourceComplete(key1)
    assertTrue(store.get(key1)!!.sourceComplete)
    assertFalse(store.get(key2)!!.sourceComplete)
  }

  @Test
  fun markStreamNotEmptyHandlesNoValue() {
    assertNull(store.get(key1))

    val result1 = store.markStreamNotEmpty(key1)

    assertFalse(result1.streamEmpty)
    assertFalse(store.get(key1)!!.streamEmpty)
  }

  @Test
  fun markStreamNotEmptyIsIdempotentAndIsolated() {
    store.set(key1, runningValue())
    store.set(key2, runningValue())
    assertTrue(store.get(key1)!!.streamEmpty)
    assertTrue(store.get(key2)!!.streamEmpty)

    store.markStreamNotEmpty(key1)
    assertFalse(store.get(key1)!!.streamEmpty)
    assertTrue(store.get(key2)!!.streamEmpty)

    store.markStreamNotEmpty(key1)
    assertFalse(store.get(key1)!!.streamEmpty)
    assertTrue(store.get(key2)!!.streamEmpty)
  }

  @Test
  fun isStreamCompleteHandlesNoValue() {
    assertNull(store.get(key1))

    val result1 = store.isStreamComplete(key1, 0)

    assertFalse(result1)
  }

  @Test
  fun isStreamCompleteMatchesStateIds() {
    store.set(key1, tenStateIdValue())
    assertFalse(store.get(key1)!!.sourceComplete)

    val result1 = store.isStreamComplete(key1, 8)
    assertFalse(result1)

    // this case shouldn't happen but documenting it here
    val result2 = store.isStreamComplete(key1, 11)
    assertFalse(result2)

    val result3 = store.isStreamComplete(key1, 10)
    assertFalse(result3)

    store.markSourceComplete(key1)
    assertTrue(store.get(key1)!!.sourceComplete)

    val result4 = store.isStreamComplete(key1, 8)
    assertFalse(result4)

    // this case shouldn't happen but documenting it here
    val result5 = store.isStreamComplete(key1, 11)
    assertFalse(result5)

    val result6 = store.isStreamComplete(key1, 10)
    assertTrue(result6)
  }

  @Test
  fun isGlobalCompleteChecksAllStreamsSourceComplete() {
    val id = 1
    store.setLatestGlobalStateId(id)

    store.set(key1, completeValue())
    store.set(key2, completeValue())
    store.set(key3, runningValue())

    assertFalse(store.isGlobalComplete(id))

    store.markSourceComplete(key3)
    store.set(key4, runningValue())

    assertFalse(store.isGlobalComplete(id))

    store.markSourceComplete(key4)

    assertTrue(store.isGlobalComplete(id))
  }

  @Test
  fun isGlobalCompleteMatchesLatestGlobalStateId() {
    store.set(key1, completeValue())
    store.set(key2, completeValue())
    store.set(key3, completeValue())

    val id1 = 121
    val id2 = 122
    val id3 = 123
    val id4 = 124
    store.setLatestGlobalStateId(id1)
    store.setLatestGlobalStateId(id2)
    store.setLatestGlobalStateId(id3)
    store.setLatestGlobalStateId(id4)

    assertFalse(store.isGlobalComplete(id1))
    assertFalse(store.isGlobalComplete(id2))
    assertFalse(store.isGlobalComplete(id3))

    assertTrue(store.isGlobalComplete(id4))
  }

  companion object {
    @JvmStatic
    fun runStateValidTransitionMatrix(): Stream<Arguments> =
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
    fun runStateInvalidTransitionMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(ApiEnum.COMPLETE, ApiEnum.RUNNING),
        Arguments.of(ApiEnum.INCOMPLETE, ApiEnum.RUNNING),
        Arguments.of(ApiEnum.COMPLETE, ApiEnum.RATE_LIMITED),
        Arguments.of(ApiEnum.INCOMPLETE, ApiEnum.RATE_LIMITED),
      )
  }

  object Fixtures {
    val key1 = StreamStatusKey(streamName = "test-stream-1", streamNamespace = null)
    val key2 = StreamStatusKey(streamName = "test-stream-2", streamNamespace = "test-namespace-1")
    val key3 = StreamStatusKey(streamName = "test-stream-3", streamNamespace = null)
    val key4 = StreamStatusKey(streamName = "test-stream-4", streamNamespace = "test-namespace-2")

    fun value(runState: StreamStatusRunState?) = StreamStatusValue(runState, 0, sourceComplete = false, streamEmpty = true)

    fun runningValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 0, sourceComplete = false, streamEmpty = true)

    fun completeValue() = StreamStatusValue(StreamStatusRunState.COMPLETE, 124, sourceComplete = true, streamEmpty = false)

    fun incompleteValue() = StreamStatusValue(StreamStatusRunState.INCOMPLETE, 246, sourceComplete = false, streamEmpty = false)

    fun zeroStateIdValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 0, sourceComplete = false, streamEmpty = false)

    fun tenStateIdValue() = StreamStatusValue(StreamStatusRunState.RUNNING, 10, sourceComplete = false, streamEmpty = false)
  }

  @Test
  fun `test all combinations of states`() {
    val states = ApiEnum.entries.toTypedArray()

    // Create a table to test all possible combinations
    for (current in states) {
      for (incoming in states) {
        val expected =
          when (current to incoming) {
            ApiEnum.RUNNING to ApiEnum.COMPLETE,
            ApiEnum.RUNNING to ApiEnum.INCOMPLETE,
            ApiEnum.RUNNING to ApiEnum.RATE_LIMITED,
            ApiEnum.RATE_LIMITED to ApiEnum.RUNNING,
            ApiEnum.RATE_LIMITED to ApiEnum.INCOMPLETE,
            ApiEnum.RATE_LIMITED to ApiEnum.COMPLETE,
            -> incoming

            else -> current
          }

        val result = store.resolveRunState(current, incoming)
        assertEquals(
          expected,
          result,
          "Failed for current=$current, incoming=$incoming",
        )
      }
    }
  }

  @Test
  fun `when current is RUNNING and incoming is COMPLETE, should return COMPLETE`() {
    val result = store.resolveRunState(ApiEnum.RUNNING, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is RUNNING and incoming is INCOMPLETE, should return INCOMPLETE`() {
    val result = store.resolveRunState(ApiEnum.RUNNING, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is RUNNING and incoming is RATE_LIMITED, should return RATE_LIMITED`() {
    val result = store.resolveRunState(ApiEnum.RUNNING, ApiEnum.RATE_LIMITED)
    assertEquals(ApiEnum.RATE_LIMITED, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is RUNNING, should return RUNNING`() {
    val result = store.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.RUNNING)
    assertEquals(ApiEnum.RUNNING, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is INCOMPLETE, should return INCOMPLETE`() {
    val result = store.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is COMPLETE, should return COMPLETE`() {
    val result = store.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is COMPLETE and incoming is RUNNING, should return COMPLETE`() {
    val result = store.resolveRunState(ApiEnum.COMPLETE, ApiEnum.RUNNING)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is COMPLETE and incoming is INCOMPLETE, should return COMPLETE`() {
    val result = store.resolveRunState(ApiEnum.COMPLETE, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is INCOMPLETE and incoming is RUNNING, should return INCOMPLETE`() {
    val result = store.resolveRunState(ApiEnum.INCOMPLETE, ApiEnum.RUNNING)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is INCOMPLETE and incoming is COMPLETE, should return INCOMPLETE`() {
    val result = store.resolveRunState(ApiEnum.INCOMPLETE, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }
//
//  @Test
//  fun `when current is RUNNING and incoming is FAILED, should return RUNNING`() {
//    val result = store.resolveRunState(ApiEnum.RUNNING, ApiEnum.)
//    assertEquals(ApiEnum.RUNNING, result)
//  }

  @Test
  fun `when current is PENDING and incoming is RUNNING, should return PENDING`() {
    val result = store.resolveRunState(ApiEnum.PENDING, ApiEnum.RUNNING)
    assertEquals(ApiEnum.PENDING, result)
  }

  @Test
  fun `when current is RUNNING and incoming is RUNNING, should return RUNNING`() {
    val result = store.resolveRunState(ApiEnum.RUNNING, ApiEnum.RUNNING)
    assertEquals(ApiEnum.RUNNING, result)
  }

  @Test
  fun `when current is COMPLETE and incoming is COMPLETE, should return COMPLETE`() {
    val result = store.resolveRunState(ApiEnum.COMPLETE, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is INCOMPLETE and incoming is INCOMPLETE, should return INCOMPLETE`() {
    val result = store.resolveRunState(ApiEnum.INCOMPLETE, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is RATE_LIMITED, should return RATE_LIMITED`() {
    val result = store.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.RATE_LIMITED)
    assertEquals(ApiEnum.RATE_LIMITED, result)
  }
}
