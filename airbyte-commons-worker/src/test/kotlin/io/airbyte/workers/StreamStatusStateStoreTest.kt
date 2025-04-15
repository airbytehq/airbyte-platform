/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusStateStore
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

class StreamStatusStateStoreTest {
  private val stateStore = StreamStatusStateStore()

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

        val result = stateStore.resolveRunState(current, incoming)
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
    val result = stateStore.resolveRunState(ApiEnum.RUNNING, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is RUNNING and incoming is INCOMPLETE, should return INCOMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.RUNNING, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is RUNNING and incoming is RATE_LIMITED, should return RATE_LIMITED`() {
    val result = stateStore.resolveRunState(ApiEnum.RUNNING, ApiEnum.RATE_LIMITED)
    assertEquals(ApiEnum.RATE_LIMITED, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is RUNNING, should return RUNNING`() {
    val result = stateStore.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.RUNNING)
    assertEquals(ApiEnum.RUNNING, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is INCOMPLETE, should return INCOMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is COMPLETE, should return COMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is COMPLETE and incoming is RUNNING, should return COMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.COMPLETE, ApiEnum.RUNNING)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is COMPLETE and incoming is INCOMPLETE, should return COMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.COMPLETE, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is INCOMPLETE and incoming is RUNNING, should return INCOMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.INCOMPLETE, ApiEnum.RUNNING)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is INCOMPLETE and incoming is COMPLETE, should return INCOMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.INCOMPLETE, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }
//
//  @Test
//  fun `when current is RUNNING and incoming is FAILED, should return RUNNING`() {
//    val result = stateStore.resolveRunState(ApiEnum.RUNNING, ApiEnum.)
//    assertEquals(ApiEnum.RUNNING, result)
//  }

  @Test
  fun `when current is PENDING and incoming is RUNNING, should return PENDING`() {
    val result = stateStore.resolveRunState(ApiEnum.PENDING, ApiEnum.RUNNING)
    assertEquals(ApiEnum.PENDING, result)
  }

  @Test
  fun `when current is RUNNING and incoming is RUNNING, should return RUNNING`() {
    val result = stateStore.resolveRunState(ApiEnum.RUNNING, ApiEnum.RUNNING)
    assertEquals(ApiEnum.RUNNING, result)
  }

  @Test
  fun `when current is COMPLETE and incoming is COMPLETE, should return COMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.COMPLETE, ApiEnum.COMPLETE)
    assertEquals(ApiEnum.COMPLETE, result)
  }

  @Test
  fun `when current is INCOMPLETE and incoming is INCOMPLETE, should return INCOMPLETE`() {
    val result = stateStore.resolveRunState(ApiEnum.INCOMPLETE, ApiEnum.INCOMPLETE)
    assertEquals(ApiEnum.INCOMPLETE, result)
  }

  @Test
  fun `when current is RATE_LIMITED and incoming is RATE_LIMITED, should return RATE_LIMITED`() {
    val result = stateStore.resolveRunState(ApiEnum.RATE_LIMITED, ApiEnum.RATE_LIMITED)
    assertEquals(ApiEnum.RATE_LIMITED, result)
  }
}
