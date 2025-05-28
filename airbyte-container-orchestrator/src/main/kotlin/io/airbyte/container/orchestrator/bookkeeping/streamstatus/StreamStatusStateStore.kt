/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRunState.COMPLETE
import io.airbyte.api.client.model.generated.StreamStatusRunState.INCOMPLETE
import io.airbyte.api.client.model.generated.StreamStatusRunState.RATE_LIMITED
import io.airbyte.api.client.model.generated.StreamStatusRunState.RUNNING
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

/**
 * Stores the per stream status state and enforces valid state transitions.
 *
 * Invalid state transitions are ignored. Methods always return the current value.
 *
 * State layer.
 */
@Singleton
class StreamStatusStateStore {
  private val store: MutableMap<StreamStatusKey, StreamStatusValue> = ConcurrentHashMap()

  private var latestGlobalStateId = 0

  fun get(key: StreamStatusKey) = store[key]

  fun getLatestGlobalStateId(): Int = latestGlobalStateId

  fun entries(): Set<Map.Entry<StreamStatusKey, StreamStatusValue>> = store.entries

  fun set(
    key: StreamStatusKey,
    value: StreamStatusValue,
  ): StreamStatusValue {
    store[key] = value

    return store[key]!!
  }

  fun setRunState(
    key: StreamStatusKey,
    runState: ApiEnum,
  ): StreamStatusValue {
    val value = store[key]
    val currentRunState = value?.runState

    // Determine new run state based on current state
    val newRunState =
      when {
        currentRunState == null -> runState
        else -> resolveRunState(currentRunState, runState)
      }

    // Create new status value
    val streamStatusValue = StreamStatusValue(runState = newRunState)

    // Only update store if run state has changed
    if (currentRunState != newRunState) {
      store[key] = streamStatusValue
    }

    return streamStatusValue
  }

  fun setLatestStateId(
    key: StreamStatusKey,
    stateId: Int,
  ): StreamStatusValue {
    val value = store[key]

    if (value == null) {
      store[key] = StreamStatusValue(latestStateId = stateId)
    } else if (value.latestStateId == null) {
      value.latestStateId = stateId
    } else if (value.latestStateId!! < stateId) {
      value.latestStateId = stateId
    }

    return store[key]!!
  }

  fun setLatestGlobalStateId(stateId: Int): Int {
    if (latestGlobalStateId < stateId) {
      latestGlobalStateId = stateId
    }

    return latestGlobalStateId
  }

  fun setMetadata(
    key: StreamStatusKey,
    metadata: StreamStatusRateLimitedMetadata?,
  ): StreamStatusValue {
    val value = store[key]

    if (value == null) {
      store[key] = StreamStatusValue(metadata = metadata)
    } else {
      value.metadata = metadata
    }

    return store[key]!!
  }

  fun markSourceComplete(key: StreamStatusKey): StreamStatusValue {
    val value = store[key]

    if (value == null) {
      store[key] = StreamStatusValue(sourceComplete = true)
    } else {
      value.sourceComplete = true
    }

    return store[key]!!
  }

  fun markStreamNotEmpty(key: StreamStatusKey): StreamStatusValue {
    val value = store[key]

    return when {
      value == null -> {
        val newValue = StreamStatusValue(streamEmpty = false)
        store[key] = newValue
        newValue
      }
      else -> {
        value.streamEmpty = false
        value
      }
    }
  }

  fun isStreamComplete(
    key: StreamStatusKey,
    destStateId: Int,
  ): Boolean {
    val value = store[key] ?: return false

    return value.sourceComplete && value.latestStateId == destStateId
  }

  fun isRateLimited(key: StreamStatusKey): Boolean {
    val value = store[key] ?: return false

    val runState = value.runState ?: false

    return runState == RATE_LIMITED
  }

  fun isGlobalComplete(destStateId: Int): Boolean {
    val sourceComplete = store.values.all { it.sourceComplete }
    val destComplete = destStateId == latestGlobalStateId

    return sourceComplete && destComplete
  }

  internal fun resolveRunState(
    current: ApiEnum,
    incoming: ApiEnum,
  ): ApiEnum =
    if (current === RUNNING) {
      if (incoming === COMPLETE ||
        incoming === INCOMPLETE ||
        incoming === RATE_LIMITED
      ) {
        incoming
      } else {
        current
      }
    } else if (current === RATE_LIMITED) {
      if (incoming === RUNNING ||
        incoming === COMPLETE ||
        incoming === INCOMPLETE
      ) {
        incoming
      } else {
        current
      }
    } else {
      current
    }
}
