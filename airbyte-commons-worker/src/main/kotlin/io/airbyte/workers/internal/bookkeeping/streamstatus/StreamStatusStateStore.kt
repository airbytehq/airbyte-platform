package io.airbyte.workers.internal.bookkeeping.streamstatus

import io.airbyte.api.client.model.generated.StreamStatusRunState.COMPLETE
import io.airbyte.api.client.model.generated.StreamStatusRunState.INCOMPLETE
import io.airbyte.api.client.model.generated.StreamStatusRunState.RUNNING
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum

private val logger = KotlinLogging.logger {}

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

  fun get(key: StreamStatusKey) = store[key]

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

    store[key] =
      if (value == null) {
        StreamStatusValue(runState = runState)
      } else if (value.runState == null) {
        StreamStatusValue(runState = runState)
      } else {
        StreamStatusValue(runState = resolveRunState(value.runState!!, runState))
      }

    return store[key]!!
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

    if (value == null) {
      store[key] = StreamStatusValue(streamEmpty = false)
    } else {
      value.streamEmpty = false
    }

    return store[key]!!
  }

  fun isDestComplete(
    key: StreamStatusKey,
    destStateId: Int,
  ): Boolean {
    val value = store[key] ?: return false

    return value.sourceComplete && value.latestStateId == destStateId
  }

  private fun resolveRunState(
    current: ApiEnum,
    incoming: ApiEnum,
  ): ApiEnum {
    return when (current to incoming) {
      RUNNING to COMPLETE, RUNNING to INCOMPLETE -> {
        incoming
      }
      else -> {
        current
      }
    }
  }
}
