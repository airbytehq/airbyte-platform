/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.state

import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.config.State
import io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.StreamDescriptor
import jakarta.inject.Named
import jakarta.inject.Singleton

interface StateAggregator {
  fun ingest(stateMessage: AirbyteStateMessage)

  fun ingest(stateAggregator: StateAggregator)

  fun getAggregated(): State

  fun isEmpty(): Boolean

  fun clear()
}

/**
 * Default state aggregator that detects which type of state is being used and aggregates appropriately.
 */
@Singleton
@Named("stateAggregator")
class DefaultStateAggregator(
  @Named("streamStateAggregator") private val streamStateAggregator: StreamStateAggregator,
  @Named("singleStateAggregator") private val singleStateAggregator: SingleStateAggregator,
) : StateAggregator {
  private var stateType: AirbyteStateType? = null

  override fun ingest(stateMessage: AirbyteStateMessage) {
    checkTypeOrSet(stateMessage.type)
    getStateAggregator().ingest(stateMessage)
  }

  override fun ingest(stateAggregator: StateAggregator) {
    if (
      stateAggregator is DefaultStateAggregator && (stateType == null || stateAggregator.stateType == null || stateType == stateAggregator.stateType)
    ) {
      singleStateAggregator.ingest(stateAggregator.singleStateAggregator)
      streamStateAggregator.ingest(stateAggregator.streamStateAggregator)

      // Since we allowed stateType to be null, make sure it is set to a value correct value
      if (stateType == null) {
        stateType = stateAggregator.stateType
      }
    } else {
      fun prettyPrintStateAggregator(aggregator: StateAggregator): String =
        if (aggregator is DefaultStateAggregator) {
          "DefaultStateAggregator<${aggregator.stateType}>"
        } else {
          aggregator.javaClass.getName()
        }

      throw IllegalArgumentException(
        "Got an incompatible StateAggregator: " + prettyPrintStateAggregator(stateAggregator) +
          ", expected " + prettyPrintStateAggregator(this),
      )
    }
  }

  override fun getAggregated(): State = getStateAggregator().getAggregated()

  override fun isEmpty(): Boolean = stateType == null || getStateAggregator().isEmpty()

  override fun clear() {
    getStateAggregator().clear()
  }

  /** Return the state aggregator that match the state type. */
  private fun getStateAggregator(): StateAggregator =
    when (stateType) {
      AirbyteStateType.STREAM -> streamStateAggregator
      AirbyteStateType.GLOBAL, AirbyteStateType.LEGACY -> singleStateAggregator
      null -> throw IllegalArgumentException("StateType must not be null")
    }

  /**
   * We cannot have 2 different state types given to the same instance of this class. This method set
   * the type if it is not. If the state type doesn't exist in the message, it is set to LEGACY
   */
  private fun checkTypeOrSet(inputStateType: AirbyteStateType?) {
    val validatedStateType: AirbyteStateType = inputStateType ?: AirbyteStateType.LEGACY
    if (stateType == null) {
      stateType = validatedStateType
    }
    if (stateType != validatedStateType) {
      throw IllegalArgumentException("Input state type $validatedStateType does not match the aggregator's current state type $stateType")
    }
  }
}

@Singleton
@Named("singleStateAggregator")
class SingleStateAggregator : StateAggregator {
  private var state: AirbyteStateMessage? = null

  @Trace(operationName = WORKER_OPERATION_NAME)
  override fun ingest(stateMessage: AirbyteStateMessage) {
    state = stateMessage
  }

  override fun ingest(stateAggregator: StateAggregator) {
    when (stateAggregator) {
      is SingleStateAggregator -> stateAggregator.state?.let { ingest(it) }
      else -> throw IllegalArgumentException("Incompatible StateAggregator: ${stateAggregator::class.simpleName}, expected SingleStateAggregator")
    }
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  override fun getAggregated(): State {
    val localState = state

    return when {
      localState == null -> throw IllegalArgumentException("State must not be null")
      localState.type == null || localState.type == AirbyteStateType.LEGACY -> State().withState(localState.data)
      else -> {
        /*
         * The destination emit a Legacy state in order to be retro-compatible with old platform. If we are
         * running this code, we know that the platform has been upgraded, and we can thus discard the legacy
         * state. Keeping the legacy state is causing issue because of its size
         * (https://github.com/airbytehq/oncall/issues/731)
         */
        localState.data = null
        State().withState(Jsons.jsonNode(listOf(localState)))
      }
    }
  }

  override fun isEmpty(): Boolean = state == null

  override fun clear() {
    state = null
  }
}

@Singleton
@Named("streamStateAggregator")
class StreamStateAggregator : StateAggregator {
  private val aggregatedState = mutableMapOf<StreamDescriptor, AirbyteStateMessage>()

  @Trace(operationName = WORKER_OPERATION_NAME)
  override fun ingest(stateMessage: AirbyteStateMessage) {
    /*
     * The destination emit a Legacy state in order to be retro-compatible with old platform. If we are
     * running this code, we know that the platform has been upgraded, and we can thus discard the legacy
     * state. Keeping the legacy state is causing issue because of its size
     * (https://github.com/airbytehq/oncall/issues/731)
     */
    stateMessage.data = null
    aggregatedState[stateMessage.stream.streamDescriptor] = stateMessage
  }

  override fun ingest(stateAggregator: StateAggregator) {
    when (stateAggregator) {
      is StreamStateAggregator -> stateAggregator.aggregatedState.forEach { (_, msg) -> ingest(msg) }
      else -> throw IllegalArgumentException("Incompatible StateAggregator: ${stateAggregator::class.simpleName}, expected StreamStateAggregator")
    }
  }

  override fun getAggregated(): State = State().withState(Jsons.jsonNode(aggregatedState.values))

  override fun isEmpty(): Boolean = aggregatedState.isEmpty()

  override fun clear() {
    aggregatedState.clear()
  }
}
