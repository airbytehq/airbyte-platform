/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.State
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.annotation.Nullable
import java.util.Optional

/**
 * State Message helpers.
 */
object StateMessageHelper {
  private val log = KotlinLogging.logger {}

  /**
   * This a takes a json blob state and tries return either a legacy state in the format of a json
   * object or a state message with the new format which is a list of airbyte state message.
   *
   * @param state - a blob representing the state
   * @return An optional state wrapper, if there is no state an empty optional will be returned
   */
  @JvmStatic
  fun getTypedState(state: JsonNode?): Optional<StateWrapper> {
    if (state == null) {
      return Optional.empty()
    } else {
      val stateMessages: List<AirbyteStateMessage>
      try {
        stateMessages = Jsons.`object`(state, AirbyteStateMessageListTypeReference())
      } catch (e: IllegalArgumentException) {
        log.warn { "Failed to convert state, falling back to legacy state wrapper" }
        return Optional.of(getLegacyStateWrapper(state))
      }
      if (stateMessages.isEmpty()) {
        return Optional.empty()
      }

      if (stateMessages.size == 1) {
        return if (stateMessages[0].type == null) {
          Optional.of(getLegacyStateWrapper(state))
        } else {
          when (stateMessages[0].type) {
            AirbyteStateType.GLOBAL -> {
              Optional.of(provideGlobalState(stateMessages[0]))
            }

            AirbyteStateType.STREAM -> {
              Optional.of(provideStreamState(stateMessages))
            }

            AirbyteStateType.LEGACY -> {
              Optional.of(getLegacyStateWrapper(stateMessages[0].data))
            }

            else -> {
              // Should not be reachable.
              throw IllegalStateException("Unexpected state type")
            }
          }
        }
      } else {
        if (stateMessages.stream().allMatch { stateMessage: AirbyteStateMessage -> stateMessage.type == AirbyteStateType.STREAM }) {
          return Optional.of(provideStreamState(stateMessages))
        }
        if (stateMessages.stream().allMatch { stateMessage: AirbyteStateMessage -> stateMessage.type == null }) {
          return Optional.of(getLegacyStateWrapper(state))
        }

        throw IllegalStateException("Unexpected state blob, the state contains either multiple global or conflicting state type.")
      }
    }
  }

  /**
   * Converts a StateWrapper to a State
   *
   * LegacyStates are directly serialized into the state. GlobalStates and StreamStates are serialized
   * as a list of AirbyteStateMessage in the state attribute.
   *
   * @param stateWrapper the StateWrapper to convert
   * @return the Converted State
   */
  @JvmStatic
  fun getState(stateWrapper: StateWrapper): State =
    when (stateWrapper.stateType) {
      StateType.LEGACY -> State().withState(stateWrapper.legacyState)
      StateType.STREAM -> State().withState(Jsons.jsonNode(stateWrapper.stateMessages))
      StateType.GLOBAL -> State().withState(Jsons.jsonNode(java.util.List.of(stateWrapper.global)))
      else -> throw RuntimeException("Unexpected StateType " + stateWrapper.stateType)
    }

  @JvmStatic
  fun isMigration(
    currentStateType: StateType,
    previousState: Optional<StateWrapper>,
  ): Boolean = previousState.isPresent && isMigration(currentStateType, previousState.get().stateType)

  fun isMigration(
    currentStateType: StateType,
    @Nullable previousStateType: StateType,
  ): Boolean = previousStateType == StateType.LEGACY && currentStateType != StateType.LEGACY

  private fun provideGlobalState(stateMessages: AirbyteStateMessage): StateWrapper =
    StateWrapper()
      .withStateType(StateType.GLOBAL)
      .withGlobal(stateMessages)

  /**
   * This is returning a wrapped state, it assumes that the state messages are ordered.
   *
   * @param stateMessages - an ordered list of state message
   * @return a wrapped state
   */
  private fun provideStreamState(stateMessages: List<AirbyteStateMessage>): StateWrapper =
    StateWrapper()
      .withStateType(StateType.STREAM)
      .withStateMessages(stateMessages)

  private fun getLegacyStateWrapper(state: JsonNode): StateWrapper =
    StateWrapper()
      .withStateType(StateType.LEGACY)
      .withLegacyState(state)

  private class AirbyteStateMessageListTypeReference : TypeReference<List<AirbyteStateMessage>>()
}
