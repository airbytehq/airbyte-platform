/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ResetSourceConfiguration
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkerSourceConfig
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.config.helpers.StateMessageHelper
import io.airbyte.metrics.lib.ApmTraceConstants
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.workers.testutils.AirbyteMessageUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.util.LinkedList
import java.util.Optional
import java.util.Queue
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import java.util.stream.Collectors

private val logger = KotlinLogging.logger {}

/**
 * This source will never emit any messages. It can be used in cases where that is helpful (hint:
 * reset connection jobs).
 */
class EmptyAirbyteSource(
  hasCustomNamespace: Boolean,
) : AirbyteSource {
  private val hasCustomNamespace: AtomicBoolean
  private val hasEmittedState: AtomicBoolean
  private val hasEmittedStreamStatus: AtomicBoolean
  private val streamsToReset: Queue<StreamDescriptor> = LinkedList()
  private val perStreamMessages: Queue<AirbyteMessage> = LinkedList()
  private var isStarted = false
  private var stateWrapper: Optional<StateWrapper> = Optional.empty<StateWrapper>()

  init {
    this.hasCustomNamespace = AtomicBoolean(hasCustomNamespace)
    hasEmittedState = AtomicBoolean()
    hasEmittedStreamStatus = AtomicBoolean()
  }

  @Trace(operationName = ApmTraceConstants.WORKER_OPERATION_NAME)
  @Throws(Exception::class)
  override fun start(
    sourceConfig: WorkerSourceConfig,
    jobRoot: Path?,
    connectionId: UUID?,
  ) {
    if (sourceConfig.sourceConnectionConfiguration != null) {
      val resetSourceConfiguration = parseResetSourceConfigurationAndLogError(sourceConfig)
      streamsToReset.addAll(resetSourceConfiguration.streamsToReset)

      if (!streamsToReset.isEmpty()) {
        if (sourceConfig.state != null) {
          stateWrapper = StateMessageHelper.getTypedState(sourceConfig.state.state)

          if (stateWrapper.isPresent &&
            stateWrapper.get().stateType == StateType.LEGACY &&
            !resettingAllCatalogStreams(sourceConfig)
          ) {
            logger.error { "The state a legacy one but we are trying to do a partial update, this is not supported." }
            throw IllegalStateException("Try to perform a partial reset on a legacy state")
          }
        }
      }
    }
    isStarted = true
  }

  @get:Trace(operationName = ApmTraceConstants.WORKER_OPERATION_NAME)
  override val isFinished: Boolean
    // always finished. it has no data to send.
    get() = hasEmittedState.get() && (hasEmittedStreamStatus.get() || hasCustomNamespace.get())

  @get:Trace(operationName = ApmTraceConstants.WORKER_OPERATION_NAME)
  override val exitValue: Int
    get() = 0

  @Trace(operationName = ApmTraceConstants.WORKER_OPERATION_NAME)
  override fun attemptRead(): Optional<AirbyteMessage> {
    check(isStarted) { "The empty source has not been started." }

    if (stateWrapper.isPresent) {
      if (stateWrapper.get().stateType == StateType.STREAM) {
        return emitPerStreamState()
      } else if (stateWrapper.get().stateType == StateType.GLOBAL) {
        return emitGlobalState()
      }
      val isLegacyStateValueAbsent =
        stateWrapper.get().legacyState == null ||
          stateWrapper.get().legacyState.isNull ||
          stateWrapper.get().legacyState.isEmpty
      return emitLegacyState(!isLegacyStateValueAbsent)
    }
    return emitLegacyState(false)
  }

  @Trace(operationName = ApmTraceConstants.WORKER_OPERATION_NAME)
  @Throws(Exception::class)
  override fun close() {
    // no op.
  }

  @Trace(operationName = ApmTraceConstants.WORKER_OPERATION_NAME)
  @Throws(Exception::class)
  override fun cancel() {
    // no op.
  }

  private fun emitPerStreamState(): Optional<AirbyteMessage> {
    if (streamsToReset.isEmpty() && perStreamMessages.isEmpty()) {
      hasEmittedState.compareAndSet(false, true)
      hasEmittedStreamStatus.compareAndSet(false, true)
      return Optional.empty<AirbyteMessage>()
    }

    if (perStreamMessages.isEmpty()) {
      // Per stream, we emit one 'started', one null state and one 'complete' message.
      // Since there's only 1 state message we move directly from 'started' to 'complete'.
      val s: io.airbyte.protocol.models.v0.StreamDescriptor = streamsToReset.poll().toProtocol()
      if (!hasCustomNamespace.get()) {
        perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED))
      }
      perStreamMessages.add(buildNullStreamStateMessage(s))
      if (!hasCustomNamespace.get()) {
        perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE))
      }
    }

    val message = perStreamMessages.poll()
    return Optional.ofNullable<AirbyteMessage>(message)
  }

  private fun emitGlobalState(): Optional<AirbyteMessage> {
    if (!hasEmittedState.get()) {
      hasEmittedState.compareAndSet(false, true)
      return Optional.of<AirbyteMessage>(getNullGlobalMessage(streamsToReset, stateWrapper.get().global))
    }

    return emitStreamResetTraceMessagesForSingleStateTypes()
  }

  private fun emitLegacyState(emitState: Boolean): Optional<AirbyteMessage> {
    if (!hasEmittedState.get()) {
      hasEmittedState.compareAndSet(false, true)
      if (emitState) {
        return Optional.of(
          AirbyteMessage()
            .withType(AirbyteMessage.Type.STATE)
            .withState(AirbyteStateMessage().withType(AirbyteStateMessage.AirbyteStateType.LEGACY).withData(Jsons.emptyObject())),
        )
      }
    }

    return emitStreamResetTraceMessagesForSingleStateTypes()
  }

  private fun emitStreamResetTraceMessagesForSingleStateTypes(): Optional<AirbyteMessage> {
    if (streamsToReset.isEmpty() && perStreamMessages.isEmpty()) {
      hasEmittedStreamStatus.compareAndSet(false, true)
      return Optional.empty<AirbyteMessage>()
    }

    if (perStreamMessages.isEmpty()) {
      // Per stream, we emit one 'started' and one 'complete' message.
      // The single null state message is to be emitted by the caller.
      val s: io.airbyte.protocol.models.v0.StreamDescriptor = streamsToReset.poll().toProtocol()
      if (!hasCustomNamespace.get()) {
        perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.STARTED))
        perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE))
      }
    }

    val message = perStreamMessages.poll()
    return Optional.ofNullable<AirbyteMessage>(message)
  }

  private fun resettingAllCatalogStreams(sourceConfig: WorkerSourceConfig): Boolean {
    val catalogStreamDescriptors =
      sourceConfig.catalog.streams
        .stream()
        .map { configuredAirbyteStream: ConfiguredAirbyteStream ->
          StreamDescriptor()
            .withName(configuredAirbyteStream.stream.name)
            .withNamespace(configuredAirbyteStream.stream.namespace)
        }.collect(Collectors.toSet())
    val streamsToResetDescriptors = streamsToReset.toMutableSet()
    return streamsToResetDescriptors.containsAll(catalogStreamDescriptors)
  }

  private fun buildNullStreamStateMessage(stream: io.airbyte.protocol.models.v0.StreamDescriptor): AirbyteMessage =
    AirbyteMessage()
      .withType(AirbyteMessage.Type.STATE)
      .withState(
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
          .withStream(
            AirbyteStreamState()
              .withStreamDescriptor(stream)
              .withStreamState(null),
          ),
      )

  private fun getNullGlobalMessage(
    streamsToReset: Queue<StreamDescriptor>,
    currentState: AirbyteStateMessage,
  ): AirbyteMessage {
    val globalState = AirbyteGlobalState()
    globalState.streamStates = ArrayList<AirbyteStreamState>()

    currentState.global.streamStates.forEach(
      Consumer { existingState: AirbyteStreamState ->
        globalState.streamStates
          .add(
            AirbyteStreamState()
              .withStreamDescriptor(existingState.streamDescriptor)
              .withStreamState(
                if (streamsToReset.contains(
                    StreamDescriptor()
                      .withName(existingState.streamDescriptor.name)
                      .withNamespace(existingState.streamDescriptor.namespace),
                  )
                ) {
                  null
                } else {
                  existingState.streamState
                },
              ),
          )
      },
    )

    // If all the streams in the current state have been reset, we consider this to be a full reset, so
    // reset the shared state as well
    if (currentState.global.streamStates.size
        .toLong() ==
      globalState.streamStates
        .stream()
        .filter { streamState: AirbyteStreamState -> streamState.streamState == null }
        .count()
    ) {
      logger.info { "All the streams of a global state have been reset, the shared state will be erased as well" }
      globalState.sharedState = null
    } else {
      logger.info { "This is a partial reset, the shared state will be preserved" }
      globalState.sharedState = currentState.global.sharedState
    }

    // Add state being reset that are not in the current state. This is made to follow the contract of
    // the global state always containing the entire
    // state
    streamsToReset.forEach(
      Consumer { configStreamDescriptor: StreamDescriptor ->
        val streamDescriptor =
          io.airbyte.protocol.models.v0
            .StreamDescriptor()
            .withName(configStreamDescriptor.name)
            .withNamespace(configStreamDescriptor.namespace)
        if (!currentState.global.streamStates
            .stream()
            .map { obj: AirbyteStreamState -> obj.streamDescriptor }
            .toList()
            .contains(streamDescriptor)
        ) {
          globalState.streamStates.add(
            AirbyteStreamState()
              .withStreamDescriptor(streamDescriptor)
              .withStreamState(null),
          )
        }
      },
    )

    return AirbyteMessage()
      .withType(AirbyteMessage.Type.STATE)
      .withState(
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
          .withGlobal(globalState),
      )
  }

  private fun parseResetSourceConfigurationAndLogError(workerSourceConfig: WorkerSourceConfig): ResetSourceConfiguration {
    try {
      return Jsons.`object`<ResetSourceConfiguration>(workerSourceConfig.sourceConnectionConfiguration, ResetSourceConfiguration::class.java)
    } catch (e: IllegalArgumentException) {
      logger.error { "The configuration provided to the reset has an invalid format" }
      throw e
    }
  }
}
