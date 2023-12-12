/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ResetSourceConfiguration;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.protocol.models.AirbyteGlobalState;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * This source will never emit any messages. It can be used in cases where that is helpful (hint:
 * reset connection jobs).
 */
@Slf4j
public class EmptyAirbyteSource implements AirbyteSource {

  private final AtomicBoolean hasEmittedState;
  private final AtomicBoolean hasEmittedStreamStatus;
  private final Queue<StreamDescriptor> streamsToReset = new LinkedList<>();
  private final Queue<AirbyteMessage> perStreamMessages = new LinkedList<>();
  private boolean isStarted = false;
  private Optional<StateWrapper> stateWrapper = Optional.empty();

  public EmptyAirbyteSource() {
    hasEmittedState = new AtomicBoolean();
    hasEmittedStreamStatus = new AtomicBoolean();
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void start(final WorkerSourceConfig workerSourceConfig, final Path jobRoot) throws Exception {
    if (workerSourceConfig != null && workerSourceConfig.getSourceConnectionConfiguration() != null) {
      final ResetSourceConfiguration resetSourceConfiguration = parseResetSourceConfigurationAndLogError(workerSourceConfig);
      streamsToReset.addAll(resetSourceConfiguration.getStreamsToReset());

      if (!streamsToReset.isEmpty()) {
        if (workerSourceConfig.getState() != null) {
          stateWrapper = StateMessageHelper.getTypedState(workerSourceConfig.getState().getState());

          if (stateWrapper.isPresent()
              && stateWrapper.get().getStateType() == StateType.LEGACY
              && !resettingAllCatalogStreams(workerSourceConfig)) {
            log.error("The state a legacy one but we are trying to do a partial update, this is not supported.");
            throw new IllegalStateException("Try to perform a partial reset on a legacy state");
          }

        }
      }
    }
    isStarted = true;
  }

  // always finished. it has no data to send.
  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public boolean isFinished() {
    return hasEmittedState.get() && hasEmittedStreamStatus.get();
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public int getExitValue() {
    return 0;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Optional<AirbyteMessage> attemptRead() {

    if (!isStarted) {
      throw new IllegalStateException("The empty source has not been started.");
    }

    if (stateWrapper.isPresent()) {
      if (stateWrapper.get().getStateType() == StateType.STREAM) {
        return emitPerStreamState();
      } else if (stateWrapper.get().getStateType() == StateType.GLOBAL) {
        return emitGlobalState();
      }
      final boolean isLegacyStateValueAbsent = stateWrapper.get().getLegacyState() == null
          || stateWrapper.get().getLegacyState().isNull()
          || stateWrapper.get().getLegacyState().isEmpty();
      return emitLegacyState(!isLegacyStateValueAbsent);
    }
    return emitLegacyState(false);

  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void close() throws Exception {
    // no op.
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void cancel() throws Exception {
    // no op.
  }

  private Optional<AirbyteMessage> emitPerStreamState() {
    if (streamsToReset.isEmpty() && perStreamMessages.isEmpty()) {
      hasEmittedState.compareAndSet(false, true);
      hasEmittedStreamStatus.compareAndSet(false, true);
      return Optional.empty();
    }

    if (perStreamMessages.isEmpty()) {
      // Per stream, we emit one 'started', one null state and one 'complete' message.
      // Since there's only 1 state message we move directly from 'started' to 'complete'.
      final StreamDescriptor s = streamsToReset.poll();
      perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatus.STARTED));
      perStreamMessages.add(buildNullStreamStateMessage(s));
      perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatus.COMPLETE));
    }

    final AirbyteMessage message = perStreamMessages.poll();
    return Optional.ofNullable(message);
  }

  private Optional<AirbyteMessage> emitGlobalState() {
    if (!hasEmittedState.get()) {
      hasEmittedState.compareAndSet(false, true);
      return Optional.of(getNullGlobalMessage(streamsToReset, stateWrapper.get().getGlobal()));
    }

    return emitStreamResetTraceMessagesForSingleStateTypes();
  }

  private Optional<AirbyteMessage> emitLegacyState(final boolean emitState) {
    if (!hasEmittedState.get()) {
      hasEmittedState.compareAndSet(false, true);
      if (emitState) {
        return Optional.of(new AirbyteMessage().withType(Type.STATE)
            .withState(new AirbyteStateMessage().withType(AirbyteStateType.LEGACY).withData(Jsons.emptyObject())));
      }
    }

    return emitStreamResetTraceMessagesForSingleStateTypes();
  }

  private Optional<AirbyteMessage> emitStreamResetTraceMessagesForSingleStateTypes() {
    if (streamsToReset.isEmpty() && perStreamMessages.isEmpty()) {
      hasEmittedStreamStatus.compareAndSet(false, true);
      return Optional.empty();
    }

    if (perStreamMessages.isEmpty()) {
      // Per stream, we emit one 'started' and one 'complete' message.
      // The single null state message is to be emitted by the caller.
      final StreamDescriptor s = streamsToReset.poll();
      perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatus.STARTED));
      perStreamMessages.add(AirbyteMessageUtils.createStatusTraceMessage(s, AirbyteStreamStatus.COMPLETE));
    }

    final AirbyteMessage message = perStreamMessages.poll();
    return Optional.ofNullable(message);
  }

  private boolean resettingAllCatalogStreams(final WorkerSourceConfig sourceConfig) {
    final Set<StreamDescriptor> catalogStreamDescriptors = sourceConfig.getCatalog().getStreams().stream().map(
        configuredAirbyteStream -> new StreamDescriptor()
            .withName(configuredAirbyteStream.getStream().getName())
            .withNamespace(configuredAirbyteStream.getStream().getNamespace()))
        .collect(Collectors.toSet());
    final Set<StreamDescriptor> streamsToResetDescriptors = new HashSet<>(streamsToReset);
    return streamsToResetDescriptors.containsAll(catalogStreamDescriptors);
  }

  private AirbyteMessage buildNullStreamStateMessage(final StreamDescriptor stream) {
    return new AirbyteMessage()
        .withType(Type.STATE)
        .withState(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new io.airbyte.protocol.models.StreamDescriptor()
                            .withName(stream.getName())
                            .withNamespace(stream.getNamespace()))
                        .withStreamState(null)));
  }

  private AirbyteMessage getNullGlobalMessage(final Queue<StreamDescriptor> streamsToReset, final AirbyteStateMessage currentState) {
    final AirbyteGlobalState globalState = new AirbyteGlobalState();
    globalState.setStreamStates(new ArrayList<>());

    currentState.getGlobal().getStreamStates().forEach(existingState -> globalState.getStreamStates()
        .add(
            new AirbyteStreamState()
                .withStreamDescriptor(existingState.getStreamDescriptor())
                .withStreamState(
                    streamsToReset.contains(new StreamDescriptor()
                        .withName(existingState.getStreamDescriptor().getName())
                        .withNamespace(existingState.getStreamDescriptor().getNamespace())) ? null : existingState.getStreamState())));

    // If all the streams in the current state have been reset, we consider this to be a full reset, so
    // reset the shared state as well
    if (currentState.getGlobal().getStreamStates().size() == globalState.getStreamStates().stream()
        .filter(streamState -> streamState.getStreamState() == null).count()) {
      log.info("All the streams of a global state have been reset, the shared state will be erased as well");
      globalState.setSharedState(null);
    } else {
      log.info("This is a partial reset, the shared state will be preserved");
      globalState.setSharedState(currentState.getGlobal().getSharedState());
    }

    // Add state being reset that are not in the current state. This is made to follow the contract of
    // the global state always containing the entire
    // state
    streamsToReset.forEach(configStreamDescriptor -> {
      final io.airbyte.protocol.models.StreamDescriptor streamDescriptor = new io.airbyte.protocol.models.StreamDescriptor()
          .withName(configStreamDescriptor.getName())
          .withNamespace(configStreamDescriptor.getNamespace());
      if (!currentState.getGlobal().getStreamStates().stream().map(streamState -> streamState.getStreamDescriptor()).toList()
          .contains(streamDescriptor)) {
        globalState.getStreamStates().add(new AirbyteStreamState()
            .withStreamDescriptor(streamDescriptor)
            .withStreamState(null));
      }
    });

    return new AirbyteMessage()
        .withType(Type.STATE)
        .withState(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.GLOBAL)
                .withGlobal(globalState));
  }

  private ResetSourceConfiguration parseResetSourceConfigurationAndLogError(final WorkerSourceConfig workerSourceConfig) {
    try {
      return Jsons.object(workerSourceConfig.getSourceConnectionConfiguration(), ResetSourceConfiguration.class);
    } catch (final IllegalArgumentException e) {
      log.error("The configuration provided to the reset has an invalid format");
      throw e;
    }
  }

}
