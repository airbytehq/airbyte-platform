/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.StreamStatusCreateRequestBody;
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRead;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.api.client.model.generated.StreamStatusUpdateRequestBody;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEvent;
import io.airbyte.workers.internal.exception.StreamStatusException;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the status of individual streams within a replication sync based on the status of
 * source/destination messages.
 */
@Singleton
public class StreamStatusTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamStatusTracker.class);

  private final Map<StreamStatusKey, CurrentStreamStatus> currentStreamStatuses = new ConcurrentHashMap<>();

  private final AirbyteApiClient airbyteApiClient;

  public StreamStatusTracker(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  /**
   * Tracks the stream status represented by the event.
   *
   * @param event The {@link ReplicationAirbyteMessageEvent} that contains a stram status message.
   */
  @Trace
  public void track(final ReplicationAirbyteMessageEvent event) {
    try {
      LOGGER.debug("Received message from {} for stream {}:{} -> {}",
          event.airbyteMessageOrigin(),
          event.airbyteMessage().getTrace().getStreamStatus().getStreamDescriptor().getNamespace(),
          event.airbyteMessage().getTrace().getStreamStatus().getStreamDescriptor().getName(),
          event.airbyteMessage().getTrace().getStreamStatus().getStatus());
      handleStreamStatus(event.airbyteMessage().getTrace(), event.airbyteMessageOrigin(), event.replicationContext());
    } catch (final StreamStatusException e) {
      LOGGER.error("Unable to update stream status for event {}.", event, e);
    }
  }

  /**
   * Retrieves the current stream status that is tracked by this tracker.
   *
   * @param streamStatusKey The {@link StreamStatusKey}.
   * @return The currently tracked status for the stream, if any.
   */
  public Optional<AirbyteStreamStatus> getCurrentStreamStatus(final StreamStatusKey streamStatusKey) {
    return Optional.ofNullable(currentStreamStatuses.get(streamStatusKey)).map(CurrentStreamStatus::getCurrentStatus);
  }

  private void handleStreamStatus(final AirbyteTraceMessage airbyteTraceMessage,
                                  final AirbyteMessageOrigin airbyteMessageOrigin,
                                  final ReplicationContext replicationContext)
      throws StreamStatusException {
    final AirbyteStreamStatusTraceMessage streamStatusTraceMessage = airbyteTraceMessage.getStreamStatus();
    final Duration transitionTimestamp = Duration.ofMillis(Double.valueOf(airbyteTraceMessage.getEmittedAt()).longValue());
    switch (streamStatusTraceMessage.getStatus()) {
      case STARTED -> handleStreamStarted(streamStatusTraceMessage, replicationContext, transitionTimestamp);
      case RUNNING -> handleStreamRunning(streamStatusTraceMessage, replicationContext, transitionTimestamp);
      case COMPLETE -> handleStreamComplete(streamStatusTraceMessage, airbyteMessageOrigin, replicationContext, transitionTimestamp);
      case INCOMPLETE -> handleStreamIncomplete(streamStatusTraceMessage, airbyteMessageOrigin, replicationContext, transitionTimestamp);
      default -> LOGGER.warn("Invalid stream status '{}' for message: {}", streamStatusTraceMessage.getStatus(), streamStatusTraceMessage);
    }
  }

  private void handleStreamStarted(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                   final ReplicationContext replicationContext,
                                   final Duration transitionTimestamp)
      throws StreamStatusException {
    final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
    final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);

    // If the stream already has a status, then there is an invalid transition
    if (currentStreamStatuses.containsKey(streamStatusKey)) {
      throw new StreamStatusException("Invalid stream status transition to STARTED.", streamDescriptor);
    }

    final CurrentStreamStatus currentStreamStatus = new CurrentStreamStatus(Optional.of(streamStatusTraceMessage), Optional.empty());
    currentStreamStatuses.put(streamStatusKey, currentStreamStatus);

    final StreamStatusCreateRequestBody streamStatusCreateRequestBody = new StreamStatusCreateRequestBody()
        .streamName(streamDescriptor.getName())
        .streamNamespace(streamDescriptor.getNamespace())
        .jobId(replicationContext.jobId())
        .jobType(mapIsResetToJobType(replicationContext.isReset()))
        .connectionId(replicationContext.connectionId())
        .attemptNumber(replicationContext.attempt())
        .runState(StreamStatusRunState.PENDING)
        .transitionedAt(transitionTimestamp.toMillis())
        .workspaceId(replicationContext.workspaceId());
    final StreamStatusRead streamStatusRead = AirbyteApiClient
        .retryWithJitter(() -> airbyteApiClient.getStreamStatusesApi().createStreamStatus(streamStatusCreateRequestBody),
            "stream status started " + streamDescriptor.getNamespace() + ":" + streamDescriptor.getName());
    currentStreamStatus.setStatusId(streamStatusRead.getId());

    LOGGER.debug("Stream status for stream {}:{} set to STARTED (id = {}, context = {}).",
        streamDescriptor.getNamespace(), streamDescriptor.getName(), streamStatusRead.getId(), replicationContext);
  }

  private void handleStreamRunning(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                   final ReplicationContext replicationContext,
                                   final Duration transitionTimestamp)
      throws StreamStatusException {
    final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
    final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);
    final CurrentStreamStatus existingStreamStatus = currentStreamStatuses.get(streamStatusKey);
    if (existingStreamStatus != null && AirbyteStreamStatus.STARTED == existingStreamStatus.getCurrentStatus()) {
      existingStreamStatus.setStatus(AirbyteMessageOrigin.SOURCE, streamStatusTraceMessage);
      sendUpdate(existingStreamStatus.getStatusId(), streamDescriptor.getName(), streamDescriptor.getNamespace(), transitionTimestamp.toMillis(),
          replicationContext, StreamStatusRunState.RUNNING, Optional.empty());
      LOGGER.debug("Stream status for stream {}:{} set to RUNNING (id = {}, context = {}).",
          streamDescriptor.getNamespace(), streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
    } else {
      throw new StreamStatusException("Invalid stream status transition to RUNNING.", streamDescriptor);
    }
  }

  private void handleStreamComplete(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                    final AirbyteMessageOrigin airbyteMessageOrigin,
                                    final ReplicationContext replicationContext,
                                    final Duration transitionTimestamp)
      throws StreamStatusException {

    if (AirbyteMessageOrigin.INTERNAL == airbyteMessageOrigin) {
      forceCompleteForConnection(replicationContext, transitionTimestamp);
    } else {
      final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
      final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);
      final CurrentStreamStatus existingStreamStatus = currentStreamStatuses.get(streamStatusKey);
      if (existingStreamStatus != null) {
        existingStreamStatus.setStatus(airbyteMessageOrigin, streamStatusTraceMessage);

        if (existingStreamStatus.isComplete()) {
          sendUpdate(existingStreamStatus.getStatusId(), streamDescriptor.getName(), streamDescriptor.getNamespace(),
              transitionTimestamp.toMillis(), replicationContext, StreamStatusRunState.COMPLETE, Optional.empty());
          LOGGER.debug("Stream status for stream {}:{} set to COMPLETE (id = {}, context = {}).", streamDescriptor.getNamespace(),
              streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
        } else {
          LOGGER.debug("Stream status for stream {}:{} set to partially COMPLETE (id = {}, context = {}).",
              streamDescriptor.getNamespace(), streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
        }

        if (existingStreamStatus.isTerminated()) {
          currentStreamStatuses.remove(streamStatusKey);
        }
      } else {
        throw new StreamStatusException("Invalid stream status transition to COMPLETE.", streamDescriptor);
      }
    }
  }

  private void handleStreamIncomplete(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                      final AirbyteMessageOrigin airbyteMessageOrigin,
                                      final ReplicationContext replicationContext,
                                      final Duration transitionTimestamp)
      throws StreamStatusException {
    final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
    final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);
    final CurrentStreamStatus existingStreamStatus = currentStreamStatuses.get(streamStatusKey);
    if (existingStreamStatus != null) {
      if (existingStreamStatus.getCurrentStatus() != AirbyteStreamStatus.INCOMPLETE) {
        sendUpdate(existingStreamStatus.getStatusId(), streamDescriptor.getName(), streamDescriptor.getNamespace(),
            transitionTimestamp.toMillis(), replicationContext, StreamStatusRunState.INCOMPLETE, Optional.of(StreamStatusIncompleteRunCause.FAILED));
        LOGGER.debug("Stream status for stream {}:{} set to INCOMPLETE (id = {}, context = {}).",
            streamDescriptor.getNamespace(), streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
      } else {
        LOGGER.debug("Stream {}:{} is already in an INCOMPLETE state.", streamDescriptor.getNamespace(), streamDescriptor.getName());
      }

      // Update the cached entry to reflect the current status of the incoming status message
      // Do this after making the API call to ensure that we only make the call to the API once
      // when the first INCOMPLETE message is handled
      existingStreamStatus.setStatus(airbyteMessageOrigin, streamStatusTraceMessage);

      if (existingStreamStatus.isTerminated()) {
        currentStreamStatuses.remove(streamStatusKey);
      }
    } else {
      throw new StreamStatusException("Invalid stream status transition to INCOMPLETE.", streamDescriptor);
    }
  }

  /**
   * Sends a stream status update request to the API.
   *
   * @param statusId The ID of the stream status to update.
   * @param streamName The name of the stream to update.
   * @param streamNamespace The namespace of the stream to update.
   * @param transitionedAtMs The timestamp of the status change.
   * @param replicationContext The {@link ReplicationContext} that holds identifying information about
   *        the sync associated with the stream.
   * @param streamStatusRunState The new stream status.
   * @param incompleteRunCause The option reason for an incomplete status.
   * @throws StreamStatusException if unable to perform the update due to a missing stream status ID.
   */
  private void sendUpdate(final Optional<UUID> statusId,
                          final String streamName,
                          final String streamNamespace,
                          final Long transitionedAtMs,
                          final ReplicationContext replicationContext,
                          final StreamStatusRunState streamStatusRunState,
                          final Optional<StreamStatusIncompleteRunCause> incompleteRunCause)
      throws StreamStatusException {
    if (statusId.isPresent()) {
      final StreamStatusUpdateRequestBody streamStatusUpdateRequestBody = new StreamStatusUpdateRequestBody()
          .id(statusId.get())
          .streamName(streamName)
          .streamNamespace(streamNamespace)
          .jobId(replicationContext.jobId())
          .jobType(mapIsResetToJobType(replicationContext.isReset()))
          .connectionId(replicationContext.connectionId())
          .attemptNumber(replicationContext.attempt())
          .runState(streamStatusRunState)
          .transitionedAt(transitionedAtMs)
          .workspaceId(replicationContext.workspaceId());

      incompleteRunCause.ifPresent(i -> streamStatusUpdateRequestBody.setIncompleteRunCause(i));

      AirbyteApiClient.retryWithJitter(() -> airbyteApiClient.getStreamStatusesApi().updateStreamStatus(streamStatusUpdateRequestBody),
          "update stream status " + streamStatusRunState.name().toLowerCase(Locale.getDefault()) + " " + streamNamespace + ":" + streamName);
    } else {
      throw new StreamStatusException("Stream status ID not present to perform update.", streamName, streamNamespace);
    }
  }

  /**
   * This method moves any streams associated with the connection ID present in the replication
   * context into a completed state. This is to ensure that all streams eventually are completed and
   * is necessary for syncs in which destination connectors do not provide messages to trigger the
   * completion. If a stream is already in a complete or incomplete state, it will be ignored from the
   * forced update. All streams associated with the connection ID are removed from the internal
   * tracking map once they are transitioned to the complete state.
   *
   * @param replicationContext The {@link ReplicationContext} used to identify tracked streams
   *        associated with a connection ID.
   * @param transitionTimestamp The timestamp of the force status change.
   */
  private void forceCompleteForConnection(final ReplicationContext replicationContext, final Duration transitionTimestamp) {
    try {
      for (final Map.Entry<StreamStatusKey, CurrentStreamStatus> e : currentStreamStatuses.entrySet()) {
        if (e.getKey().connectionId.equals(replicationContext.connectionId())) {
          /*
           * If the current stream is terminated, that means it is already in an incomplete or fully complete
           * state. If that is the case, there is nothing to do. Otherwise, force the stream to a completed
           * state.
           */
          if (!e.getValue().isTerminated()) {
            sendUpdate(e.getValue().getStatusId(), e.getKey().streamName(), e.getKey().streamNamespace(), transitionTimestamp.toMillis(),
                replicationContext, StreamStatusRunState.COMPLETE, Optional.empty());

          }
        }
      }

      // Remove all streams from the tracking map associated with the connection ID after the force update
      final Set<StreamStatusKey> toBeRemoved =
          currentStreamStatuses.keySet().stream().filter(e -> e.connectionId().equals(replicationContext.connectionId())).collect(
              Collectors.toSet());
      toBeRemoved.forEach(r -> currentStreamStatuses.remove(r));
    } catch (final StreamStatusException ex) {
      LOGGER.error("Unable to force complete streams for connection {}.", replicationContext.connectionId(), ex);
    }
  }

  /**
   * Builds a {@link StreamStatusKey} from the provided criteria.
   *
   * @param replicationContext The {@link ReplicationContext} for the replication execution.
   * @param streamDescriptor The {@link StreamDescriptor} of the stream involved in the replication
   *        execution.
   * @return The new {@link StreamStatusKey}.
   */
  private StreamStatusKey generateStreamStatusKey(final ReplicationContext replicationContext, final StreamDescriptor streamDescriptor) {
    return new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(), replicationContext.connectionId(),
        replicationContext.jobId(), replicationContext.attempt());
  }

  /**
   * Key for the internal current stream status map. This key includes the stream information and
   * replication execution context.
   *
   * @param streamName The stream name.
   * @param streamNamespace The stream namespace.
   * @param connectionId The connection ID associated with the replication execution.
   * @param jobId The job ID associated with the replication execution.
   * @param attempt The attempt number associated with the replication execution.
   */
  public record StreamStatusKey(String streamName, String streamNamespace, UUID connectionId, Long jobId, Integer attempt) {}

  /**
   * Represents the current status of a stream. It is used to track the transition through the various
   * status values.
   */
  private static class CurrentStreamStatus {

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentStreamStatus.class);

    private Optional<AirbyteStreamStatusTraceMessage> sourceStatus;
    private Optional<AirbyteStreamStatusTraceMessage> destinationStatus;

    private Optional<UUID> statusId = Optional.empty();

    CurrentStreamStatus(final Optional<AirbyteStreamStatusTraceMessage> sourceStatus,
                        final Optional<AirbyteStreamStatusTraceMessage> destinationStatus) {
      this.sourceStatus = sourceStatus;
      this.destinationStatus = destinationStatus;
    }

    void setStatus(final AirbyteMessageOrigin airbyteMessageOrigin, final AirbyteStreamStatusTraceMessage statusMessage) {
      switch (airbyteMessageOrigin) {
        case DESTINATION -> setDestinationStatus(statusMessage);
        case SOURCE -> setSourceStatus(statusMessage);
        default -> LOGGER.warn("Unsupported status message for {} message source.", airbyteMessageOrigin);
      }
    }

    void setStatusId(final UUID id) {
      this.statusId = Optional.ofNullable(id);
    }

    void setSourceStatus(final AirbyteStreamStatusTraceMessage sourceStatus) {
      this.sourceStatus = Optional.ofNullable(sourceStatus);
    }

    void setDestinationStatus(final AirbyteStreamStatusTraceMessage destinationStatus) {
      this.destinationStatus = Optional.ofNullable(destinationStatus);
    }

    Optional<UUID> getStatusId() {
      return statusId;
    }

    AirbyteStreamStatus getCurrentStatus() {
      if (destinationStatus.isPresent()) {
        return destinationStatus.get().getStatus();
      } else if (sourceStatus.isPresent()) {
        return sourceStatus.get().getStatus();
      } else {
        return null;
      }
    }

    /**
     * Tests whether the stream is complete based on the status of both the source and destination
     * connectors that are part of the sync.
     * <p>
     * </p>
     * If the source status is present and is equal to {@link AirbyteStreamStatus#COMPLETE} <b>AND</b>
     * the destination status is present and is equal to {@link AirbyteStreamStatus#COMPLETE}, then the
     * status is considered to be complete. For any other combination, the status is considered to be
     * incomplete.
     *
     * @return {@code True} if the stream status is considered to be complete, {@code false} otherwise.
     */
    boolean isComplete() {
      return sourceStatus.isPresent() && AirbyteStreamStatus.COMPLETE == sourceStatus.get().getStatus()
          && destinationStatus.isPresent() && AirbyteStreamStatus.COMPLETE == destinationStatus.get().getStatus();
    }

    /**
     * Tests whether the stream is in a terminal status.
     * <p>
     * </p>
     * If the source status is present and has either a {@link AirbyteStreamStatus#COMPLETE} or
     * {@link AirbyteStreamStatus#INCOMPLETE} status <b>AND</b> the destination status is present and
     * has either a {@link AirbyteStreamStatus#COMPLETE} or {@link AirbyteStreamStatus#INCOMPLETE}
     * status, then the status is considered to be in a terminal state. For any other combination, the
     * status is considered to be non-terminal and may still be updated.
     *
     * @return {@code True} if the stream status is considered to be in a terminal state, {@code false}
     *         otherwise.
     */
    boolean isTerminated() {
      return (sourceStatus.isPresent()
          && (AirbyteStreamStatus.INCOMPLETE == sourceStatus.get().getStatus() || AirbyteStreamStatus.COMPLETE == sourceStatus.get()
              .getStatus()))
          && (destinationStatus.isPresent()
              && (AirbyteStreamStatus.INCOMPLETE == destinationStatus.get().getStatus() || AirbyteStreamStatus.COMPLETE == destinationStatus.get()
                  .getStatus()));
    }

  }

  StreamStatusJobType mapIsResetToJobType(final boolean isReset) {
    return isReset ? StreamStatusJobType.RESET : StreamStatusJobType.SYNC;
  }

}
