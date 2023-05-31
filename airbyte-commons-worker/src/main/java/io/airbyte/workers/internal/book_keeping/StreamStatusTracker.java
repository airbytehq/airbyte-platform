/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import com.google.common.annotations.VisibleForTesting;
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

  /**
   * Empty {@link AirbyteStreamStatusTraceMessage} that represents a missing stream status in the
   * cache.
   */
  private static final AirbyteStreamStatusTraceMessage MISSING_STATUS_MESSAGE = new AirbyteStreamStatusTraceMessage();

  private final Map<StreamStatusKey, CurrentStreamStatus> currentStreamStatuses = new ConcurrentHashMap<>();

  private final AirbyteApiClient airbyteApiClient;

  public StreamStatusTracker(final AirbyteApiClient airbyteApiClient) {
    this.airbyteApiClient = airbyteApiClient;
  }

  /**
   * Tracks the stream status represented by the event.
   *
   * @param event The {@link ReplicationAirbyteMessageEvent} that contains a stream status message.
   */
  public void track(final ReplicationAirbyteMessageEvent event) {
    try {
      LOGGER.debug("Received message from {} for stream {}:{} -> {}",
          event.airbyteMessageOrigin(),
          event.airbyteMessage().getTrace().getStreamStatus().getStreamDescriptor().getNamespace(),
          event.airbyteMessage().getTrace().getStreamStatus().getStreamDescriptor().getName(),
          event.airbyteMessage().getTrace().getStreamStatus().getStatus());
      handleStreamStatus(event.airbyteMessage().getTrace(), event.airbyteMessageOrigin(), event.replicationContext());
    } catch (final Exception e) {
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
      throws Exception {
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
      throws Exception {
    final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
    final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);

    // If the stream already has a status, then there is an invalid transition
    if (currentStreamStatuses.containsKey(streamStatusKey)) {
      throw new StreamStatusException("Invalid stream status transition to STARTED.", AirbyteMessageOrigin.SOURCE, replicationContext,
          streamDescriptor);
    }

    // Create the new stream status
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
        .retryWithJitterThrows(() -> airbyteApiClient.getStreamStatusesApi().createStreamStatus(streamStatusCreateRequestBody),
            "stream status started " + streamDescriptor.getNamespace() + ":" + streamDescriptor.getName());

    // Add the cached entry to reflect the current status after performing a successful API call to
    // update the status.
    final CurrentStreamStatus currentStreamStatus = new CurrentStreamStatus(Optional.of(streamStatusTraceMessage), Optional.empty());
    currentStreamStatus.setStatusId(streamStatusRead.getId());
    currentStreamStatuses.put(streamStatusKey, currentStreamStatus);

    LOGGER.info("Stream status for stream {}:{} set to STARTED (id = {}, context = {}).",
        streamDescriptor.getNamespace(), streamDescriptor.getName(), streamStatusRead.getId(), replicationContext);
  }

  private void handleStreamRunning(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                   final ReplicationContext replicationContext,
                                   final Duration transitionTimestamp)
      throws Exception {
    final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
    final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);
    final CurrentStreamStatus existingStreamStatus = currentStreamStatuses.get(streamStatusKey);
    if (existingStreamStatus != null && AirbyteStreamStatus.STARTED == existingStreamStatus.getCurrentStatus()) {
      // Update the new stream status
      sendUpdate(existingStreamStatus.getStatusId(), streamDescriptor.getName(), streamDescriptor.getNamespace(), transitionTimestamp.toMillis(),
          replicationContext, StreamStatusRunState.RUNNING, Optional.empty(), AirbyteMessageOrigin.SOURCE);

      // Update the cached entry to reflect the current status after performing a successful API call to
      // update the status.
      existingStreamStatus.setStatus(AirbyteMessageOrigin.SOURCE, streamStatusTraceMessage);

      LOGGER.info("Stream status for stream {}:{} set to RUNNING (id = {}, context = {}).",
          streamDescriptor.getNamespace(), streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
    } else {
      throw new StreamStatusException("Invalid stream status transition to RUNNING.", AirbyteMessageOrigin.SOURCE, replicationContext,
          streamDescriptor);
    }
  }

  private void handleStreamComplete(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                    final AirbyteMessageOrigin airbyteMessageOrigin,
                                    final ReplicationContext replicationContext,
                                    final Duration transitionTimestamp)
      throws Exception {

    if (AirbyteMessageOrigin.INTERNAL == airbyteMessageOrigin) {
      forceCompleteForConnection(replicationContext, transitionTimestamp);
    } else {
      final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
      final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);
      final CurrentStreamStatus existingStreamStatus = currentStreamStatuses.get(streamStatusKey);
      if (existingStreamStatus != null) {
        final CurrentStreamStatus updatedStreamStatus = existingStreamStatus.copy();
        updatedStreamStatus.setStatus(airbyteMessageOrigin, streamStatusTraceMessage);
        if (updatedStreamStatus.isComplete()) {
          sendUpdate(existingStreamStatus.getStatusId(), streamDescriptor.getName(), streamDescriptor.getNamespace(),
              transitionTimestamp.toMillis(), replicationContext, StreamStatusRunState.COMPLETE, Optional.empty(), airbyteMessageOrigin);

          LOGGER.info("Stream status for stream {}:{} set to COMPLETE (id = {}, context = {}).", streamDescriptor.getNamespace(),
              streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
        } else {
          LOGGER.info("Stream status for stream {}:{} set to partially COMPLETE (id = {}, context = {}).",
              streamDescriptor.getNamespace(), streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
        }

        // Update the cached entry to reflect the current status after performing a successful API call to
        // update the status.
        existingStreamStatus.setStatus(airbyteMessageOrigin, streamStatusTraceMessage);
      } else {
        throw new StreamStatusException("Invalid stream status transition to COMPLETE.", airbyteMessageOrigin, replicationContext, streamDescriptor);
      }
    }
  }

  private void handleStreamIncomplete(final AirbyteStreamStatusTraceMessage streamStatusTraceMessage,
                                      final AirbyteMessageOrigin airbyteMessageOrigin,
                                      final ReplicationContext replicationContext,
                                      final Duration transitionTimestamp)
      throws Exception {
    if (AirbyteMessageOrigin.INTERNAL == airbyteMessageOrigin) {
      forceIncompleteForConnection(replicationContext, transitionTimestamp);
    } else {
      final StreamDescriptor streamDescriptor = streamStatusTraceMessage.getStreamDescriptor();
      final StreamStatusKey streamStatusKey = generateStreamStatusKey(replicationContext, streamDescriptor);
      final CurrentStreamStatus existingStreamStatus = currentStreamStatuses.get(streamStatusKey);
      if (existingStreamStatus != null) {
        if (existingStreamStatus.getCurrentStatus() != AirbyteStreamStatus.INCOMPLETE) {
          sendUpdate(existingStreamStatus.getStatusId(), streamDescriptor.getName(), streamDescriptor.getNamespace(),
              transitionTimestamp.toMillis(), replicationContext, StreamStatusRunState.INCOMPLETE,
              Optional.of(StreamStatusIncompleteRunCause.FAILED), airbyteMessageOrigin);
          LOGGER.info("Stream status for stream {}:{} set to INCOMPLETE (id = {}, context = {}).",
              streamDescriptor.getNamespace(), streamDescriptor.getName(), existingStreamStatus.getStatusId(), replicationContext);
        } else {
          LOGGER.info("Stream {}:{} is already in an INCOMPLETE state.", streamDescriptor.getNamespace(), streamDescriptor.getName());
        }

        // Update the cached entry to reflect the current status of the incoming status message
        // Do this after making the API call to ensure that we only make the call to the API once
        // when the first INCOMPLETE message is handled
        existingStreamStatus.setStatus(airbyteMessageOrigin, streamStatusTraceMessage);
      } else {
        throw new StreamStatusException("Invalid stream status transition to INCOMPLETE.", airbyteMessageOrigin, replicationContext,
            streamDescriptor);
      }
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
   * @param airbyteMessageOrigin The origin of the message being handled.
   * @throws StreamStatusException if unable to perform the update due to a missing stream status ID.
   * @throws Exception if unable to call the Airbyte API to update the stream status.
   */
  private void sendUpdate(final Optional<UUID> statusId,
                          final String streamName,
                          final String streamNamespace,
                          final Long transitionedAtMs,
                          final ReplicationContext replicationContext,
                          final StreamStatusRunState streamStatusRunState,
                          final Optional<StreamStatusIncompleteRunCause> incompleteRunCause,
                          final AirbyteMessageOrigin airbyteMessageOrigin)
      throws Exception {
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

      AirbyteApiClient.retryWithJitterThrows(() -> airbyteApiClient.getStreamStatusesApi().updateStreamStatus(streamStatusUpdateRequestBody),
          "update stream status " + streamStatusRunState.name().toLowerCase(Locale.getDefault()) + " " + streamNamespace + ":" + streamName);
    } else {
      throw new StreamStatusException("Stream status ID not present to perform update.", airbyteMessageOrigin, replicationContext, streamName,
          streamNamespace);
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
    forceStatusForConnection(replicationContext, transitionTimestamp, StreamStatusRunState.COMPLETE, Optional.empty());
  }

  /**
   * This method moves any streams associated with the connection ID present in the replication
   * context into an incomplete state. This is to ensure that all streams eventually are moved to a
   * final status. If a stream is already in a complete or incomplete state, it will be ignored from
   * the forced update. All streams associated with the connection ID are removed from the internal
   * tracking map once they are transitioned to the incomplete state.
   *
   * @param replicationContext The {@link ReplicationContext} used to identify tracked streams
   *        associated with a connection ID.
   * @param transitionTimestamp The timestamp of the force status change.
   */
  private void forceIncompleteForConnection(final ReplicationContext replicationContext, final Duration transitionTimestamp) {
    forceStatusForConnection(replicationContext, transitionTimestamp, StreamStatusRunState.INCOMPLETE,
        Optional.of(StreamStatusIncompleteRunCause.FAILED));
  }

  /**
   * This methods moves any streams associated with the connection ID present in the replication
   * context into a terminal status state. This is to ensure that all streams eventually are moved to
   * a final status. If the stream is already in a terminal status state (complete or incomplete), it
   * will be ignored from the forced update. All streams associated with the connection ID are removed
   * from the internal tracking map once they are transitioned to the terminal state provided to this
   * method.
   *
   * @param replicationContext The {@link ReplicationContext} used to identify tracked streams
   *        associated with a connection ID.
   * @param transitionTimestamp The timestamp of the force status change.
   * @param streamStatusRunState The desired terminal status state.
   * @param streamStatusIncompleteRunCause The optional incomplete cause if the desired terminal state
   *        is {@link StreamStatusRunState#INCOMPLETE}.
   */
  private void forceStatusForConnection(final ReplicationContext replicationContext,
                                        final Duration transitionTimestamp,
                                        final StreamStatusRunState streamStatusRunState,
                                        final Optional streamStatusIncompleteRunCause) {
    try {
      for (final Map.Entry<StreamStatusKey, CurrentStreamStatus> e : currentStreamStatuses.entrySet()) {
        /*
         * If the current stream is terminated, that means it is already in an incomplete or fully complete
         * state. If that is the case, there is nothing to do. Otherwise, force the stream to the provided
         * status.
         */
        if (matchesReplicationContext(e.getKey(), replicationContext) && !e.getValue().isTerminated()) {
          sendUpdate(e.getValue().getStatusId(), e.getKey().streamName(), e.getKey().streamNamespace(), transitionTimestamp.toMillis(),
              replicationContext, streamStatusRunState, streamStatusIncompleteRunCause, AirbyteMessageOrigin.INTERNAL);
          LOGGER.info("Stream status for stream {}:{} forced to {} (id = {}, context = {}).",
              e.getKey().streamNamespace(), e.getKey().streamName(), streamStatusRunState.name(), e.getValue().getStatusId(), replicationContext);
        } else {
          LOGGER.info("Stream {}:{} already has a terminal status.  Nothing to force (id = {}, context = {}).", e.getKey().streamNamespace(),
              e.getKey().streamName(), e.getValue().getStatusId(), replicationContext);
        }
      }

      // Remove all streams from the tracking map associated with the connection ID after the force update
      final Set<StreamStatusKey> toBeRemoved =
          currentStreamStatuses.keySet().stream().filter(e -> matchesReplicationContext(e, replicationContext)).collect(Collectors.toSet());
      toBeRemoved.forEach(r -> currentStreamStatuses.remove(r));
    } catch (final Exception ex) {
      LOGGER.error("Unable to force streams for connection {} to status {}.", replicationContext.connectionId(), streamStatusRunState, ex);
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
    return new StreamStatusKey(streamDescriptor.getName(), streamDescriptor.getNamespace(), replicationContext.workspaceId(),
        replicationContext.connectionId(), replicationContext.jobId(), replicationContext.attempt());
  }

  /**
   * Tests whether the {@link StreamStatusKey} matches the {@link ReplicationContext}. This comparison
   * checks the attempt number, connection ID, job ID and workspace ID associated with the sync,
   * ignoring the stream information, to determine if the stream is part of the replication context.
   *
   * @param streamStatusKey The stream status key that is used to look up the status of the stream.
   * @param replicationContext The replication context for the current sync.
   * @return {@code true} if the stream is part of the replication context, {@code false} otherwise.
   */
  private boolean matchesReplicationContext(final StreamStatusKey streamStatusKey, final ReplicationContext replicationContext) {
    return streamStatusKey.attempt().equals(replicationContext.attempt())
        && streamStatusKey.connectionId().equals(replicationContext.connectionId())
        && streamStatusKey.jobId().equals(replicationContext.jobId())
        && streamStatusKey.workspaceId().equals(replicationContext.workspaceId());
  }

  /**
   * Key for the internal current stream status map. This key includes the stream information and
   * replication execution context.
   *
   * @param streamName The stream name.
   * @param streamNamespace The stream namespace.
   * @param workspaceId The workspace ID associated with the replication execution.
   * @param connectionId The connection ID associated with the replication execution.
   * @param jobId The job ID associated with the replication execution.
   * @param attempt The attempt number associated with the replication execution.
   */
  public record StreamStatusKey(String streamName, String streamNamespace, UUID workspaceId, UUID connectionId, Long jobId, Integer attempt) {}

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
      return AirbyteStreamStatus.COMPLETE.equals(sourceStatus.orElse(MISSING_STATUS_MESSAGE).getStatus())
          && AirbyteStreamStatus.COMPLETE.equals(destinationStatus.orElse(MISSING_STATUS_MESSAGE).getStatus());
    }

    /**
     * Tests whether the stream is incomplete based on the status of either the source and destination
     * connectors that are part of the sync.
     * <p>
     * </p>
     * If the source status is present and is equal to {@link AirbyteStreamStatus#INCOMPLETE} <b>OR</b>
     * the destination status is present and is equal to {@link AirbyteStreamStatus#INCOMPLETE}, then
     * the status is considered to be incomplete.
     *
     * @return {@code True} if the stream status is considered to be incomplete, {@code false}
     *         otherwise.
     */
    boolean isIncomplete() {
      return AirbyteStreamStatus.INCOMPLETE.equals(sourceStatus.orElse(MISSING_STATUS_MESSAGE).getStatus())
          || AirbyteStreamStatus.INCOMPLETE.equals(destinationStatus.orElse(MISSING_STATUS_MESSAGE).getStatus());
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
      return isIncomplete() || isComplete();
    }

    /**
     * Creates a copy of this {@link CurrentStreamStatus}.
     *
     * @return A copy of the this {@link CurrentStreamStatus}.
     */
    CurrentStreamStatus copy() {
      return new CurrentStreamStatus(sourceStatus, destinationStatus);
    }

  }

  @VisibleForTesting
  StreamStatusJobType mapIsResetToJobType(final boolean isReset) {
    return isReset ? StreamStatusJobType.RESET : StreamStatusJobType.SYNC;
  }

}
