/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping;

import io.airbyte.config.FailureReason;
import io.airbyte.config.State;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import java.util.Optional;

/**
 * Interface to handle extracting metadata from the stream of data flowing from a Source to a
 * Destination.
 */
public interface MessageTracker {

  /**
   * Accepts an AirbyteMessage emitted from a source and tracks any metadata about it that is required
   * by the Platform.
   *
   * @param message message to derive metadata from.
   */
  void acceptFromSource(AirbyteMessage message);

  /**
   * Accepts an AirbyteMessage emitted from a destination and tracks any metadata about it that is
   * required by the Platform.
   *
   * @param message message to derive metadata from.
   */
  void acceptFromDestination(AirbyteMessage message);

  /**
   * Get the current source state of the stream.
   *
   * @return returns the last StateMessage that was accepted from the source. If no StateMessage was
   *         accepted, empty.
   */
  Optional<State> getSourceOutputState();

  /**
   * Get the current destination state of the stream.
   *
   * @return returns the last StateMessage that was accepted from the destination. If no StateMessage
   *         was accepted, empty.
   */
  Optional<State> getDestinationOutputState();

  AirbyteTraceMessage getFirstDestinationErrorTraceMessage();

  AirbyteTraceMessage getFirstSourceErrorTraceMessage();

  FailureReason errorTraceMessageFailure(Long jobId, Integer attempt);

  /**
   * Get the current SyncStatsTracker.
   */
  SyncStatsTracker getSyncStatsTracker();

}
