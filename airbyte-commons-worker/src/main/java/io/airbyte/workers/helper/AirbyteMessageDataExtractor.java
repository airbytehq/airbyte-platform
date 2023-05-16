/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.StreamDescriptor;
import jakarta.inject.Singleton;

/**
 * Utility class that provides helper functions for extracting data from a {@link AirbyteMessage}.
 */
@Singleton
public class AirbyteMessageDataExtractor {

  /**
   * Extracts the stream descriptor from the provided {@link AirbyteMessage}, returning the provided
   * {@code defaultValue} value if the {@link AirbyteMessage} does not include a stream descriptor.
   *
   * @param airbyteMessage The {@link AirbyteMessage} that may contain stream information.
   * @param defaultValue The default value to return if the provided {@link AirbyteMessage} does not
   *        contain stream information.
   * @return The {@link StreamDescriptor} extracted from the provided {@link AirbyteMessage} or the
   *         provided {@code defaultValue} if the message does not contain stream information.
   */
  public StreamDescriptor extractStreamDescriptor(final AirbyteMessage airbyteMessage, final StreamDescriptor defaultValue) {
    final StreamDescriptor extractedStreamDescriptor = getStreamFromMessage(airbyteMessage);
    return extractedStreamDescriptor != null ? extractedStreamDescriptor : defaultValue;
  }

  private StreamDescriptor getStreamFromMessage(final AirbyteMessage airbyteMessage) {
    switch (airbyteMessage.getType()) {
      case RECORD:
        return new StreamDescriptor().withName(airbyteMessage.getRecord().getStream()).withNamespace(airbyteMessage.getRecord().getNamespace());
      case STATE:
        return airbyteMessage.getState().getStream() != null ? airbyteMessage.getState().getStream().getStreamDescriptor() : null;
      case TRACE:
        return getStreamFromTrace(airbyteMessage.getTrace());
      default:
        return null;
    }
  }

  private StreamDescriptor getStreamFromTrace(final AirbyteTraceMessage airbyteTraceMessage) {
    switch (airbyteTraceMessage.getType()) {
      case STREAM_STATUS:
        return airbyteTraceMessage.getStreamStatus().getStreamDescriptor();
      case ERROR:
        return airbyteTraceMessage.getError().getStreamDescriptor();
      case ESTIMATE:
        return new StreamDescriptor().withName(airbyteTraceMessage.getEstimate().getName())
            .withNamespace(airbyteTraceMessage.getEstimate().getNamespace());
      default:
        return null;
    }
  }

}
