/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping.events;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.workers.internal.book_keeping.StreamStatusTracker;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.scheduling.annotation.Async;
import jakarta.inject.Singleton;

/**
 * Custom application listener that handles Airbyte Protocol {@link Type#TRACE} messages of type
 * {@link AirbyteTraceMessage.Type#STREAM_STATUS} to track the status (or progress) of a stream
 * within replication. The listener handles the messages asynchronously to avoid blocking
 * replication.
 */
@Singleton
public class AirbyteStreamStatusMessageEventListener implements ApplicationEventListener<ReplicationAirbyteMessageEvent> {

  private final StreamStatusTracker streamStatusTracker;

  public AirbyteStreamStatusMessageEventListener(final StreamStatusTracker streamStatusTracker) {
    this.streamStatusTracker = streamStatusTracker;
  }

  @Override
  @Async("stream-status")
  public void onApplicationEvent(final ReplicationAirbyteMessageEvent event) {
    streamStatusTracker.track(event);
  }

  @Override
  public boolean supports(final ReplicationAirbyteMessageEvent event) {
    return isStreamStatusTraceMessage(event.airbyteMessage());
  }

  private boolean isStreamStatusTraceMessage(final AirbyteMessage airbyteMessage) {
    return Type.TRACE.equals(airbyteMessage.getType())
        && AirbyteTraceMessage.Type.STREAM_STATUS.equals(airbyteMessage.getTrace().getType());
  }

}
