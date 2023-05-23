/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping.events;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageOrigin;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of utility methods that aid with the publishing of
 * {@link ReplicationAirbyteMessageEvent} messages.
 */
@Singleton
public class ReplicationAirbyteMessageEventPublishingHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationAirbyteMessageEventPublishingHelper.class);

  private final ApplicationEventPublisher<ReplicationAirbyteMessageEvent> eventPublisher;

  public ReplicationAirbyteMessageEventPublishingHelper(final ApplicationEventPublisher<ReplicationAirbyteMessageEvent> eventPublisher) {
    this.eventPublisher = eventPublisher;
  }

  /**
   * Publishes a complete status event used to indicate that the source or destination has finished
   * its execution for the given stream successfully.
   *
   * @param stream The stream to be associated with the status.
   * @param replicationContext The replication context containing information about the sync.
   * @param airbyteMessageOrigin The {@link AirbyteMessage} origin that will be associated with the
   *        published complete status event.
   */
  public void publishCompleteStatusEvent(final StreamDescriptor stream,
                                         final ReplicationContext replicationContext,
                                         final AirbyteMessageOrigin airbyteMessageOrigin) {
    publishStatusEvent(stream, AirbyteStreamStatus.COMPLETE, replicationContext, airbyteMessageOrigin);
  }

  /**
   * Publishes an incomplete status event used to indicate that the source or destination has finished
   * its execution for the given stream unsuccessfully.
   *
   * @param stream The stream to be associated with the status.
   * @param replicationContext The replication context containing information about the sync.
   * @param airbyteMessageOrigin The {@link AirbyteMessage} origin that will be associated with the
   *        published incomplete status event.
   */
  public void publishIncompleteStatusEvent(final StreamDescriptor stream,
                                           final ReplicationContext replicationContext,
                                           final AirbyteMessageOrigin airbyteMessageOrigin) {
    publishStatusEvent(stream, AirbyteStreamStatus.INCOMPLETE, replicationContext,
        airbyteMessageOrigin);
  }

  /**
   * Publishes a stream status event using the provided event publisher.
   *
   * @param event A {@link ReplicationAirbyteMessageEvent} that includes the stream status
   *        information.
   */
  public void publishStatusEvent(final ReplicationAirbyteMessageEvent event) {
    eventPublisher.publishEvent(event);
  }

  /**
   * Publishes a stream status event using the provided event publisher.
   *
   * @param stream The stream to be associated with the status.
   * @param streamStatus The stream status.
   * @param replicationContext The replication context containing information about the sync.
   * @param airbyteMessageOrigin The {@link AirbyteMessage} origin that will be associated with the
   *        published status event.
   */
  public void publishStatusEvent(final StreamDescriptor stream,
                                 final AirbyteStreamStatus streamStatus,
                                 final ReplicationContext replicationContext,
                                 final AirbyteMessageOrigin airbyteMessageOrigin) {
    final AirbyteMessage airbyteMessage = AirbyteMessageUtils.createStatusTraceMessage(stream, streamStatus);

    final ReplicationAirbyteMessageEvent replicationAirbyteMessageEvent =
        new ReplicationAirbyteMessageEvent(airbyteMessageOrigin, airbyteMessage, replicationContext);
    LOGGER.debug("Publishing {} event for stream {}:{} -> {}",
        airbyteMessageOrigin, stream.getNamespace(), stream.getName(), streamStatus);
    this.publishStatusEvent(replicationAirbyteMessageEvent);
  }

}
