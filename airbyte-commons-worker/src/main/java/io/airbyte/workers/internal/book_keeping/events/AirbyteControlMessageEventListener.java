/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.book_keeping.events;

import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.protocol.models.AirbyteControlMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.workers.context.ReplicationContext;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.scheduling.annotation.Async;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom application listener that handles Airbyte Protocol {@link Type#CONTROL} messages produced
 * by both sources and destinations. It handles the messages synchronously to ensure that all
 * control messages are processed before continuing with replication.
 */
@Singleton
public class AirbyteControlMessageEventListener implements ApplicationEventListener<ReplicationAirbyteMessageEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteControlMessageEventListener.class);

  private final ConnectorConfigUpdater connectorConfigUpdater;

  public AirbyteControlMessageEventListener(final ConnectorConfigUpdater connectorConfigUpdater) {
    this.connectorConfigUpdater = connectorConfigUpdater;
  }

  @Override
  @Async("control-message")
  public void onApplicationEvent(final ReplicationAirbyteMessageEvent event) {
    switch (event.airbyteMessageOrigin()) {
      case DESTINATION -> acceptDstControlMessage(event.airbyteMessage().getControl(), event.replicationContext());
      case SOURCE -> acceptSrcControlMessage(event.airbyteMessage().getControl(), event.replicationContext());
      default -> LOGGER.warn("Invalid event from {} message origin for message: {}", event.airbyteMessageOrigin(), event.airbyteMessage());
    }
  }

  @Override
  public boolean supports(final ReplicationAirbyteMessageEvent event) {
    return Type.CONTROL.equals(event.airbyteMessage().getType());
  }

  private void acceptDstControlMessage(final AirbyteControlMessage controlMessage, final ReplicationContext context) {
    switch (controlMessage.getType()) {
      case CONNECTOR_CONFIG -> connectorConfigUpdater.updateDestination(context.destinationId(), controlMessage.getConnectorConfig().getConfig());
      default -> LOGGER.debug("Control message type {} not supported.", controlMessage.getType());
    }
  }

  private void acceptSrcControlMessage(final AirbyteControlMessage controlMessage, final ReplicationContext context) {
    switch (controlMessage.getType()) {
      case CONNECTOR_CONFIG -> connectorConfigUpdater.updateSource(context.sourceId(), controlMessage.getConnectorConfig().getConfig());
      default -> LOGGER.debug("Control message type {} not supported.", controlMessage.getType());
    }
  }

}
