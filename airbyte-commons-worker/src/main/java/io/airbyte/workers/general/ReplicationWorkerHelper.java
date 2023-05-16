/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.protocol.models.AirbyteControlMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.book_keeping.MessageTracker;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEvent;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.sync_persistence.SyncPersistence;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the business logic that has been extracted from the DefaultReplicationWorker.
 * <p>
 * Expected lifecycle of this object is a sync.
 * <p>
 * This needs to be broken down further by responsibility. Until it happens, it holds the processing
 * of the DefaultReplicationWorker that isn't control flow related.
 */
class ReplicationWorkerHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationWorkerHelper.class);

  private final AirbyteMessageDataExtractor airbyteMessageDataExtractor;
  private final FieldSelector fieldSelector;
  private final AirbyteMapper mapper;
  private final MessageTracker messageTracker;
  private final SyncPersistence syncPersistence;
  private final ConnectorConfigUpdater connectorConfigUpdater;
  private final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;
  private long recordsRead;
  private StreamDescriptor currentDestinationStream = null;
  private StreamDescriptor currentSourceStream = null;

  public ReplicationWorkerHelper(
                                 final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                 final FieldSelector fieldSelector,
                                 final AirbyteMapper mapper,
                                 final MessageTracker messageTracker,
                                 final SyncPersistence syncPersistence,
                                 final ConnectorConfigUpdater connectorConfigUpdater,
                                 final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper) {
    this.airbyteMessageDataExtractor = airbyteMessageDataExtractor;
    this.fieldSelector = fieldSelector;
    this.mapper = mapper;
    this.messageTracker = messageTracker;
    this.syncPersistence = syncPersistence;
    this.connectorConfigUpdater = connectorConfigUpdater;
    this.replicationAirbyteMessageEventPublishingHelper = replicationAirbyteMessageEventPublishingHelper;

    this.recordsRead = 0L;
  }

  public void beforeReplication(final ConfiguredAirbyteCatalog catalog) {
    fieldSelector.populateFields(catalog);
  }

  public void endOfSource(final ReplicationContext replicationContext) {
    LOGGER.info("Total records read: {} ({})", recordsRead,
        FileUtils.byteCountToDisplaySize(messageTracker.getSyncStatsTracker().getTotalBytesEmitted()));

    fieldSelector.reportMetrics(replicationContext.sourceId());
  }

  public Optional<AirbyteMessage> processMessageFromSource(final AirbyteMessage airbyteMessage,
                                                           final ReplicationContext replicationContext,
                                                           final boolean handleStreamStatus) {
    final StreamDescriptor previousStream = currentSourceStream;
    currentSourceStream = airbyteMessageDataExtractor.extractStreamDescriptor(airbyteMessage, previousStream);
    if (currentSourceStream != null) {
      LOGGER.debug("SOURCE > The current stream is {}:{}", currentSourceStream.getNamespace(), currentSourceStream.getName());
    }

    fieldSelector.filterSelectedFields(airbyteMessage);
    fieldSelector.validateSchema(airbyteMessage);

    final AirbyteMessage message = mapper.mapMessage(airbyteMessage);

    messageTracker.acceptFromSource(message);

    if (handleStreamStatus && shouldPublishMessage(airbyteMessage)) {
      LOGGER.debug("Publishing source event for stream {}:{}...", currentSourceStream.getNamespace(), currentSourceStream.getName());
      replicationAirbyteMessageEventPublishingHelper
          .publishStatusEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, message, replicationContext));
    } else {
      try {
        if (message.getType() == Type.CONTROL) {
          acceptSrcControlMessage(replicationContext.sourceId(), message.getControl());
        }
      } catch (final Exception e) {
        LOGGER.error("Error updating source configuration", e);
      }
    }

    recordsRead += 1;

    if (recordsRead % 5000 == 0) {
      LOGGER.info("Records read: {} ({})", recordsRead,
          FileUtils.byteCountToDisplaySize(messageTracker.getSyncStatsTracker().getTotalBytesEmitted()));
    }

    return Optional.of(message);
  }

  public void processMessageFromDestination(final AirbyteMessage message,
                                            final boolean commitStatesAsap,
                                            final boolean handleStreamStatus,
                                            final ReplicationContext replicationContext) {
    LOGGER.info("State in DefaultReplicationWorker from destination: {}", message);
    final StreamDescriptor previousStream = currentDestinationStream;
    currentDestinationStream = airbyteMessageDataExtractor.extractStreamDescriptor(message, previousStream);
    if (currentDestinationStream != null) {
      LOGGER.debug("DESTINATION > The current stream is {}:{}", currentDestinationStream.getNamespace(), currentDestinationStream.getName());
    }

    // If the worker has moved on to the next stream, ensure that a completed status is sent
    // for the previously tracked stream.
    if (handleStreamStatus && previousStream != null && !previousStream.equals(currentDestinationStream)) {
      replicationAirbyteMessageEventPublishingHelper.publishCompleteStatusEvent(currentDestinationStream, replicationContext,
          AirbyteMessageOrigin.DESTINATION);
    }

    messageTracker.acceptFromDestination(message);
    if (commitStatesAsap && message.getType() == Type.STATE) {
      syncPersistence.persist(replicationContext.connectionId(), message.getState());
    }

    if (handleStreamStatus && shouldPublishMessage(message)) {
      LOGGER.debug("Publishing destination event for stream {}:{}...", currentDestinationStream.getNamespace(), currentDestinationStream.getName());
      replicationAirbyteMessageEventPublishingHelper
          .publishStatusEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, message, replicationContext));
    } else {
      try {
        if (message.getType() == Type.CONTROL) {
          acceptDstControlMessage(replicationContext.destinationId(), message.getControl());
        }
      } catch (final Exception e) {
        LOGGER.error("Error updating destination configuration", e);
      }
    }
  }

  public StreamDescriptor getCurrentDestinationStream() {
    return currentDestinationStream;
  }

  public StreamDescriptor getCurrentSourceStream() {
    return currentSourceStream;
  }

  private void acceptSrcControlMessage(final UUID sourceId,
                                       final AirbyteControlMessage controlMessage) {
    if (controlMessage.getType() == AirbyteControlMessage.Type.CONNECTOR_CONFIG) {
      connectorConfigUpdater.updateSource(sourceId, controlMessage.getConnectorConfig().getConfig());
    }
  }

  private void acceptDstControlMessage(final UUID destinationId,
                                       final AirbyteControlMessage controlMessage) {
    if (controlMessage.getType() == AirbyteControlMessage.Type.CONNECTOR_CONFIG) {
      connectorConfigUpdater.updateDestination(destinationId, controlMessage.getConnectorConfig().getConfig());
    }
  }

  /**
   * Tests whether the {@link AirbyteMessage} should be published via Micronaut's event publishing
   * mechanism.
   *
   * @param airbyteMessage The {@link AirbyteMessage} to be considered for event publishing.
   * @return {@code True} if the message should be published, false otherwise.
   */
  private static boolean shouldPublishMessage(final AirbyteMessage airbyteMessage) {
    return Type.CONTROL.equals(airbyteMessage.getType())
        || (Type.TRACE.equals(airbyteMessage.getType()) && AirbyteTraceMessage.Type.STREAM_STATUS.equals(airbyteMessage.getTrace().getType()));
  }

}
