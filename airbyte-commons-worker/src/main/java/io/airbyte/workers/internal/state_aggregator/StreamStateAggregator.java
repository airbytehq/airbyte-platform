/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.state_aggregator;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.State;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.StreamDescriptor;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates stream states for a connection. Has all the stream states for a connection.
 */
class StreamStateAggregator implements StateAggregator {

  Map<StreamDescriptor, AirbyteStateMessage> aggregatedState = new HashMap<>();

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void ingest(final AirbyteStateMessage stateMessage) {
    /*
     * The destination emit a Legacy state in order to be retro-compatible with old platform. If we are
     * running this code, we know that the platform has been upgraded and we can thus discard the legacy
     * state. Keeping the legacy state is causing issue because of its size
     * (https://github.com/airbytehq/oncall/issues/731)
     */
    stateMessage.setData(null);
    aggregatedState.put(stateMessage.getStream().getStreamDescriptor(), stateMessage);
  }

  @Override
  public void ingest(final StateAggregator stateAggregator) {
    if (stateAggregator instanceof StreamStateAggregator) {
      for (final var message : ((StreamStateAggregator) stateAggregator).aggregatedState.values()) {
        ingest(message);
      }
    } else {
      throw new IllegalArgumentException(
          "Got an incompatible StateAggregator: " + stateAggregator.getClass().getName() + ", expected StreamStateAggregator");
    }
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public State getAggregated() {

    return new State()
        .withState(
            Jsons.jsonNode(aggregatedState.values()));
  }

  @Override
  public boolean isEmpty() {
    return aggregatedState.isEmpty();
  }

}
