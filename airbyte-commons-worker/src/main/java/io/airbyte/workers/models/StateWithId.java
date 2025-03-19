/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

public class StateWithId {

  private static final String ID = "id";

  public static AirbyteMessage attachIdToStateMessageFromSource(final AirbyteMessage message) {
    if (message.getType() == AirbyteMessage.Type.STATE) {
      message.getState().setAdditionalProperty(ID, StateIdProvider.getNextId());
    }
    return message;
  }

  public static AirbyteStateMessage attachIdToStateMessageFromSource(final AirbyteStateMessage message) {
    message.setAdditionalProperty(ID, StateIdProvider.getNextId());
    return message;
  }

  public static OptionalInt getIdFromStateMessage(final AirbyteMessage message) {
    if (message.getType() == AirbyteMessage.Type.STATE) {
      return OptionalInt.of(getIdFromStateMessage(message.getState()));
    }
    return OptionalInt.empty();
  }

  public static int getIdFromStateMessage(final AirbyteStateMessage message) {
    return (int) message.getAdditionalProperties().get(ID);
  }

  private static class StateIdProvider {

    private static final AtomicInteger id = new AtomicInteger(0);

    public static int getNextId() {
      return id.incrementAndGet();
    }

  }

}
