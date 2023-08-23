/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.internal.AirbyteMapper;

/**
 * Stub mapper testing what happens without any mapping.
 */
public class StubAirbyteMapper implements AirbyteMapper {

  @Override
  public ConfiguredAirbyteCatalog mapCatalog(final ConfiguredAirbyteCatalog catalog) {
    return null;
  }

  @Override
  public AirbyteMessage mapMessage(final AirbyteMessage message) {
    return message;
  }

  @Override
  public AirbyteMessage revertMap(final AirbyteMessage message) {
    return message;
  }

}
