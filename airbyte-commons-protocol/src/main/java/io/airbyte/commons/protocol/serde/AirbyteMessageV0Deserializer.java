/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.protocol.models.AirbyteMessage;
import jakarta.inject.Singleton;

/**
 * Deserializer for Protocol V0.
 */
@Singleton
public class AirbyteMessageV0Deserializer extends AirbyteMessageGenericDeserializer<AirbyteMessage> {

  public AirbyteMessageV0Deserializer() {
    super(AirbyteProtocolVersion.V0, AirbyteMessage.class);
  }

}
