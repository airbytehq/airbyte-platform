/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.protocol.models.AirbyteMessage;
import jakarta.inject.Singleton;

/**
 * Serializer for Protocol V1.
 */
@Singleton
public class AirbyteMessageV1Serializer extends AirbyteMessageGenericSerializer<AirbyteMessage> {

  public AirbyteMessageV1Serializer() {
    super(AirbyteProtocolVersion.V1);
  }

}
