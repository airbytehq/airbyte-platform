/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.version.Version;

/**
 * Airbyte Protocol deserialization interface.
 *
 * @param <T> protocol type
 */
public interface AirbyteMessageDeserializer<T> {

  T deserialize(final JsonNode json);

  Version getTargetVersion();

}
