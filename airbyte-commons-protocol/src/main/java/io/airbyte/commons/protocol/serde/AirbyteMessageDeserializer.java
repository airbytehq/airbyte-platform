/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import io.airbyte.commons.version.Version;
import java.util.Optional;

/**
 * Airbyte Protocol deserialization interface.
 *
 * @param <T> protocol type
 */
public interface AirbyteMessageDeserializer<T> {

  Optional<T> deserialize(final String json);

  Version getTargetVersion();

}
