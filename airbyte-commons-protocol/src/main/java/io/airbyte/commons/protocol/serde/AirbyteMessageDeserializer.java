/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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

  /**
   * Exact deserializes preserves float information by using the Java Big Decimal type. Use this to
   * preserve numeric precision.
   */
  Optional<T> deserializeExact(final String json);

  Version getTargetVersion();

}
