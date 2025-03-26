/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import java.util.Optional;

/**
 * Default Airbyte Protocol deserializer.
 *
 * @param <T> object type
 */
public class AirbyteMessageGenericDeserializer<T> implements AirbyteMessageDeserializer<T> {

  final Version targetVersion;
  final Class<T> typeClass;

  public AirbyteMessageGenericDeserializer(final Version targetVersion, final Class<T> typeClass) {
    this.targetVersion = targetVersion;
    this.typeClass = typeClass;
  }

  @Override
  public Optional<T> deserializeExact(final String json) {
    return Jsons.tryDeserializeExact(json, typeClass);
  }

  @Override
  public Version getTargetVersion() {
    return targetVersion;
  }

}
