/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;

/**
 * Default Airbyte Protocol serializer.
 *
 * @param <T> object type
 */
public class AirbyteMessageGenericSerializer<T> implements AirbyteMessageSerializer<T> {

  private final Version targetVersion;

  public AirbyteMessageGenericSerializer(Version targetVersion) {
    this.targetVersion = targetVersion;
  }

  @Override
  public String serialize(T message) {
    return Jsons.serialize(message);
  }

  @Override
  public Version getTargetVersion() {
    return targetVersion;
  }

}
