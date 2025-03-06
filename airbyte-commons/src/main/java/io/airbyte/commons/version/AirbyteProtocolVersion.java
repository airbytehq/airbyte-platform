/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version;

/**
 * Represents the of a version of the Airbyte Protocol.
 */
public class AirbyteProtocolVersion {

  public static final Version DEFAULT_AIRBYTE_PROTOCOL_VERSION = new Version("0.2.0");
  public static final Version V0 = new Version("0.3.0");
  public static final Version V1 = new Version("1.0.0");

  public static final String AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME = "airbyte_protocol_version_max";
  public static final String AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME = "airbyte_protocol_version_min";

  /**
   * Parse string representation of a version to a {@link Version}.
   *
   * @param version to parse
   * @return parse version
   */
  public static Version getWithDefault(final String version) {
    if (version == null || version.isEmpty() || version.isBlank()) {
      return DEFAULT_AIRBYTE_PROTOCOL_VERSION;
    } else {
      return new Version(version);
    }
  }

}
