/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version;

/**
 * The AirbyteVersion identifies the version of the database used internally by Airbyte services.
 */
public class AirbyteVersion extends Version {

  public static final String AIRBYTE_VERSION_KEY_NAME = "airbyte_version";

  public AirbyteVersion(final String version) {
    super(version);
  }

  public AirbyteVersion(final String major, final String minor, final String patch) {
    super(major, minor, patch);
  }

  @Override
  public String toString() {
    return "AirbyteVersion{"
        + "version='" + version + '\''
        + ", major='" + major + '\''
        + ", minor='" + minor + '\''
        + ", patch='" + patch + '\''
        + '}';
  }

}
