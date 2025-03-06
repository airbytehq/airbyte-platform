/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version;

/**
 * Describes the range between two {@link Version}s.
 *
 * @param min version
 * @param max version
 */
public record AirbyteProtocolVersionRange(Version min, Version max) {

  /**
   * Test if the provided version is inside the range.
   *
   * @param version to test
   * @return true if within range. otherwise, false.
   */
  public boolean isSupported(final Version version) {
    final Integer major = getMajor(version);
    return getMajor(min) <= major && major <= getMajor(max);
  }

  private static Integer getMajor(final Version v) {
    return Integer.valueOf(v.getMajorVersion());
  }

}
