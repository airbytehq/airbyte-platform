/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

/**
 * The AirbyteVersion identifies the version of the database used internally by Airbyte services.
 */
class AirbyteVersion : Version {
  constructor(version: String) : super(version)

  constructor(major: String, minor: String, patch: String) : super(major, minor, patch)

  public override fun toString(): String =
    (
      "AirbyteVersion{" +
        "version='" + serialize() + '\'' +
        ", major='" + getMajorVersion() + '\'' +
        ", minor='" + getMinorVersion() + '\'' +
        ", patch='" + getPatchVersion() + '\'' +
        '}'
    )

  companion object {
    const val AIRBYTE_VERSION_KEY_NAME: String = "airbyte_version"
  }
}
