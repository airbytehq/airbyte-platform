/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

/**
 * Represents the of a version of the Airbyte Protocol.
 */
object AirbyteProtocolVersion {
  @JvmField
  val DEFAULT_AIRBYTE_PROTOCOL_VERSION: Version = Version("0.2.0")
  val V0: Version = Version("0.3.0")
  val V1: Version = Version("1.0.0")

  const val AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME: String = "airbyte_protocol_version_max"
  const val AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME: String = "airbyte_protocol_version_min"

  /**
   * Parse string representation of a version to a [Version].
   *
   * @param version to parse
   * @return parse version
   */
  fun getWithDefault(version: String?): Version =
    if (version == null || version.isEmpty() || version.isBlank()) {
      DEFAULT_AIRBYTE_PROTOCOL_VERSION
    } else {
      Version(version)
    }
}
