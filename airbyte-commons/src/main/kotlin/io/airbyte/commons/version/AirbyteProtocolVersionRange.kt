/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

/**
 * Describes the range between two [Version]s.
 *
 * @param min version
 * @param max version
 */
@JvmRecord
data class AirbyteProtocolVersionRange(
  val min: Version,
  val max: Version,
) {
  /**
   * Test if the provided version is inside the range.
   *
   * @param version to test
   * @return true if within range. otherwise, false.
   */
  fun isSupported(version: Version): Boolean {
    val major = getMajor(version)
    return getMajor(min) <= major && major <= getMajor(max)
  }

  companion object {
    private fun getMajor(v: Version): Int = v.getMajorVersion()!!.toInt()
  }
}
