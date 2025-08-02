/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.version

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class AirbyteProtocolVersionRangeTest {
  @Test
  fun checkRanges() {
    val range = AirbyteProtocolVersionRange(Version("1.2.3"), Version("4.3.2"))
    Assertions.assertTrue(range.isSupported(Version("2.0.0")))
    Assertions.assertTrue(range.isSupported(Version("1.2.3")))
    Assertions.assertTrue(range.isSupported(Version("4.3.2")))

    // We should only be requiring major to be within range
    Assertions.assertTrue(range.isSupported(Version("1.0.0")))
    Assertions.assertTrue(range.isSupported(Version("4.4.0")))

    Assertions.assertFalse(range.isSupported(Version("0.2.3")))
    Assertions.assertFalse(range.isSupported(Version("5.0.0")))
  }

  @Test
  fun checkRangeWithOnlyOneMajor() {
    val range = AirbyteProtocolVersionRange(Version("2.0.0"), Version("2.1.2"))

    Assertions.assertTrue(range.isSupported(Version("2.0.0")))
    Assertions.assertTrue(range.isSupported(Version("2.5.0")))

    Assertions.assertFalse(range.isSupported(Version("1.0.0")))
    Assertions.assertFalse(range.isSupported(Version("3.0.0")))
  }
}
