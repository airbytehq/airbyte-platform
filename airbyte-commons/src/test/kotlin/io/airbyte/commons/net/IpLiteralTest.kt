/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.net

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource

class IpLiteralTest {
  @ParameterizedTest
  @CsvSource("10.0.0.1,10.0.0.1", "8.8.8.8,8.8.8.8", "::1,0:0:0:0:0:0:0:1", "[::1],0:0:0:0:0:0:0:1", "[10.0.0.1],10.0.0.1")
  fun `parses ip literals`(
    host: String,
    expected: String,
  ) {
    assertEquals(expected, parseIpLiteral(host)?.hostAddress)
  }

  @ParameterizedTest
  @ValueSource(
    strings = [
      "", "localhost", "db.example.com", "999.0.0.1", "256.1.1.1", "10.0.0", "1:2:3:4:5:6:7:8:9",
      "fe80::1%nope", "fe80::1%1", "[10.0.0.1", "10.0.0.1]",
    ],
  )
  fun `returns null for hostnames, malformed literals and zone ids`(host: String) {
    assertNull(parseIpLiteral(host))
  }
}
