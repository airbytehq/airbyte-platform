/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

internal class LogSourceTest {
  @ParameterizedTest
  @EnumSource(LogSource::class)
  fun testLogSource(logSource: LogSource) {
    val expected = logSource.displayName
    assertEquals(expected, logSource.toMdc()[LOG_SOURCE_MDC_KEY])
  }

  @ParameterizedTest
  @EnumSource(LogSource::class)
  fun testFindLogSource(logSource: LogSource) {
    assertEquals(logSource, LogSource.find(logSource.displayName))
  }
}
