/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteAnalyticsConfigDefaultTest {
  @Inject
  private lateinit var airbyteAnalyticsConfig: AirbyteAnalyticsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(10, airbyteAnalyticsConfig.flushIntervalSec)
    assertEquals(AnalyticsTrackingStrategy.LOGGING, airbyteAnalyticsConfig.strategy)
    assertEquals("", airbyteAnalyticsConfig.writeKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-analytics-segment.yml"])
internal class AirbyteAnalyticsConfigSegmentTest {
  @Inject
  private lateinit var airbyteAnalyticsConfig: AirbyteAnalyticsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(60, airbyteAnalyticsConfig.flushIntervalSec)
    assertEquals(AnalyticsTrackingStrategy.SEGMENT, airbyteAnalyticsConfig.strategy)
    assertEquals("test-write-key", airbyteAnalyticsConfig.writeKey)
  }
}

@MicronautTest(propertySources = ["classpath:application-analytics-logging.yml"])
internal class AirbyteAnalyticsConfigLoggingTest {
  @Inject
  private lateinit var airbyteAnalyticsConfig: AirbyteAnalyticsConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(10, airbyteAnalyticsConfig.flushIntervalSec)
    assertEquals(AnalyticsTrackingStrategy.LOGGING, airbyteAnalyticsConfig.strategy)
    assertEquals("", airbyteAnalyticsConfig.writeKey)
  }
}
