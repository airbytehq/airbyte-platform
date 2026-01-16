/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.logging.LogLevel
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteLoggingConfigDefaultTest {
  @Inject
  private lateinit var airbyteLoggingConfig: AirbyteLoggingConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(DEFAULT_LOG_TAIL_SIZE, airbyteLoggingConfig.client.logTailSize)
    assertEquals(LogLevel.INFO, airbyteLoggingConfig.logLevel)
    assertEquals("", airbyteLoggingConfig.s3PathStyleAccess)
  }
}

@MicronautTest(propertySources = ["classpath:application-logging.yml"])
internal class AirbyteLoggingConfigTest {
  @Inject
  private lateinit var airbyteLoggingConfig: AirbyteLoggingConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(50, airbyteLoggingConfig.client.logTailSize)
    assertEquals(LogLevel.DEBUG, airbyteLoggingConfig.logLevel)
    assertEquals("test", airbyteLoggingConfig.s3PathStyleAccess)
  }
}
