/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.spi.ILoggingEvent
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private class AirbyteStorageMDCBasedDiscriminatorTest {
  private lateinit var discriminator: AirbyteStorageMDCBasedDiscriminator

  @BeforeEach
  fun setup() {
    discriminator = AirbyteStorageMDCBasedDiscriminator(mdcValueExtractor = { mdc -> mdc[DEFAULT_JOB_LOG_PATH_MDC_KEY] ?: "" })
  }

  @Test
  fun testLoggingEventWithPathInContext() {
    val jobPath = "/some/job/path"
    val context = mapOf(DEFAULT_JOB_LOG_PATH_MDC_KEY to jobPath)
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns context
      }

    val discriminatorValue = discriminator.getDiscriminatingValue(event = loggingEvent)
    assertEquals(jobPath, discriminatorValue)
  }

  @Test
  fun testLoggingEventWithEmptyContext() {
    val context = emptyMap<String, String>()
    val loggingEvent =
      mockk<ILoggingEvent> {
        every { mdcPropertyMap } returns context
      }

    val discriminatorValue = discriminator.getDiscriminatingValue(event = loggingEvent)
    assertEquals("", discriminatorValue)
  }
}
