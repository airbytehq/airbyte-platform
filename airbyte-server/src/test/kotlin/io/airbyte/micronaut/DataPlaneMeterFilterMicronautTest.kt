/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut

import io.airbyte.commons.server.metrics.PrettifyDataplaneMetricTagsMeterFilterBuilder
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Property
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.runtime.event.ApplicationStartupEvent
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull

@MicronautTest(
  rebuildContext = true,
  startApplication = false,
)
@Property(name = "micronaut.metrics.enabled", value = "true")
class DataPlaneMeterFilterMicronautTest {
  @Inject
  lateinit var meterRegistry: MeterRegistry

  @Inject
  lateinit var startUpEventListeners: List<ApplicationEventListener<ApplicationStartupEvent>>

  @Test
  fun `verify that if metrics are enabled, one of the startup event listener will register the dataplane metric prettifier filter`() {
    assertNotNull(meterRegistry)

    // Verify the bean exists in the startup event listeners
    val beans = startUpEventListeners.filterIsInstance<PrettifyDataplaneMetricTagsMeterFilterBuilder>()
    assertEquals(1, beans.size, "Expected 1 PrettifyDataplaneMetricTagsMeterFilterBuilder bean")
  }
}
