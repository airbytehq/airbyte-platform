package io.airbyte.metrics.lib

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DatadogClientConfigurationTest {
  @Test
  fun `parse constant tags`() {
    // `DD_CONSTANT_TAGS` is defined in the build.gradle.kts file
    with(DatadogClientConfiguration()) {
      assertEquals(listOf("a", "c", "d", "e"), this.constantTags)
    }
  }
}
