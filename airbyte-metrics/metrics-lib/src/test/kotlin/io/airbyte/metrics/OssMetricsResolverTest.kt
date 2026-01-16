/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

internal class OssMetricsResolverTest {
  @Test
  fun testResolve() {
    val resolver = OssMetricsResolver()
    assertEquals(OssMetricsRegistry.WORKLOAD_MONITOR_RUN, resolver.resolve(OssMetricsRegistry.WORKLOAD_MONITOR_RUN.getMetricName()))
    assertNull(resolver.resolve("unknown"))
  }
}
