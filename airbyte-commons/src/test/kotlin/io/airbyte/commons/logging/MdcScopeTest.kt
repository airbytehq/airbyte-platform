/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.slf4j.MDC

internal class MdcScopeTest {
  @BeforeEach
  fun init() {
    MDC.setContextMap(originalMap)
  }

  @Test
  @DisplayName("The MDC context is properly overrided")
  fun testMDCModified() {
    MdcScope.Builder().setExtraMdcEntries(modificationInMDC).build().use { _ ->
      val mdcState = MDC.getCopyOfContextMap()
      Assertions.assertThat(mdcState).containsExactlyInAnyOrderEntriesOf(
        mapOf("test" to "entry", "new" to "will be added", "testOverride" to "will override"),
      )
    }
  }

  @Test
  @DisplayName("The MDC context is properly restored")
  fun testMDCRestore() {
    MdcScope
      .Builder()
      .setExtraMdcEntries(modificationInMDC)
      .build()
      .use { _ -> }
    val mdcState = MDC.getCopyOfContextMap()

    Assertions.assertThat(mdcState).containsAllEntriesOf(originalMap)
    Assertions.assertThat(mdcState).doesNotContainKey("new")
  }

  companion object {
    private val originalMap = mapOf("test" to "entry", "testOverride" to "should be overridden")

    private val modificationInMDC = mapOf<String?, String?>("new" to "will be added", "testOverride" to "will override")
  }
}
