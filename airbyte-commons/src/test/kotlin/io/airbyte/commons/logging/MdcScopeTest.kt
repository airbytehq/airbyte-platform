/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.util.Map

internal class MdcScopeTest {
  @BeforeEach
  fun init() {
    MDC.setContextMap(originalMap)
  }

  @Test
  @DisplayName("The MDC context is properly overrided")
  fun testMDCModified() {
    MdcScope.Builder().setExtraMdcEntries(modificationInMDC).build().use { ignored ->
      val mdcState = MDC.getCopyOfContextMap()
      Assertions.assertThat<String?, String?>(mdcState).containsExactlyInAnyOrderEntriesOf(
        Map.of<String?, String?>("test", "entry", "new", "will be added", "testOverride", "will override"),
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
      .use { ignored -> }
    val mdcState = MDC.getCopyOfContextMap()

    Assertions.assertThat<String?, String?>(mdcState).containsAllEntriesOf(originalMap)
    Assertions.assertThat<String?, String?>(mdcState).doesNotContainKey("new")
  }

  companion object {
    private val originalMap: MutableMap<String?, String?> = Map.of<String?, String?>("test", "entry", "testOverride", "should be overrided")

    private val modificationInMDC: MutableMap<String?, String?> =
      Map.of<String?, String?>("new", "will be added", "testOverride", "will override")
  }
}
