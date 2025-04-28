/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.config.init.ApplyDefinitionsHelper
import io.airbyte.config.init.DeclarativeSourceUpdater
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.IOException

/**
 * Test suite for the [DefaultPostLoadExecutor] class.
 */
internal class DefaultPostLoadExecutorTest {
  @Test
  fun testPostLoadExecution() {
    val applyDefinitionsHelper = mockk<ApplyDefinitionsHelper>()
    val declarativeSourceUpdater = mockk<DeclarativeSourceUpdater>()

    // Set up expected behavior
    every { applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true) } returns Unit
    every { declarativeSourceUpdater.apply() } returns Unit

    val postLoadExecution =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater)

    Assertions.assertDoesNotThrow { postLoadExecution.execute() }
    verify(exactly = 1) { applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true) }
    verify(exactly = 1) { declarativeSourceUpdater.apply() }
  }

  @Test
  fun testPostLoadExecutionWithException() {
    val applyDefinitionsHelper = mockk<ApplyDefinitionsHelper>()
    val declarativeSourceUpdater = mockk<DeclarativeSourceUpdater>()

    every { applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true) } throws IOException("test")

    val postLoadExecution =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater)

    Assertions.assertThrows(IOException::class.java) { postLoadExecution.execute() }
  }
}
