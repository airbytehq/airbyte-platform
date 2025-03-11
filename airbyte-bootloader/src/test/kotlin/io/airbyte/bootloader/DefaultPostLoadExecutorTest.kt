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
    val authSecretInitializer = mockk<AuthKubernetesSecretInitializer>()

    // Set up expected behavior
    every { applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true) } returns Unit
    every { declarativeSourceUpdater.apply() } returns Unit
    every { authSecretInitializer.initializeSecrets() } returns Unit

    val postLoadExecution =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, authSecretInitializer)

    Assertions.assertDoesNotThrow { postLoadExecution.execute() }
    verify(exactly = 1) { applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true) }
    verify(exactly = 1) { declarativeSourceUpdater.apply() }
    verify(exactly = 1) { authSecretInitializer.initializeSecrets() }
  }

  @Test
  fun testPostLoadExecutionWithException() {
    val applyDefinitionsHelper = mockk<ApplyDefinitionsHelper>()
    val declarativeSourceUpdater = mockk<DeclarativeSourceUpdater>()
    val authSecretInitializer = mockk<AuthKubernetesSecretInitializer>()

    every { applyDefinitionsHelper.apply(updateAll = false, reImportVersionInUse = true) } throws IOException("test")

    val postLoadExecution =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, authSecretInitializer)

    Assertions.assertThrows(IOException::class.java) { postLoadExecution.execute() }
  }
}
