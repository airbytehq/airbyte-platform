/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.config.init.ApplyDefinitionsHelper
import io.airbyte.config.init.DeclarativeSourceUpdater
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException

/**
 * Test suite for the [DefaultPostLoadExecutor] class.
 */
internal class DefaultPostLoadExecutorTest {
  @Test
  fun testPostLoadExecution() {
    val applyDefinitionsHelper = Mockito.mock(ApplyDefinitionsHelper::class.java)
    val declarativeSourceUpdater = Mockito.mock(DeclarativeSourceUpdater::class.java)
    val authSecretInitializer = Mockito.mock(AuthKubernetesSecretInitializer::class.java)

    val postLoadExecution =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, authSecretInitializer)

    Assertions.assertDoesNotThrow { postLoadExecution.execute() }
    Mockito.verify(applyDefinitionsHelper, Mockito.times(1)).apply(false, true)
    Mockito.verify(declarativeSourceUpdater, Mockito.times(1)).apply()
    Mockito.verify(authSecretInitializer, Mockito.times(1)).initializeSecrets()
  }

  @Test
  fun testPostLoadExecutionWithException() {
    val applyDefinitionsHelper = Mockito.mock(ApplyDefinitionsHelper::class.java)
    val declarativeSourceUpdater = Mockito.mock(DeclarativeSourceUpdater::class.java)
    val authSecretInitializer = Mockito.mock(AuthKubernetesSecretInitializer::class.java)

    Mockito.doThrow(IOException("test")).`when`(applyDefinitionsHelper).apply(false, true)

    val postLoadExecution =
      DefaultPostLoadExecutor(applyDefinitionsHelper, declarativeSourceUpdater, authSecretInitializer)

    Assertions.assertThrows(IOException::class.java) { postLoadExecution.execute() }
  }
}
