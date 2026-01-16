/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.ksp.config

import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

internal class AirbyteConfigurationProcessorProviderTest {
  @Test
  fun testCreation() {
    val provider = AirbyteConfigurationProcessorProvider()
    val environment =
      mockk<SymbolProcessorEnvironment> {
        every { codeGenerator } returns mockk<CodeGenerator>()
        every { logger } returns mockk<KSPLogger>()
      }
    val processor = provider.create(environment)
    assertNotNull(processor)
  }

  @Test
  fun testProviderMetadata() {
    val metadata =
      this::class.java
        .getResourceAsStream(
          "/META-INF/services/com.google.devtools.ksp.processing.SymbolProcessorProvider",
        )?.bufferedReader()
        ?.use {
          it.readText()
        }
    assertEquals(AirbyteConfigurationProcessorProvider::class.qualifiedName, metadata)
  }
}
