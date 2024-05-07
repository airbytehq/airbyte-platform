/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class CdkVersionProviderTest {
  private lateinit var cdkVersionProvider: CdkVersionProvider

  @BeforeEach
  fun setup() {
    cdkVersionProvider = CdkVersionProvider()
  }

  @Test
  fun `get cdk version from resources`() {
    Assertions.assertEquals("12.13.14", cdkVersionProvider.cdkVersion)
  }
}
