/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CdkVersionProviderTest {

  private CdkVersionProvider cdkVersionProvider;

  @BeforeEach
  void setup() {
    cdkVersionProvider = new CdkVersionProvider();
  }

  @Test
  void testGetVersionFromResources() {
    assertEquals("12.13.14", cdkVersionProvider.getCdkVersion());
  }

}
