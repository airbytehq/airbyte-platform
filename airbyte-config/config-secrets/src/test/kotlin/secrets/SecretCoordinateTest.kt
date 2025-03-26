/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class SecretCoordinateTest {
  @Test
  fun `test retrieving the full coordinate value`() {
    val coordinate = SecretCoordinate("some_base", 1)
    Assertions.assertEquals("some_base_v1", coordinate.fullCoordinate)
  }

  @Test
  fun `test creating the coordinate from full coordinate`() {
    val fullCoordinate = "airbyte_workspace_e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2_secret_9eba44d8-51e7-48f1-bde2-619af0e42c22_v1"
    val coordinate = SecretCoordinate.fromFullCoordinate(fullCoordinate)
    Assertions.assertEquals(
      "airbyte_workspace_e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2_secret_9eba44d8-51e7-48f1-bde2-619af0e42c22",
      coordinate.coordinateBase,
    )
    Assertions.assertEquals(1L, coordinate.version)
  }
}
