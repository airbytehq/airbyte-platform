/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.ExternalSecretCoordinate
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class SecretCoordinateTest {
  @Test
  fun `test retrieving the full coordinate value`() {
    val coordinate = AirbyteManagedSecretCoordinate("airbyte_some_base", 1)
    Assertions.assertEquals("airbyte_some_base_v1", coordinate.fullCoordinate)
  }

  @Test
  fun `test creating an airbyte-managed coordinate from full coordinate`() {
    val fullCoordinate = "airbyte_workspace_e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2_secret_9eba44d8-51e7-48f1-bde2-619af0e42c22_v1"
    val coordinate = SecretCoordinate.fromFullCoordinate(fullCoordinate)
    Assertions.assertTrue(coordinate is AirbyteManagedSecretCoordinate)
    Assertions.assertEquals(
      "airbyte_workspace_e0eb0554-ffe0-4e9c-9dc0-ed7f52023eb2_secret_9eba44d8-51e7-48f1-bde2-619af0e42c22",
      (coordinate as AirbyteManagedSecretCoordinate).coordinateBase,
    )
    Assertions.assertEquals(1L, coordinate.version)
  }

  @Test
  fun `test creating an external coordinate from full coordinate`() {
    val fullCoordinate = "some_external_secret_that_does_not_start_with_airbyte_prefix_but_ends_with_v1"
    val coordinate = SecretCoordinate.fromFullCoordinate(fullCoordinate)
    Assertions.assertTrue(coordinate is ExternalSecretCoordinate)
    Assertions.assertEquals(fullCoordinate, (coordinate as ExternalSecretCoordinate).fullCoordinate)
  }
}
