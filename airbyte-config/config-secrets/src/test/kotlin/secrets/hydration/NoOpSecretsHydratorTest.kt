/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.hydration

import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class NoOpSecretsHydratorTest {
  @Test
  fun `test secret hydration`() {
    val coordinate = "secret_coordinate_v1"
    val hydrator = NoOpSecretsHydrator()
    val partialConfig = Jsons.jsonNode(mapOf("_secret" to coordinate))
    val hydratedConfig = hydrator.hydrateFromDefaultSecretPersistence(partialConfig)
    Assertions.assertEquals(partialConfig, hydratedConfig)
  }

  @Test
  fun `test coordinate secret hydration`() {
    val coordinate = "secret_coordinate_v1"
    val hydrator = NoOpSecretsHydrator()
    val secretCoordinate = Jsons.jsonNode(mapOf("_secret" to coordinate))
    val hydratedCoordinate = hydrator.hydrateSecretCoordinateFromDefaultSecretPersistence(secretCoordinate)
    Assertions.assertEquals(secretCoordinate, hydratedCoordinate)
  }
}
