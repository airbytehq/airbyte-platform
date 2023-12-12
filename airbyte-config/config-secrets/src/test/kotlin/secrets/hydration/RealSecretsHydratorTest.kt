/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.hydration

import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class RealSecretsHydratorTest {
  @Test
  fun `test secret hydration`() {
    val coordinate = "secret_coordinate_v1"
    val secretValue = "secret_value"
    val secretPersistence: SecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns secretValue
    val hydrator = RealSecretsHydrator(secretPersistence)
    val partialConfig = Jsons.jsonNode(mapOf("_secret" to coordinate))
    val hydratedConfig = hydrator.hydrateFromDefaultSecretPersistence(partialConfig)
    Assertions.assertEquals(secretValue, hydratedConfig.asText())
  }

  @Test
  fun `test coordinate secret hydration`() {
    val coordinate = "secret_coordinate_v1"
    val secretValue = "secret_value"
    val secret = mapOf("config" to secretValue)
    val secretPersistence: SecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns Jsons.serialize(secret)
    val hydrator = RealSecretsHydrator(secretPersistence)
    val secretCoordinate = Jsons.jsonNode(mapOf("_secret" to coordinate))
    val hydratedCoordinate = hydrator.hydrateSecretCoordinateFromDefaultSecretPersistence(secretCoordinate)
    Assertions.assertEquals(secret["config"], hydratedCoordinate.get("config").asText())
  }
}
