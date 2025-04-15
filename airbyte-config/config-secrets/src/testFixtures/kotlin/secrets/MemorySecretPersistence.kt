/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence

/**
 * Map-based implementation of a [SecretPersistence] used for unit testing.
 */
class MemorySecretPersistence : SecretPersistence {
  private val secretMap: MutableMap<AirbyteManagedSecretCoordinate, String> = mutableMapOf()

  override fun read(coordinate: SecretCoordinate): String = secretMap[coordinate] ?: ""

  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    secretMap[coordinate] = payload
  }

  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    secretMap.remove(coordinate)
  }

  override fun disable(coordinate: AirbyteManagedSecretCoordinate) {
    // Mimic the behavior of the real implementation.
    secretMap.remove(coordinate)
  }

  val map: Map<AirbyteManagedSecretCoordinate, String>
    get() = secretMap.toMutableMap()
}
