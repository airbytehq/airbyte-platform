/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^no_op$")
@Named("secretPersistence")
class NoOpSecretPersistence : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String = coordinate.fullCoordinate

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    return
  }

  override fun delete(coordinate: SecretCoordinate) {
    return
  }
}
