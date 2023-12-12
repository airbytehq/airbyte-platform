package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^no_op$")
@Named("secretPersistence")
class NoOpSecretPersistence : SecretPersistence {
  override fun read(coordinate: SecretCoordinate): String {
    return coordinate.fullCoordinate
  }

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    return
  }
}
