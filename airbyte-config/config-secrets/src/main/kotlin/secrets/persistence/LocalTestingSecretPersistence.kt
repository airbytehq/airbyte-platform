/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence.ImplementationTypes.TESTING_CONFIG_DB_TABLE
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import jakarta.transaction.Transactional
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import io.micronaut.transaction.annotation.Transactional as TransactionalAdvice

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^$TESTING_CONFIG_DB_TABLE$")
@Named("secretPersistence")
open class LocalTestingSecretPersistence(
  @Named("local-secrets") val dslContext: DSLContext,
) : SecretPersistence {
  private var initialized = false

  @Throws(DataAccessException::class)
  @Transactional
  @TransactionalAdvice("local-secrets")
  override fun initialize() {
    if (!initialized) {
      dslContext.execute("CREATE TABLE IF NOT EXISTS secrets ( coordinate TEXT PRIMARY KEY, payload TEXT);")
      initialized = true
    }
  }

  @Throws(DataAccessException::class)
  @Transactional
  @TransactionalAdvice("local-secrets")
  override fun read(coordinate: SecretCoordinate): String {
    initialize()
    val result =
      dslContext.fetch("SELECT payload FROM secrets WHERE coordinate = ?;", coordinate.fullCoordinate)
    return if (result.size == 0) {
      ""
    } else {
      result[0].getValue(0, String::class.java) ?: ""
    }
  }

  @Transactional
  @TransactionalAdvice("local-secrets")
  override fun write(
    coordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ) {
    initialize()
    dslContext
      .query(
        "INSERT INTO secrets(coordinate,payload) VALUES(?, ?) ON CONFLICT (coordinate) DO UPDATE SET payload = ?;",
        coordinate.fullCoordinate,
        payload,
        payload,
        coordinate.fullCoordinate,
      ).execute()
  }

  @Transactional
  @TransactionalAdvice("local-secrets")
  override fun delete(coordinate: AirbyteManagedSecretCoordinate) {
    initialize()
    dslContext.execute("DELETE FROM secrets WHERE coordinate = ?;", coordinate.fullCoordinate)
  }
}
