/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate
import io.micronaut.context.annotation.Requires
import io.micronaut.transaction.annotation.TransactionalAdvice
import jakarta.inject.Named
import jakarta.inject.Singleton
import jakarta.transaction.Transactional
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException

@Singleton
@Requires(property = "airbyte.secret.persistence", pattern = "(?i)^testing_config_db_table$")
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
    if (result.size == 0) {
      return ""
    } else {
      return result[0].getValue(0, String::class.java) ?: ""
    }
  }

  @Transactional
  @TransactionalAdvice("local-secrets")
  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    initialize()
    dslContext.query(
      "INSERT INTO secrets(coordinate,payload) VALUES(?, ?) ON CONFLICT (coordinate) DO UPDATE SET payload = ?;",
      coordinate.fullCoordinate,
      payload,
      payload,
      coordinate.fullCoordinate,
    ).execute()
  }
}
