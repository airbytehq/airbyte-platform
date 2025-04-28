/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.jdbc

/**
 * Shared JDBC utils.
 */
object JdbcUtils {
  const val HOST_KEY: String = "host"
  const val PORT_KEY: String = "port"
  const val DATABASE_KEY: String = "database"
  const val SCHEMA_KEY: String = "schema"

  const val USERNAME_KEY: String = "username"
  const val PASSWORD_KEY: String = "password"
  const val SSL_KEY: String = "ssl"
  const val JDBC_URL_PARAMS: String = "jdbc_url_params"
}
