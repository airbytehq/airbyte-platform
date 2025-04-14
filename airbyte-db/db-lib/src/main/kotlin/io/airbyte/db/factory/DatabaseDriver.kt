/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.factory

/**
 * Collection of JDBC driver class names and the associated JDBC URL format string.
 */
enum class DatabaseDriver(
  val driverClassName: String,
  val urlFormatString: String,
) {
  POSTGRESQL(driverClassName = "org.postgresql.Driver", urlFormatString = "jdbc:postgresql://%s:%d/%s"),
  ;

  /**
   * Returns a formated connection string
   */
  fun url(
    host: String,
    port: Int,
    database: String,
  ): String = urlFormatString.format(host, port, database)

  companion object {
    /**
     * Finds the [DatabaseDriver] enumerated value that matches the provided driver class name.
     *
     * @param driverClassName The driver class name.
     * @return The matching [DatabaseDriver] enumerated value or `null` if no match is
     * found.
     */
    internal fun findByDriverClassName(driverClassName: String): DatabaseDriver? =
      entries.firstOrNull { it.driverClassName.equals(driverClassName, ignoreCase = true) }
  }
}
