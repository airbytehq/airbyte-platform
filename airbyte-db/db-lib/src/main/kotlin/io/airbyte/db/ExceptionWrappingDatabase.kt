/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db

import java.io.IOException
import java.sql.SQLException

/**
 * Wraps a [Database] object and throwing IOExceptions instead of SQLExceptions.
 *
 * TODO: remove null [database] param, only exists for testing purposes
 */
class ExceptionWrappingDatabase(
  private val database: Database?,
) {
  /**
   * Run a query.
   *
   * @param transform query to run
   * @param <T> type of return value
   * @return value of query
   * @throws IOException exception when accessing db </T>
   * */
  @Throws(IOException::class)
  fun <T> query(transform: ContextQueryFunction<T>): T =
    try {
      database ?: throw IOException("missing database connection")
      database.query(transform)
    } catch (e: SQLException) {
      throw IOException(e)
    }

  /**
   * Execute query in a transaction.
   *
   * @param transform query to run
   * @param <T> type of return value
   * @return value of query
   * @throws IOException exception when accessing db </T>
   * */
  @Throws(IOException::class)
  fun <T> transaction(transform: ContextQueryFunction<T>): T =
    try {
      database ?: throw IOException("missing database connection")
      database.transaction(transform)
    } catch (e: SQLException) {
      throw IOException(e)
    }
}
