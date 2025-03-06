/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Wraps a {@link Database} object and throwing IOExceptions instead of SQLExceptions.
 */
public class ExceptionWrappingDatabase {

  private final Database database;

  public ExceptionWrappingDatabase(final Database database) {
    this.database = database;
  }

  /**
   * Run a query.
   *
   * @param transform query to run
   * @param <T> type of return value
   * @return value of query
   * @throws IOException exception when accessing db
   */
  public <T> T query(final ContextQueryFunction<T> transform) throws IOException {
    try {
      return database.query(transform);
    } catch (final SQLException e) {
      throw new IOException(e);
    }
  }

  /**
   * Execute query in a transaction.
   *
   * @param transform query to run
   * @param <T> type of return value
   * @return value of query
   * @throws IOException exception when accessing db
   */
  public <T> T transaction(final ContextQueryFunction<T> transform) throws IOException {
    try {
      return database.transaction(transform);
    } catch (final SQLException e) {
      throw new IOException(e);
    }
  }

}
