/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import java.sql.SQLException;

/**
 * Exception when a SQL operation was prevented due to the current state of the database.
 */
public class SQLOperationNotAllowedException extends SQLException {

  public SQLOperationNotAllowedException(final String message) {
    super(message);
  }

}
