/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Value already exists and must be unique.
 */
public class ValueConflictKnownException extends KnownException {

  public ValueConflictKnownException(final String message) {
    super(message);
  }

  public ValueConflictKnownException(final String message, final Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getHttpCode() {
    return 409;
  }

}
