/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Exception when an operation is correctly formatted and syntactically valid, but not allowed due
 * to the current state of the system. For example, deletion of a resource that should not be
 * deleted according to business logic.
 */
public class OperationNotAllowedException extends KnownException {

  public OperationNotAllowedException(final String message) {
    super(message);
  }

  public OperationNotAllowedException(final String message, final Exception cause) {
    super(message, cause);
  }

  @Override
  public int getHttpCode() {
    return 403;
  }

}
