/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import io.micronaut.http.HttpStatus;

/**
 * Exception when a request conflicts with the current state of the server. For example, trying to
 * accept an invitation that was already accepted.
 */
public class ConflictException extends KnownException {

  public ConflictException(final String message) {
    super(message);
  }

  public ConflictException(final String message, final Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getHttpCode() {
    return HttpStatus.CONFLICT.getCode();
  }

}
