/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Simple exception for returning a 400 from an HTTP endpoint.
 */
public class BadRequestException extends KnownException {

  public BadRequestException(final String msg) {
    super(msg);
  }

  public BadRequestException(final String message, final Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getHttpCode() {
    return 400;
  }

}
