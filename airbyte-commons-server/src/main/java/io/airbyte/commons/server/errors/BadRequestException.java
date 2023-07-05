/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Simple exception for returning a 400 from an HTTP endpoint.
 */
public class BadRequestException extends KnownException {

  public BadRequestException(final String msg) {
    super(msg);
  }

  @Override
  public int getHttpCode() {
    return 400;
  }

}
