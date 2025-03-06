/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.http.HttpStatus;

/**
 * Thrown when the server understands the content type of the request entity, and the syntax of the
 * request entity is correct, but it was unable to process the contained instructions.
 */
public class UnprocessableEntityException extends KnownException {

  public UnprocessableEntityException(final String msg) {
    super(msg);
  }

  public UnprocessableEntityException(final String msg, final Throwable t) {
    super(msg, t);
  }

  @Override
  public int getHttpCode() {
    return HttpStatus.UNPROCESSABLE_ENTITY.getCode();
  }

}
