/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.http.HttpStatus;

/**
 * Thrown when the CDK encountered an error when processing the request.
 */
public class CdkUnknownException extends KnownException {

  public CdkUnknownException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return HttpStatus.INTERNAL_SERVER_ERROR.getCode();
  }

}
