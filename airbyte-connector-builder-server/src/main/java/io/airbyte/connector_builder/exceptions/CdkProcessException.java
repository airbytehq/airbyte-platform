/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.exceptions;

import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.http.HttpStatus;

/**
 * Thrown when the CDK encountered an error when processing the request.
 */
public class CdkProcessException extends KnownException {

  public CdkProcessException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return HttpStatus.INTERNAL_SERVER_ERROR.getCode();
  }

}
