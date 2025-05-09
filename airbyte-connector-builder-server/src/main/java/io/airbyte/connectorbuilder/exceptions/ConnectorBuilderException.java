/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.exceptions;

import io.airbyte.commons.server.errors.KnownException;
import io.micronaut.http.HttpStatus;

/**
 * Thrown when the Connector Builder encountered an error when processing the request.
 */
public class ConnectorBuilderException extends KnownException {

  public ConnectorBuilderException(final String message, final Exception exception) {
    super(message, exception);
  }

  public ConnectorBuilderException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return HttpStatus.INTERNAL_SERVER_ERROR.getCode();
  }

}
