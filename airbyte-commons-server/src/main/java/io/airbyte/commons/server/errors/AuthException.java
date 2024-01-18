/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import javax.ws.rs.core.Response;

/**
 * Thrown when there are authentication issues.
 */
public class AuthException extends KnownException {

  private static final long serialVersionUID = 1L;

  public AuthException(final String message, final Exception exception) {
    super(message, exception);
  }

  public AuthException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return Response.Status.UNAUTHORIZED.getStatusCode();
  }

}
