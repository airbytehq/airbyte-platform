/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Exception when an operation related to declarative sources is requested on a source that is not
 * found for a specfic workspace.
 */
public class DeclarativeSourceNotFoundException extends KnownException {

  public DeclarativeSourceNotFoundException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return 404;
  }

}
