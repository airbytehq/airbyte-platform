/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

/**
 * Exception when an operation related to declarative sources is requested on a source that isn't
 * declarative.
 */
public class SourceIsNotDeclarativeException extends KnownException {

  public SourceIsNotDeclarativeException(final String message) {
    super(message);
  }

  @Override
  public int getHttpCode() {
    return 400;
  }

}
