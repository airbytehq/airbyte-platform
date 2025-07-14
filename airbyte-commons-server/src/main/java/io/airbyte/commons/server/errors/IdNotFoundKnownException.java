/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import io.airbyte.api.model.generated.NotFoundKnownExceptionInfo;

/**
 * Thrown when an api input requests an id that does not exist.
 */
public class IdNotFoundKnownException extends KnownException {

  String id;

  public IdNotFoundKnownException(final String message, final String id) {
    super(message);
    this.id = id;
  }

  public IdNotFoundKnownException(final String message, final String id, final Throwable cause) {
    super(message, cause);
    this.id = id;
  }

  public IdNotFoundKnownException(final String message, final Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getHttpCode() {
    return 404;
  }

  public String getId() {
    return id;
  }

  /**
   * Get additional info about the not found resource.
   *
   * @return info
   */
  public NotFoundKnownExceptionInfo getNotFoundKnownExceptionInfo() {
    final NotFoundKnownExceptionInfo exceptionInfo = new NotFoundKnownExceptionInfo()
        .exceptionClassName(this.getClass().getName())
        .message(this.getMessage())
        .exceptionStack(getStackTraceAsList(this));
    if (this.getCause() != null) {
      exceptionInfo.rootCauseExceptionClassName(this.getClass().getClass().getName());
      exceptionInfo.rootCauseExceptionStack(getStackTraceAsList(this.getCause()));
    }
    exceptionInfo.id(this.getId());
    return exceptionInfo;
  }

}
