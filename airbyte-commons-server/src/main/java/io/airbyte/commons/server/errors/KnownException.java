/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import io.airbyte.api.model.generated.KnownExceptionInfo;
import org.apache.logging.log4j.core.util.Throwables;

/**
 * Exception wrapper to handle formatting API exception outputs nicely.
 */
public abstract class KnownException extends RuntimeException {

  public KnownException(final String message) {
    super(message);
  }

  public KnownException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public abstract int getHttpCode();

  public KnownExceptionInfo getKnownExceptionInfo() {
    return KnownException.infoFromThrowable(this);
  }

  /**
   * Static factory for creating a known exception.
   *
   * @param t throwable to wrap
   * @param message error message
   * @return known exception
   */
  public static KnownExceptionInfo infoFromThrowableWithMessage(final Throwable t, final String message) {
    final KnownExceptionInfo exceptionInfo = new KnownExceptionInfo()
        .exceptionClassName(t.getClass().getName())
        .message(message)
        .exceptionStack(Throwables.toStringList(t));
    if (t.getCause() != null) {
      exceptionInfo.rootCauseExceptionClassName(t.getClass().getClass().getName());
      exceptionInfo.rootCauseExceptionStack(Throwables.toStringList(t.getCause()));
    }
    return exceptionInfo;
  }

  public static KnownExceptionInfo infoFromThrowable(final Throwable t) {
    return infoFromThrowableWithMessage(t, t.getMessage());
  }

}
