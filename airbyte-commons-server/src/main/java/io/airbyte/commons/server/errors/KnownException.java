/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import io.airbyte.api.model.generated.KnownExceptionInfo;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Exception wrapper to handle formatting API exception outputs nicely.
 */
public abstract class KnownException extends RuntimeException {

  private final Map<String, Object> details; // Add an optional details field

  public KnownException(final String message) {
    super(message);
    this.details = null;
  }

  public KnownException(final String message, final Map<String, Object> details) {
    super(message);
    this.details = details;
  }

  public KnownException(final String message, final Throwable cause) {
    super(message, cause);
    this.details = null;
  }

  public KnownException(final String message, final Throwable cause, final Map<String, Object> details) {
    super(message, cause);
    this.details = details;
  }

  public abstract int getHttpCode();

  public Map<String, Object> getDetails() {
    return details;
  }

  public KnownExceptionInfo getKnownExceptionInfo() {
    return KnownException.infoFromThrowable(this, details);
  }

  public static List<String> getStackTraceAsList(final Throwable throwable) {
    final StringWriter stringWriter = new StringWriter();
    throwable.printStackTrace(new PrintWriter(stringWriter));
    final String[] stackTrace = stringWriter.toString().split("\n");
    return Stream.of(stackTrace).collect(Collectors.toList());
  }

  public static KnownExceptionInfo infoFromThrowableWithMessage(final Throwable t, final String message) {
    return infoFromThrowableWithMessage(t, message, null); // Call the other static method with null details
  }

  /**
   * Static factory for creating a known exception.
   *
   * @param t throwable to wrap
   * @param message error message
   * @param details additional details
   * @return known exception
   */
  public static KnownExceptionInfo infoFromThrowableWithMessage(final Throwable t, final String message, final Map<String, Object> details) {
    final KnownExceptionInfo exceptionInfo = new KnownExceptionInfo()
        .exceptionClassName(t.getClass().getName())
        .message(message)
        .exceptionStack(getStackTraceAsList(t));

    if (t.getCause() != null) {
      exceptionInfo.rootCauseExceptionClassName(t.getCause().getClass().getName());
      exceptionInfo.rootCauseExceptionStack(getStackTraceAsList(t.getCause()));
    }

    if (details != null) {
      exceptionInfo.details(details);
    }

    return exceptionInfo;
  }

  public static KnownExceptionInfo infoFromThrowable(final Throwable t, final Map<String, Object> details) {
    return infoFromThrowableWithMessage(t, t.getMessage(), details);
  }

}
