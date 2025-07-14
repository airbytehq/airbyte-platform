/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors;

import io.airbyte.api.model.generated.KnownExceptionInfo;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
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
        .message(message);

    if (details != null) {
      exceptionInfo.details(details);
    }

    return exceptionInfo;
  }

  public static KnownExceptionInfo infoFromThrowableWithMessage(final Throwable t, final String message) {
    return infoFromThrowableWithMessage(t, message, null); // Call the other static method with null details
  }

  public KnownExceptionInfo getKnownExceptionInfoWithStackTrace() {
    return KnownException.infoFromThrowableWithMessageAndStackTrace(this);
  }

  private static KnownExceptionInfo infoFromThrowableWithMessageAndStackTrace(final Throwable t) {
    final KnownExceptionInfo exceptionInfo = new KnownExceptionInfo()
        .exceptionClassName(t.getClass().getName())
        .message(t.getMessage())
        .exceptionStack(toStringList(t));
    if (t.getCause() != null) {
      exceptionInfo.rootCauseExceptionClassName(t.getClass().getName());
      exceptionInfo.rootCauseExceptionStack(toStringList(t.getCause()));
    }
    return exceptionInfo;
  }

  public static KnownExceptionInfo infoFromThrowable(final Throwable t, final Map<String, Object> details) {
    return infoFromThrowableWithMessage(t, t.getMessage(), details);
  }

  @SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.AvoidInstanceofChecksInCatchClause"})
  private static List<String> toStringList(final Throwable throwable) {
    final StringWriter sw = new StringWriter();
    final PrintWriter pw = new PrintWriter(sw);
    try {
      throwable.printStackTrace(pw);
    } catch (final RuntimeException ex) {
      // Ignore any exceptions.
    }
    pw.flush();
    final List<String> lines = new ArrayList<>();
    try (LineNumberReader reader = new LineNumberReader(new StringReader(sw.toString()))) {
      String line = reader.readLine();
      while (line != null) {
        lines.add(line);
        line = reader.readLine();
      }
    } catch (final IOException ex) {
      if (ex instanceof InterruptedIOException) {
        Thread.currentThread().interrupt();
      }
      lines.add(ex.toString());
    }
    return lines;
  }

}
