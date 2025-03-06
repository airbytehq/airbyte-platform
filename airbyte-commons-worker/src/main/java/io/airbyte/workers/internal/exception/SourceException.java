/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception;

/**
 * Exceptions thrown from a source.
 */
public class SourceException extends RuntimeException {

  public SourceException(final String message) {
    super(message);
  }

  public SourceException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
