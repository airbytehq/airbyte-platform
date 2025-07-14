/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.exception;

/**
 * Exceptions thrown from a destination.
 */
public class DestinationException extends RuntimeException {

  public DestinationException(final String message) {
    super(message);
  }

  public DestinationException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
