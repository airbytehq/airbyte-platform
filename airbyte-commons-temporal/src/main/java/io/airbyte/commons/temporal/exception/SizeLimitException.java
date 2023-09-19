/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception;

/**
 * Exception when an activity fails because the output size exceeds Temporal limits.
 */
public class SizeLimitException extends RuntimeException {

  public SizeLimitException(final String message) {
    super(message);
  }

}
