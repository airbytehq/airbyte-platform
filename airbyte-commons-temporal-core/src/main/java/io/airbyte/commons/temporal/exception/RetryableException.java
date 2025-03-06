/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception;

/**
 * Exception when an activity fails for a reason that can be retried and lead to a non-exceptional
 * outcome.
 */
public class RetryableException extends RuntimeException {

  public RetryableException(final Exception e) {
    super(e);
  }

}
