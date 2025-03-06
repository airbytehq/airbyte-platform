/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception;

/**
 * Exception that is thrown when attempting (and failing) to access a workflow.
 */
public class UnreachableWorkflowException extends Exception {

  public UnreachableWorkflowException(final String message) {
    super(message);
  }

  public UnreachableWorkflowException(final String message, final Throwable t) {
    super(message, t);
  }

}
