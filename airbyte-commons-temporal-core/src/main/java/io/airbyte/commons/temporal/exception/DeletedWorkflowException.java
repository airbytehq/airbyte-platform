/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception;

/**
 * Exception that is thrown when trying to interact with a workflow that has been deleted.
 */
public class DeletedWorkflowException extends Exception {

  public DeletedWorkflowException(final String message) {
    super(message);
  }

}
