/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception

/**
 * Exception that is thrown when attempting (and failing) to access a workflow.
 */
class UnreachableWorkflowException : Exception {
  constructor(message: String?) : super(message)

  constructor(message: String?, t: Throwable?) : super(message, t)
}
