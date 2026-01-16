/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception

/**
 * Exception that is thrown when trying to interact with a workflow that has been deleted.
 */
class DeletedWorkflowException(
  message: String?,
) : Exception(message)
