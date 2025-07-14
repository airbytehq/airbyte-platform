/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

/**
 * Exception when an operation is correctly formatted and syntactically valid, but not allowed due
 * to the current state of the system. For example, deletion of a resource that should not be
 * deleted according to business logic.
 */
class OperationNotAllowedException : KnownException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Exception?) : super(message, cause)

  override fun getHttpCode(): Int = 403
}
