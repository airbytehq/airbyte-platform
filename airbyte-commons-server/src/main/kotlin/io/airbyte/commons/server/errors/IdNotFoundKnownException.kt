/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import io.airbyte.api.model.generated.NotFoundKnownExceptionInfo

/**
 * Thrown when an api input requests an id that does not exist.
 */
class IdNotFoundKnownException : KnownException {
  var id: String? = null

  constructor(message: String?, id: String?) : super(message) {
    this.id = id
  }

  constructor(message: String?, id: String?, cause: Throwable?) : super(message, cause) {
    this.id = id
  }

  constructor(message: String?, cause: Throwable?) : super(message, cause)

  override fun getHttpCode(): Int = 404

  /**
   * Get additional info about the not found resource (without stack trace).
   *
   * @return info
   */
  fun getNotFoundKnownExceptionInfo(): NotFoundKnownExceptionInfo {
    val exceptionInfo =
      NotFoundKnownExceptionInfo()
        .exceptionClassName(javaClass.name)
        .message(this.message)
        .id(this.id)
        .errorId(this.errorId)
    return exceptionInfo
  }

  /**
   * Get additional info about the not found resource with stack trace (for logging).
   *
   * @return info
   */
  fun getNotFoundKnownExceptionInfoWithStackTrace(): NotFoundKnownExceptionInfo {
    val exceptionInfo =
      NotFoundKnownExceptionInfo()
        .exceptionClassName(javaClass.name)
        .message(this.message)
        .exceptionStack(getStackTraceAsList(this))
        .id(this.id)
        .errorId(this.errorId)
    if (this.cause != null) {
      exceptionInfo.rootCauseExceptionClassName(this.cause!!.javaClass.name)
      exceptionInfo.rootCauseExceptionStack(
        getStackTraceAsList(cause!!),
      )
    }
    return exceptionInfo
  }
}
