/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
   * Get additional info about the not found resource.
   *
   * @return info
   */
  fun getNotFoundKnownExceptionInfo(): NotFoundKnownExceptionInfo {
    val exceptionInfo =
      NotFoundKnownExceptionInfo()
        .exceptionClassName(javaClass.name)
        .message(this.message)
        .exceptionStack(getStackTraceAsList(this))
    if (this.cause != null) {
      exceptionInfo.rootCauseExceptionClassName(javaClass.javaClass.name)
      exceptionInfo.rootCauseExceptionStack(
        getStackTraceAsList(cause!!),
      )
    }
    exceptionInfo.id(this.id)
    return exceptionInfo
  }
}
