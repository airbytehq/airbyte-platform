/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.errors

import io.airbyte.workload.api.domain.KnownExceptionInfo
import io.micronaut.http.HttpStatus

abstract class KnownException(
  message: String?,
) : Throwable(message) {
  abstract fun getHttpCode(): HttpStatus

  fun getInfo(): KnownExceptionInfo = KnownExceptionInfo(this.message ?: "")
}
