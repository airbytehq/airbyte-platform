/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class InvalidStatusTransitionException(
  message: String?,
) : KnownException(message) {
  override fun getHttpCode(): HttpStatus = HttpStatus.GONE
}
