/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class NotFoundException(
  message: String?,
) : KnownException(message) {
  override fun getHttpCode(): HttpStatus = HttpStatus.NOT_FOUND
}
