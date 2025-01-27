/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class ConflictException(
  message: String?,
) : KnownException(message) {
  override fun getHttpCode(): HttpStatus = HttpStatus.CONFLICT
}
