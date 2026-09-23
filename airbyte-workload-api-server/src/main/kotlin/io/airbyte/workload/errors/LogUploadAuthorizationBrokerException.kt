/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.errors

import io.micronaut.http.HttpStatus

class LogUploadAuthorizationBrokerException : KnownException("Log upload authorization is unavailable.") {
  override fun getHttpCode(): HttpStatus = HttpStatus.INTERNAL_SERVER_ERROR
}
