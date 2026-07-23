/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import io.micronaut.http.HttpStatus

class EntityDeletedException : KnownException {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)

  override fun getHttpCode(): Int = HttpStatus.GONE.code
}
