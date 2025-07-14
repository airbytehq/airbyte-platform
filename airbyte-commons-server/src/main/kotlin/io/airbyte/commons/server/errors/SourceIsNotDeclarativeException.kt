/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

/**
 * Exception when an operation related to declarative sources is requested on a source that isn't
 * declarative.
 */
class SourceIsNotDeclarativeException(
  message: String?,
) : KnownException(message) {
  override fun getHttpCode(): Int = 400
}
