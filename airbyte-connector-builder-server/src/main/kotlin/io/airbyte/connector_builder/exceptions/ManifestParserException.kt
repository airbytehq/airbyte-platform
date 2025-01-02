/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.exceptions

import io.airbyte.commons.server.errors.KnownException
import io.micronaut.http.HttpStatus

class CircularReferenceException(ref: String) : ManifestParserException("Circular reference detected: $ref")

class UndefinedReferenceException(path: String, ref: String) : ManifestParserException("Undefined reference: $ref at $path")

/**
 * Thrown when the ManifestParser encountered an error when processing the given manifest.
 */
open class ManifestParserException : KnownException {
  constructor(message: String?, exception: Exception?) : super(message, exception)
  constructor(message: String?) : super(message)

  override fun getHttpCode(): Int {
    return HttpStatus.UNAUTHORIZED.code
  }
}
