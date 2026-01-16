/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json

/**
 * A JSON object was not valid against a given JSONSchema.
 */
class JsonValidationException : Exception {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
