/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

/**
 * Exception thrown from Retrofit clients, specifically the [body] and [bodyOrNull] functions.
 */
class ApiException(
  val statusCode: Int,
  val url: String,
  override val message: String,
) : RuntimeException(message)
