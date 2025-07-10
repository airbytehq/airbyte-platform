/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.exceptions

/**
 * Struct representing an invalid request. Add metadata here as necessary.
 */
class InvalidRequestException(
  message: String?,
) : Exception(message)
