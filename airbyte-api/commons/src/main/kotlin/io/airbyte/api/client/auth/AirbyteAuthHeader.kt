/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.auth

/**
 * Defines the custom Airbyte authentication header.
 */
interface AirbyteAuthHeader {
  fun getHeaderName(): String

  fun getHeaderValue(): String
}
