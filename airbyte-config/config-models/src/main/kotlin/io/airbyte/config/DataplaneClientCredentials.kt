/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import java.util.UUID

/**
 * Object containing credentials for requesting tokens for dataplane authentication.
 */
data class DataplaneClientCredentials(
  val id: UUID,
  val dataplaneId: UUID,
  val clientId: String,
  val clientSecret: String,
  val createdAt: java.time.OffsetDateTime? = null,
)
