/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging.model

data class Actor(
  val actorId: String,
  val email: String? = null,
  var ipAddress: String? = null,
  var userAgent: String? = null,
)
