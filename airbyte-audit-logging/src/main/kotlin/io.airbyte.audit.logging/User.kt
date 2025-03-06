/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

data class User(
  val userId: String,
  val email: String? = null,
  var ipAddress: String? = null,
  var userAgent: String? = null,
)
