/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.util.UUID

data class ServiceAccount(
  val id: UUID,
  val name: String,
  val secret: String,
  val managed: Boolean = false,
)
