/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.messages

import java.util.UUID

data class SourceInfo(
  val id: UUID,
  val name: String,
  val url: String,
)
