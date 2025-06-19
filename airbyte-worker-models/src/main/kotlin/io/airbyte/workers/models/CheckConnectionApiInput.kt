/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import java.util.UUID

data class CheckConnectionApiInput(
  val actorId: UUID,
  val jobId: String,
  val attemptId: Long,
)
