/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config

import java.util.UUID

/**
 * A stream reset record is a reference to a stream that has a reset pending or running.
 */
data class StreamResetRecord(
  val connectionId: UUID,
  val streamName: String,
  val streamNamespace: String?,
)
