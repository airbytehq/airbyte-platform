/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import java.nio.file.Path

/**
 * Metadata about a temporal workflow.
 */

data class JobMetadata(
  val succeeded: Boolean,
  val logPath: Path,
)
