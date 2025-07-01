/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.exception

/**
 * Exception when an activity fails because the output size exceeds Temporal limits.
 */
class SizeLimitException(
  message: String?,
) : RuntimeException(message)
