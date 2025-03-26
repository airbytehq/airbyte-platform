/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.exception

class WorkloadMonitorException(
  message: String?,
) : RuntimeException(message)
