/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.exception

class WorkloadLauncherException(
  message: String?,
) : RuntimeException(message)
