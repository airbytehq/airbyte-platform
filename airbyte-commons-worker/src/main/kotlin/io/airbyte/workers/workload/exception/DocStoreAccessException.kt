/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload.exception

class DocStoreAccessException(
  override val message: String,
  override val cause: Throwable,
) : Exception(message, cause)
