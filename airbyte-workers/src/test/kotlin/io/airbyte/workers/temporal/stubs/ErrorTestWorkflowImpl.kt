/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.stubs

import io.airbyte.commons.temporal.exception.RetryableException

class ErrorTestWorkflowImpl : TestWorkflow {
  override val state: Int? = 1

  override fun run(): Unit = throw RetryableException(NullPointerException("test"))

  override fun cancel() {}
}
