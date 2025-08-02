/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.stubs

import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.exception.RetryableException

class InvalidTestWorkflowImpl : TestWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "missingActivityOptions")
  private val testActivity: TestActivity? = null

  override val state: Int? = 1

  @Throws(RetryableException::class)
  override fun run() {
  }

  override fun cancel() {}
}
