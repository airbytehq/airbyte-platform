/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs

import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.exception.RetryableException

open class InvalidTestWorkflowImpl : TestWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "missingActivityOptions")
  private lateinit var testActivity: TestActivity

  @Throws(RetryableException::class)
  override fun run() {
  }

  override fun cancel() {}

  override val state: Int
    get() = 1
}
