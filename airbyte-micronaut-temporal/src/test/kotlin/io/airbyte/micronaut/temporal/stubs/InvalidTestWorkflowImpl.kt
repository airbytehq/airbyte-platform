/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs

import io.airbyte.commons.temporal.annotations.TemporalActivityStub

open class InvalidTestWorkflowImpl : TestWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "missingActivityOptions")
  private lateinit var testActivity: TestActivity

  override fun run() {
  }

  override fun cancel() {}

  override val state: Int
    get() = 1
}
