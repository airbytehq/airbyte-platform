/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.stubs

import io.airbyte.commons.temporal.annotations.TemporalActivityStub

class InvalidTestWorkflowImpl : TestWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "missingActivityOptions")
  private val testActivity: TestActivity? = null

  override val state: Int? = 1

  override fun run() {
  }

  override fun cancel() {}
}
