/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.stubs

import io.airbyte.commons.temporal.annotations.TemporalActivityStub

class ValidTestWorkflowImpl : TestWorkflow {
  var isCancelled: Boolean = false
    private set
  var isHasRun: Boolean = false
    private set

  @TemporalActivityStub(activityOptionsBeanName = "activityOptions")
  private val testActivity: TestActivity? = null

  override val state: Int? = 1

  override fun run() {
    testActivity!!.value
    this.isHasRun = true
  }

  override fun cancel() {
    this.isCancelled = true
  }
}
