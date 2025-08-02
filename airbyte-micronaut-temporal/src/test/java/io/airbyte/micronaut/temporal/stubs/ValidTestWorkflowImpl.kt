/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs

import io.airbyte.commons.temporal.annotations.TemporalActivityStub

open class ValidTestWorkflowImpl : TestWorkflow {
  var isCancelled: Boolean = false
    private set
  var isHasRun: Boolean = false
    private set

  @TemporalActivityStub(activityOptionsBeanName = "activityOptions")
  private lateinit var testActivity: TestActivity

  override fun run() {
    testActivity.getValue()
    this.isHasRun = true
  }

  override fun cancel() {
    this.isCancelled = true
  }

  override val state: Int
    get() = 1
}
