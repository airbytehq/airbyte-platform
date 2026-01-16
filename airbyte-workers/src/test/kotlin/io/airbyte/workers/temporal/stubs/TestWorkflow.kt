/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.stubs

import io.temporal.workflow.QueryMethod
import io.temporal.workflow.SignalMethod
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod

@WorkflowInterface
interface TestWorkflow {
  @WorkflowMethod
  fun run()

  @SignalMethod
  fun cancel()

  @get:QueryMethod
  val state: Int?
}
