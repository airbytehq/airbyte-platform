/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import io.airbyte.workers.ApplicationInitializer.Companion.inferWorkflowExecSizeFromActivityExecutionSize
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class ApplicationInitializerTest {
  @Test
  fun testInferWorkflowExecSizeFromActivityExecutionSize() {
    Assertions.assertEquals(2, inferWorkflowExecSizeFromActivityExecutionSize(10))

    Assertions.assertEquals(2, inferWorkflowExecSizeFromActivityExecutionSize(5))

    Assertions.assertEquals(2, inferWorkflowExecSizeFromActivityExecutionSize(1))

    Assertions.assertEquals(20, inferWorkflowExecSizeFromActivityExecutionSize(100))
  }
}
