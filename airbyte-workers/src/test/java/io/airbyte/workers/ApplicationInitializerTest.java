/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ApplicationInitializerTest {

  @Test
  void testInferWorkflowExecSizeFromActivityExecutionSize() {
    assertEquals(2, ApplicationInitializer.inferWorkflowExecSizeFromActivityExecutionSize(10));

    assertEquals(2, ApplicationInitializer.inferWorkflowExecSizeFromActivityExecutionSize(5));

    assertEquals(2, ApplicationInitializer.inferWorkflowExecSizeFromActivityExecutionSize(1));

    assertEquals(20, ApplicationInitializer.inferWorkflowExecSizeFromActivityExecutionSize(100));
  }

}
