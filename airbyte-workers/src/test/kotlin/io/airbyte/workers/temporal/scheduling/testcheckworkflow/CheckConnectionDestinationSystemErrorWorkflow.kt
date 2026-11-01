/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow

import io.airbyte.commons.temporal.scheduling.CheckCommandApiInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest
import org.junit.jupiter.api.Assertions.assertInstanceOf

class CheckConnectionDestinationSystemErrorWorkflow : ConnectorCommandWorkflow {
  override fun run(input: ConnectorCommandInput): ConnectorJobOutput {
    assertInstanceOf(CheckCommandApiInput::class.java, input)
    if ((input as CheckCommandApiInput).input.actorId == ConnectionManagerWorkflowTest.SOURCE_ID) {
      return ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED).withMessage("check worked"),
        )
    } else {
      return ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withFailureReason(FailureReason().withFailureType(FailureReason.FailureType.SYSTEM_ERROR))
    }
  }

  override fun checkTerminalStatus() {
  }
}
