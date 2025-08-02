/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow

import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput

class CheckConnectionDestinationSystemErrorWorkflow : ConnectorCommandWorkflow {
  override fun run(input: ConnectorCommandInput): ConnectorJobOutput {
    assert(input is CheckCommandInput)
    if ((input as CheckCommandInput).input.checkConnectionInput.getActorType() == ActorType.SOURCE) {
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
