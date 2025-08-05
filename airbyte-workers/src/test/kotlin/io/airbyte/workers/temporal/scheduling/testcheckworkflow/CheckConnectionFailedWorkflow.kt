/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow

import io.airbyte.commons.temporal.scheduling.CheckCommandApiInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionOutput

class CheckConnectionFailedWorkflow : ConnectorCommandWorkflow {
  override fun run(input: ConnectorCommandInput): ConnectorJobOutput {
    assert(input is CheckCommandApiInput)
    return ConnectorJobOutput()
      .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
      .withCheckConnection(StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED).withMessage("nope"))
  }

  override fun checkTerminalStatus() {
  }
}
