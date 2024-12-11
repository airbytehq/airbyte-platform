package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.config.ConnectorJobOutput

class MockConnectorCommandWorkflow : ConnectorCommandWorkflow {
  override fun run(input: ConnectorCommandInput): ConnectorJobOutput = ConnectorJobOutput()

  override fun checkTerminalStatus() { }
}
