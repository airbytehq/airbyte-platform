/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow;

import io.airbyte.commons.temporal.scheduling.CheckCommandInput;
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput;
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import org.jetbrains.annotations.NotNull;

public class CheckConnectionSuccessWorkflow implements ConnectorCommandWorkflow {

  @NotNull
  @Override
  public ConnectorJobOutput run(@NotNull ConnectorCommandInput input) {
    assert input instanceof CheckCommandInput;
    return new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
        .withCheckConnection(new StandardCheckConnectionOutput().withStatus(Status.SUCCEEDED).withMessage("check worked"));
  }

  @Override
  public void checkTerminalStatus() {

  }

}
