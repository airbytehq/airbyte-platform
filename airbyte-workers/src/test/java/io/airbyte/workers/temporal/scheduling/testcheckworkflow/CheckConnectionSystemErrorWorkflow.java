/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow;

import io.airbyte.commons.temporal.scheduling.CheckCommandInput;
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput;
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureType;
import org.jetbrains.annotations.NotNull;

public class CheckConnectionSystemErrorWorkflow implements ConnectorCommandWorkflow {

  @NotNull
  @Override
  public ConnectorJobOutput run(@NotNull ConnectorCommandInput input) {
    assert input instanceof CheckCommandInput;
    return new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
        .withFailureReason(new FailureReason().withFailureType(FailureType.SYSTEM_ERROR));
  }

  @Override
  public void checkTerminalStatus() {

  }

}
