/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow;

import io.airbyte.commons.temporal.scheduling.CheckConnectionWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;

public class CheckConnectionSystemErrorWorkflow implements CheckConnectionWorkflow {

  @Override
  public ConnectorJobOutput run(JobRunConfig jobRunConfig,
                                IntegrationLauncherConfig launcherConfig,
                                StandardCheckConnectionInput connectionConfiguration) {
    return new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
        .withFailureReason(new FailureReason().withFailureType(FailureType.SYSTEM_ERROR));
  }

}
