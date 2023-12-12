/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.testcheckworkflow;

import io.airbyte.commons.temporal.scheduling.CheckConnectionWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;

public class CheckConnectionFailedWorkflow implements CheckConnectionWorkflow {

  @Override
  public ConnectorJobOutput run(JobRunConfig jobRunConfig,
                                IntegrationLauncherConfig launcherConfig,
                                StandardCheckConnectionInput connectionConfiguration) {
    return new ConnectorJobOutput().withOutputType(OutputType.CHECK_CONNECTION)
        .withCheckConnection(new StandardCheckConnectionOutput().withStatus(Status.FAILED).withMessage("nope"));
  }

}
