/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.workers.models.JobInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * GenerateInputActivity.
 */
@ActivityInterface
public interface GenerateInputActivity {

  /**
   * SyncInput.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class SyncInput {

    private int attemptId;
    private long jobId;

  }

  /**
   * SyncInputWithAttemptNumber.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class SyncInputWithAttemptNumber {

    private int attemptNumber;
    private long jobId;

  }

  /**
   * GeneratedJobInput.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class SyncJobCheckConnectionInputs {

    private IntegrationLauncherConfig sourceLauncherConfig;
    private IntegrationLauncherConfig destinationLauncherConfig;
    private StandardCheckConnectionInput sourceCheckConnectionInput;
    private StandardCheckConnectionInput destinationCheckConnectionInput;

  }

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  JobInput getSyncWorkflowInput(SyncInput input) throws Exception;

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  JobInput getSyncWorkflowInputWithAttemptNumber(SyncInputWithAttemptNumber input) throws Exception;

  @ActivityMethod
  SyncJobCheckConnectionInputs getCheckConnectionInputs(SyncInputWithAttemptNumber input);

}
