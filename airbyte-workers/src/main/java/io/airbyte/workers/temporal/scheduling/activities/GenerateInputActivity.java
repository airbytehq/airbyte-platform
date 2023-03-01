/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
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
   * Generated job input.
   */
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  class GeneratedJobInput {

    private JobRunConfig jobRunConfig;
    private IntegrationLauncherConfig sourceLauncherConfig;
    private IntegrationLauncherConfig destinationLauncherConfig;
    private StandardSyncInput syncInput;

  }

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  GeneratedJobInput getSyncWorkflowInput(SyncInput input);

  /**
   * This generate the input needed by the child sync workflow.
   */
  @ActivityMethod
  GeneratedJobInput getSyncWorkflowInputWithAttemptNumber(SyncInputWithAttemptNumber input);

  @ActivityMethod
  SyncJobCheckConnectionInputs getCheckConnectionInputs(SyncInputWithAttemptNumber input);

}
