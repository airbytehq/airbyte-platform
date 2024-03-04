/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.SpecInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.UUID;

/**
 * SpecActivity.
 */
@ActivityInterface
public interface SpecActivity {

  @ActivityMethod
  ConnectorJobOutput run(JobRunConfig jobRunConfig, IntegrationLauncherConfig launcherConfig);

  @ActivityMethod
  ConnectorJobOutput runWithWorkload(final SpecInput input) throws WorkerException;

  @ActivityMethod
  boolean shouldUseWorkload(final UUID workspaceId);

  @ActivityMethod
  void reportSuccess();

  @ActivityMethod
  void reportFailure();

}
