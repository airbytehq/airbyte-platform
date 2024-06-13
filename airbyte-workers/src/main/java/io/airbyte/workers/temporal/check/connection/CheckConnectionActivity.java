/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.CheckConnectionInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.time.Duration;
import java.util.UUID;

/**
 * Check connection activity temporal interface.
 */
@ActivityInterface
public interface CheckConnectionActivity {

  @ActivityMethod
  ConnectorJobOutput runWithJobOutput(CheckConnectionInput input);

  @ActivityMethod
  ConnectorJobOutput runWithWorkload(CheckConnectionInput input) throws WorkerException;

  Duration getCheckConnectionTimeout();

  boolean shouldUseWorkload(UUID workspaceId);

}
