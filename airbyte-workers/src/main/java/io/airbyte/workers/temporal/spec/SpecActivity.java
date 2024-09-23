/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.SpecInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * SpecActivity.
 */
@ActivityInterface
public interface SpecActivity {

  @ActivityMethod
  ConnectorJobOutput runWithWorkload(final SpecInput input) throws WorkerException;

  @ActivityMethod
  void reportSuccess();

  @ActivityMethod
  void reportFailure();

}
