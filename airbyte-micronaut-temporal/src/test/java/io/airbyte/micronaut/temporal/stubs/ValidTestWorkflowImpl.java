/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs;

import io.airbyte.commons.temporal.annotations.TemporalActivityStub;

public class ValidTestWorkflowImpl implements TestWorkflow {

  private boolean cancelled = false;
  private boolean hasRun = false;

  @TemporalActivityStub(activityOptionsBeanName = "activityOptions")
  private TestActivity testActivity;

  @Override
  public void run() {
    testActivity.getValue();
    hasRun = true;
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  @Override
  public Integer getState() {
    return 1;
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public boolean isHasRun() {
    return hasRun;
  }

}
