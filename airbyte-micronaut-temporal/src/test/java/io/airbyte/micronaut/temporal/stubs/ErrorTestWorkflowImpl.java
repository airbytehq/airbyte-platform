/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs;

import io.airbyte.commons.temporal.exception.RetryableException;

public class ErrorTestWorkflowImpl implements TestWorkflow {

  @Override
  public void run() throws RetryableException {
    throw new RetryableException(new NullPointerException("test"));
  }

  @Override
  public void cancel() {}

  @Override
  public Integer getState() {
    return 1;
  }

}
