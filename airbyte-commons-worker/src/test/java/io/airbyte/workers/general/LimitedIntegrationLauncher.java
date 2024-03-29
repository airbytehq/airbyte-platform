/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.process.IntegrationLauncher;
import java.nio.file.Path;

/**
 * Test-only launcher to launch {@link LimitedFatRecordSourceProcess}. Intended as a convenient test
 * harness for testing.
 */
public class LimitedIntegrationLauncher implements IntegrationLauncher {

  private final Process testProcess;

  public LimitedIntegrationLauncher(Process testProcess) {
    this.testProcess = testProcess;
  }

  @Override
  public Process spec(Path jobRoot) throws WorkerException {
    return null;
  }

  @Override
  public Process check(Path jobRoot, String configFilename, String configContents) throws WorkerException {
    return null;
  }

  @Override
  public Process discover(Path jobRoot, String configFilename, String configContents) throws WorkerException {
    return null;
  }

  @Override
  public Process read(Path jobRoot,
                      String configFilename,
                      String configContents,
                      String catalogFilename,
                      String catalogContents,
                      String stateFilename,
                      String stateContents)
      throws WorkerException {
    return testProcess;
  }

  @Override
  public Process write(Path jobRoot,
                       String configFilename,
                       String configContents,
                       String catalogFilename,
                       String catalogContents)
      throws WorkerException {
    return null;
  }

}
