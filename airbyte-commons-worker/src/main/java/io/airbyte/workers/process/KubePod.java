/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

/**
 * Interface for interacting with kube pods.
 */
public interface KubePod {

  int exitValue();

  void destroy();

  int waitFor() throws InterruptedException;

  KubePodInfo getInfo();

  Process toProcess();

}
