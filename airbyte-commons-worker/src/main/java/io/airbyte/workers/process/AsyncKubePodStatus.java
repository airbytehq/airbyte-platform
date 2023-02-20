/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

/**
 * Kube pod statues.
 */
public enum AsyncKubePodStatus {
  NOT_STARTED, // Pod hasn't been started yet.
  INITIALIZING, // On-start container started but not completed
  RUNNING, // Main container posted running
  FAILED, // Reported status was "failed" or pod was in Error without a reported status.
  SUCCEEDED; // Reported status was "success" so both main and on-start succeeded.
}
