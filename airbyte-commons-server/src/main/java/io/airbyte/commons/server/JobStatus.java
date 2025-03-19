/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server;

/**
 * Indicates whether the worker's underlying process was successful. E.g this should return
 * SUCCEEDED if a connection check succeeds, FAILED otherwise.
 */
public enum JobStatus {
  FAILED,
  SUCCEEDED
}
