/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

/**
 * The statuses of a job.
 */
public enum JobStatus {

  PENDING,
  RUNNING,
  INCOMPLETE,
  FAILED,
  SUCCEEDED,
  CANCELLED;

  public static final Set<JobStatus> TERMINAL_STATUSES = Set.of(FAILED, SUCCEEDED, CANCELLED);
  public static final Set<JobStatus> NON_TERMINAL_STATUSES = Sets.difference(Set.of(values()), TERMINAL_STATUSES);

  public static final Map<JobStatus, Set<JobStatus>> VALID_STATUS_CHANGES = Map.of(
      PENDING, Set.of(RUNNING, FAILED, CANCELLED, INCOMPLETE),
      RUNNING, Set.of(INCOMPLETE, SUCCEEDED, FAILED, CANCELLED),
      INCOMPLETE, Set.of(PENDING, RUNNING, FAILED, CANCELLED, INCOMPLETE, SUCCEEDED),
      SUCCEEDED, Set.of(),
      FAILED, Set.of(FAILED),
      CANCELLED, Set.of());

}
