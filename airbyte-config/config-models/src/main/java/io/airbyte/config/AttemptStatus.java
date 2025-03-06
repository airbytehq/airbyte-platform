/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.google.common.collect.Sets;
import java.util.Set;

/**
 * The statuses of an attempt.
 */
public enum AttemptStatus {

  RUNNING,
  FAILED,
  SUCCEEDED;

  public static final Set<AttemptStatus> TERMINAL_STATUSES = Sets.newHashSet(FAILED, SUCCEEDED);

}
