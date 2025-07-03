/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.google.common.collect.Sets

/**
 * The statuses of an attempt.
 */
enum class AttemptStatus {
  RUNNING,
  FAILED,
  SUCCEEDED,
  ;

  companion object {
    val TERMINAL_STATUSES: Set<AttemptStatus> = Sets.newHashSet(FAILED, SUCCEEDED)
  }
}
