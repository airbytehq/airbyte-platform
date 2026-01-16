/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

enum class StageName {
  CLAIM,
  CHECK_STATUS,
  BUILD,
  LOAD_SHED,
  MUTEX,
  ARCHITECTURE,
  LAUNCH,
}
