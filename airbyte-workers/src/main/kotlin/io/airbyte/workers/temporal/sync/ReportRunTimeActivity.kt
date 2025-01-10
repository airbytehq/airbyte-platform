/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.temporal.sync

import io.airbyte.workers.temporal.activities.ReportRunTimeActivityInput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface ReportRunTimeActivity {
  @ActivityMethod
  fun reportRunTime(input: ReportRunTimeActivityInput)
}
