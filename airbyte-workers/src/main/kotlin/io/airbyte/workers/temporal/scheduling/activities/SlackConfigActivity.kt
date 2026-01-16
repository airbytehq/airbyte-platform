/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.config.SlackNotificationConfiguration
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import java.util.Optional
import java.util.UUID

/**
 * SlackConfigActivity.
 */
@ActivityInterface
interface SlackConfigActivity {
  @ActivityMethod
  fun fetchSlackConfiguration(connectionId: UUID): Optional<SlackNotificationConfiguration>
}
