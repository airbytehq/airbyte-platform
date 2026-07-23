/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.config.OperatorWebhookInput
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

/**
 * Webhook operation temporal interface.
 */
@ActivityInterface
interface WebhookOperationActivity {
  @ActivityMethod
  fun invokeWebhook(input: OperatorWebhookInput): Boolean
}
