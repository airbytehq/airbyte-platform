/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.config.OperatorWebhookInput;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Webhook operation temporal interface.
 */
@ActivityInterface
public interface WebhookOperationActivity {

  @ActivityMethod
  boolean invokeWebhook(OperatorWebhookInput input);

}
