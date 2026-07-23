/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal.stubs

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod

@ActivityInterface
interface TestActivity {
  @ActivityMethod
  fun getValue(): String?
}
