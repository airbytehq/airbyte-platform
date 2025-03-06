/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.time.Clock

@Factory
class HelperBeanFactory {
  @Singleton
  fun getClock(): Clock = Clock.systemUTC()
}
