package io.airbyte.workers.config

import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.time.Clock

@Factory
class HelperBeanFactory {
  @Singleton
  fun getClock(): Clock {
    return Clock.systemUTC()
  }
}
