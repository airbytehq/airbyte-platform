/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron

import io.airbyte.commons.temporal.TemporalInitializationUtils
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import io.micronaut.runtime.Micronaut
import jakarta.inject.Singleton

const val SCHEDULED_TRACE_OPERATION_NAME = "scheduled"

/**
 * Micronaut server responsible for running scheduled method. The methods need to be separated in
 * Bean based on what they are cleaning and contain a method annotated with `@Scheduled`
 *
 * Injected object looks unused but they are not
 */
fun main(args: Array<String>) {
  Micronaut
    .build(*args)
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()
}

/** Wait until temporal is ready before saying the app is ready. */
@Singleton
@Requires(notEnv = [Environment.TEST])
class ApplicationInitializer(
  private val temporalInitializationUtils: TemporalInitializationUtils,
) : ApplicationEventListener<ServiceReadyEvent> {
  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    temporalInitializationUtils.waitForTemporalNamespace()
  }
}
