/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduling

import io.micronaut.scheduling.TaskExecutors

/**
 * Names of common task schedulers used to offload work in a Micronaut application.
 */
interface AirbyteTaskExecutors : TaskExecutors {
  companion object {
    // In kotlin, the way we were extending this in java doesn't work.
    // Inherited from TaskExecutors
    const val IO = TaskExecutors.IO
    const val BLOCKING = TaskExecutors.BLOCKING
    const val VIRTUAL = TaskExecutors.VIRTUAL
    const val SCHEDULED = TaskExecutors.SCHEDULED
    const val MESSAGE_CONSUMER = TaskExecutors.MESSAGE_CONSUMER

    // Airbyte-specific
    const val HEALTH = "health"
    const val SCHEDULER = "scheduler"
    const val PUBLIC_API = "public-api"
    const val WEBHOOK = "webhook"
  }
}
