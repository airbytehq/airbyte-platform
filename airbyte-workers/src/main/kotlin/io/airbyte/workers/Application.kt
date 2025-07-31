/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers

import io.micronaut.runtime.Micronaut

/**
 * Worker micronaut application.
 */
fun main(args: Array<String>) {
  Micronaut
    .build(*args)
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()
}
