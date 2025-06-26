/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.micronaut.runtime.Micronaut

/**
 * Metric Reporter application.
 *
 *
 * Responsible for emitting metric information on a periodic basis.
 */
fun main(args: Array<String>) {
  Micronaut
    .build(*args)
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()
}
