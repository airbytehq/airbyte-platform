/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server

import io.micronaut.runtime.Micronaut

/**
 * Config Server Micronaut App.
 */
fun main(args: Array<String>) {
  Micronaut
    .build(*args)
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()
}
