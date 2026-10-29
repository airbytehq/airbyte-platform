/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner

import io.micronaut.runtime.Micronaut.build

fun main(args: Array<String>) {
  build(*args)
    .deduceCloudEnvironment(false)
    .deduceEnvironment(false)
    .start()
}
