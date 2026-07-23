/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.server

import io.micronaut.runtime.Micronaut.build

class Application {
  companion object {
    @JvmStatic fun main(args: Array<String>) {
      build(*args)
        .deduceCloudEnvironment(false)
        .deduceEnvironment(false)
        .mainClass(Application::class.java)
        .start()
    }
  }
}
