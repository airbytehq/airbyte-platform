/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.micronaut.runtime.Micronaut.build

object ConnectorRolloutWorkerApplication {
  @JvmStatic
  fun main(args: Array<String>) {
    val context =
      build(*args)
        .deduceCloudEnvironment(false)
        .deduceEnvironment(false)
        .mainClass(ConnectorRolloutWorkerApplication::class.java)
        .start()
    val rolloutWorker = context.getBean(ConnectorRolloutWorker::class.java)
    rolloutWorker.startWorker()
  }
}
