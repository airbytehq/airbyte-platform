/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.micronaut.runtime.Micronaut

object ConnectorRolloutWorkerApplication {
  @JvmStatic
  fun main(args: Array<String>) {
    val context = Micronaut.run(ConnectorRolloutWorkerApplication::class.java)
    val rolloutWorker = context.getBean(ConnectorRolloutWorker::class.java)
    rolloutWorker.startWorker()
  }
}
