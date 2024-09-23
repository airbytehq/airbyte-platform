/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.temporal.worker.WorkerFactory
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory

@Singleton
class ConnectorRolloutWorker(
  @Named("connectorRolloutWorkerFactory") private val workerFactory: WorkerFactory,
) {
  private val log = LoggerFactory.getLogger(ConnectorRolloutWorker::class.java)

  fun startWorker() {
    workerFactory.start()
  }
}
