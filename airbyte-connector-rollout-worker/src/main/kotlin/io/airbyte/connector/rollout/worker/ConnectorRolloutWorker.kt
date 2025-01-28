/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.temporal.worker.WorkerFactory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
class ConnectorRolloutWorker(
  @Named("connectorRolloutWorkerFactory") private val workerFactory: WorkerFactory,
) {
  fun startWorker() {
    workerFactory.start()
  }
}
