/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivityImpl
import io.micronaut.context.annotation.Factory
import io.temporal.client.WorkflowClient
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory

@Factory
class ConnectorRolloutWorkerFactory {
  private val log = LoggerFactory.getLogger(ConnectorRolloutWorkerFactory::class.java)

  @Singleton
  @Named("connectorRolloutWorkerFactory")
  fun connectorRolloutWorkerFactory(
    workflowClient: WorkflowClient,
    startRolloutActivityImpl: StartRolloutActivityImpl,
    getRolloutActivityImpl: GetRolloutActivityImpl,
    findRolloutActivityImpl: FindRolloutActivityImpl,
    updateRolloutActivityImpl: DoRolloutActivityImpl,
    finalizeRolloutActivityImpl: FinalizeRolloutActivityImpl,
  ): WorkerFactory {
    log.info("ConnectorRolloutWorkerFactory registering workflow")
    val workerFactory = WorkerFactory.newInstance(workflowClient)
    val worker: Worker = workerFactory.newWorker(Constants.TASK_QUEUE)
    worker.registerWorkflowImplementationTypes(ConnectorRolloutWorkflowImpl::class.java)
    worker.registerActivitiesImplementations(
      startRolloutActivityImpl,
      getRolloutActivityImpl,
      findRolloutActivityImpl,
      updateRolloutActivityImpl,
      finalizeRolloutActivityImpl,
    )
    return workerFactory
  }
}
