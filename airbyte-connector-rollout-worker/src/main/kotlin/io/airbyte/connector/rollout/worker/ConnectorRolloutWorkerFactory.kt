/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.connector.rollout.worker.activities.CleanupActivityImpl
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.PauseRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivityImpl
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivityImpl
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Factory
import io.temporal.client.WorkflowClient
import io.temporal.worker.Worker
import io.temporal.worker.WorkerFactory
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Factory
class ConnectorRolloutWorkerFactory {
  @Singleton
  @Named("connectorRolloutWorkerFactory")
  fun connectorRolloutWorkerFactory(
    workflowClient: WorkflowClient,
    startRolloutActivityImpl: StartRolloutActivityImpl,
    getRolloutActivityImpl: GetRolloutActivityImpl,
    findRolloutActivityImpl: FindRolloutActivityImpl,
    updateRolloutActivityImpl: DoRolloutActivityImpl,
    finalizeRolloutActivityImpl: FinalizeRolloutActivityImpl,
    promoteOrRollbackActivityImpl: PromoteOrRollbackActivityImpl,
    verifyDefaultVersionActivityImpl: VerifyDefaultVersionActivityImpl,
    cleanupActivityImpl: CleanupActivityImpl,
    pauseRolloutActivityImpl: PauseRolloutActivityImpl,
  ): WorkerFactory {
    logger.info { "ConnectorRolloutWorkerFactory registering workflow" }
    val workerFactory = WorkerFactory.newInstance(workflowClient)
    val worker: Worker = workerFactory.newWorker(Constants.TASK_QUEUE)
    worker.registerWorkflowImplementationTypes(ConnectorRolloutWorkflowImpl::class.java)
    worker.registerActivitiesImplementations(
      startRolloutActivityImpl,
      getRolloutActivityImpl,
      findRolloutActivityImpl,
      updateRolloutActivityImpl,
      finalizeRolloutActivityImpl,
      promoteOrRollbackActivityImpl,
      verifyDefaultVersionActivityImpl,
      cleanupActivityImpl,
      pauseRolloutActivityImpl,
    )
    return workerFactory
  }
}
