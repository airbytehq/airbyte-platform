package io.airbyte.workload.launcher.config

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for general workload beans.
 */
@Factory
class WorkloadBeanFactory {
  @Singleton
  fun workerConfigs(workerConfigsProvider: WorkerConfigsProvider): WorkerConfigs {
    return workerConfigsProvider.getConfig(WorkerConfigsProvider.ResourceType.REPLICATION)
  }
}
