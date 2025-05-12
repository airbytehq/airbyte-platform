/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.container.orchestrator.worker.BufferConfiguration
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.withBufferSize
import io.airbyte.container.orchestrator.worker.withDefaultConfiguration
import io.airbyte.featureflag.ReplicationBufferOverride
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.internal.NamespacingMapper
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ReplicationFactory {
  @Singleton
  fun bufferConfiguration(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader): BufferConfiguration {
    val bufferSize = replicationInputFeatureFlagReader.read(ReplicationBufferOverride)
    return if (bufferSize > 0) withBufferSize(bufferSize) else withDefaultConfiguration()
  }

  @Singleton
  fun namespaceMapper(replicationInput: ReplicationInput) =
    NamespacingMapper(
      replicationInput.namespaceDefinition,
      replicationInput.namespaceFormat,
      replicationInput.prefix,
    )

  @Singleton
  fun replicationContext(
    replicationContextProvider: ReplicationContextProvider,
    replicationInput: ReplicationInput,
  ) = replicationContextProvider.provideContext(replicationInput)

  @Singleton
  @Named("onReplicationRunning")
  fun replicationRunningCallback(
    @Named("workloadId") workloadId: String,
  ): VoidCallable = VoidCallable { workloadId }
}
