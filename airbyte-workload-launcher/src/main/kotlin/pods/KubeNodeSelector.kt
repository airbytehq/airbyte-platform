/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.NodeSelectorOverride
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Encapsulates the logic used to determine the node for a pod given a specific worker configuration.
 */
@Singleton
class KubeNodeSelector(
  private val featureFlagClient: FeatureFlagClient,
) {
  fun getNodeSelectors(
    usesCustomConnector: Boolean,
    workerConfigs: WorkerConfigs,
    connectionId: UUID? = null,
  ): Map<String, String> {
    val overrides = getNodeSelectorsOverride(connectionId = connectionId)
    return if (!overrides.isNullOrEmpty()) {
      overrides
    } else {
      if (usesCustomConnector) {
        workerConfigs.workerIsolatedKubeNodeSelectors ?: workerConfigs.workerKubeNodeSelectors
      } else {
        workerConfigs.workerKubeNodeSelectors
      }!!
    }
  }

  private fun getNodeSelectorsOverride(connectionId: UUID?): Map<String, String>? =
    if (connectionId == null) {
      null
    } else {
      val flagContext = Connection(connectionId)
      val nodeSelectorOverride = featureFlagClient.stringVariation(NodeSelectorOverride, flagContext)
      if (nodeSelectorOverride.isBlank()) {
        null
      } else {
        nodeSelectorOverride.toNodeSelectorMap()
      }
    }
}

@InternalForTesting
internal fun String.toNodeSelectorMap(): Map<String, String> =
  split(";")
    .associate {
      val (key, value) = it.split("=")
      key.trim() to value.trim()
    }
