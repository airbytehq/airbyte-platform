/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.api.server.apiTracking

import io.airbyte.config.Configs
import io.airbyte.config.EnvConfigs
import jakarta.inject.Singleton

/**
 * Used to track usage of the airbyte-api-server.
 */
@Singleton
class TrackingClient {
  val configs: Configs = EnvConfigs()

  init {
    setupTrackingClient(configs.airbyteVersion, configs.deploymentMode, configs.trackingStrategy, configs.workerEnvironment)
  }
}
