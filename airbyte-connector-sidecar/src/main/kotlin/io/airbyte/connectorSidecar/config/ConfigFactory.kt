/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar.config

import io.airbyte.workers.pod.FileConstants
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

@Factory
class ConfigFactory {
  /**
   * Returns the path of where the connector output is being stored
   */
  @Singleton
  @Named("output")
  fun output(): Path = Path.of(FileConstants.JOB_OUTPUT_FILE)
}
