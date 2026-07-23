/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar.config

import io.airbyte.commons.json.Jsons
import io.airbyte.micronaut.runtime.AirbyteConnectorConfig
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.pod.FileConstants
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton
import java.nio.file.Files.readString
import java.nio.file.Path.of

@Factory
class SidecarInputFactory {
  @Singleton
  fun sidecarInput(airbyteConnectorConfig: AirbyteConnectorConfig) = readSidecarInput(airbyteConnectorConfig.configDir)

  private fun readSidecarInput(configDir: String): SidecarInput {
    val inputContent = readString(of(configDir, FileConstants.SIDECAR_INPUT_FILE))
    return Jsons.deserialize(inputContent, SidecarInput::class.java)
  }
}
