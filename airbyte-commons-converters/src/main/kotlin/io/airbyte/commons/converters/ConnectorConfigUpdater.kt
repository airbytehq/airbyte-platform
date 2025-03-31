/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import com.google.common.hash.Hashing
import datadog.trace.api.Trace
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationUpdate
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.SourceUpdate
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.Config
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.nio.charset.StandardCharsets
import java.util.UUID

val log = KotlinLogging.logger {}

/**
 * Helper class for workers to persist updates to Source/Destination configs emitted from
 * AirbyteControlMessages. This is in order to support connectors updating configs when running
 * commands, which is specially useful for migrating configuration to a new version or for enabling
 * connectors that require single-use or short-lived OAuth tokens.
 */
@Singleton
class ConnectorConfigUpdater(
  private val airbyteApiClient: AirbyteApiClient,
) {
  /**
   * Updates the Source from a sync job ID with the provided Configuration. Secrets and OAuth
   * parameters will be masked when saving.
   */
  @Trace
  fun updateSource(
    sourceId: UUID,
    config: Config,
  ) {
    val source = airbyteApiClient.sourceApi.getSource(SourceIdRequestBody(sourceId))

    val updatedSource =
      airbyteApiClient.sourceApi.updateSource(
        SourceUpdate(
          sourceId = sourceId,
          connectionConfiguration = Jsons.jsonNode(config.additionalProperties),
          name = source.name,
          secretId = null,
          resourceAllocation = source.resourceAllocation,
        ),
      )

    log.info {
      val hash = Hashing.sha256().hashString(updatedSource.connectionConfiguration.asText(), StandardCharsets.UTF_8)
      "Persisted updated configuration for source $sourceId. New config hash: $hash."
    }
  }

  /**
   * Updates the Destination from a sync job ID with the provided Configuration. Secrets and OAuth
   * parameters will be masked when saving.
   */
  fun updateDestination(
    destinationId: UUID,
    config: Config,
  ) {
    val destination = airbyteApiClient.destinationApi.getDestination(DestinationIdRequestBody(destinationId))

    val updatedDestination =
      airbyteApiClient.destinationApi.updateDestination(
        DestinationUpdate(
          destinationId = destinationId,
          connectionConfiguration = Jsons.jsonNode(config.additionalProperties),
          name = destination.name,
          resourceAllocation = destination.resourceAllocation,
        ),
      )

    log.info {
      val hash = Hashing.sha256().hashString(updatedDestination.connectionConfiguration.asText(), StandardCharsets.UTF_8)
      "Persisted updated configuration for destination $destinationId. New config hash: $hash."
    }
  }
}
