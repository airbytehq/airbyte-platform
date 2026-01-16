/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.publicApi.server.generated.models.DestinationCreateRequest
import io.airbyte.publicApi.server.generated.models.DestinationPatchRequest
import io.airbyte.publicApi.server.generated.models.DestinationPutRequest
import io.airbyte.publicApi.server.generated.models.SourceCreateRequest
import io.airbyte.publicApi.server.generated.models.SourcePatchRequest
import io.airbyte.publicApi.server.generated.models.SourcePutRequest
import io.airbyte.server.apis.publicapi.constants.DESTINATION_TYPE
import io.airbyte.server.apis.publicapi.constants.SOURCE_TYPE

/**
 * Removes the sourceType node from the actor's configuration.
 *
 * @param actor any actor model marked as a configurable actor via the x-implements extension in the
 * api.yaml.
 */
fun removeSourceTypeNode(actor: SourceCreateRequest) {
  removeConfigurationNode(actor, SOURCE_TYPE)
}

fun removeSourceTypeNode(actor: SourcePatchRequest) {
  removeConfigurationNode(actor, SOURCE_TYPE)
}

fun removeSourceTypeNode(actor: SourcePutRequest) {
  removeConfigurationNode(actor, SOURCE_TYPE)
}

/**
 * Removes the destinationType node from the actor's configuration.
 *
 * @param actor any actor model marked as a configurable actor via the x-implements extension in the
 * api.yaml.
 */
fun removeDestinationType(actor: DestinationCreateRequest) {
  removeConfigurationNode(actor, DESTINATION_TYPE)
}

fun removeDestinationType(actor: DestinationPatchRequest) {
  removeConfigurationNode(actor, DESTINATION_TYPE)
}

fun removeDestinationType(actor: DestinationPutRequest) {
  removeConfigurationNode(actor, DESTINATION_TYPE)
}

fun removeConfigurationNode(
  actor: Any,
  node: String,
) {
  val configuration = getConfiguration(actor) ?: return
  (configuration as? ObjectNode)?.remove(node)
}

internal fun getConfiguration(actor: Any): JsonNode? =
  when (actor) {
    is DestinationCreateRequest -> actor.configuration
    is DestinationPatchRequest -> actor.configuration
    is DestinationPutRequest -> actor.configuration
    is SourceCreateRequest -> actor.configuration
    is SourcePatchRequest -> actor.configuration
    is SourcePutRequest -> actor.configuration
    else -> null
  }
