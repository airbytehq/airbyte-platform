/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.helpers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.common.ConfigurableActor
import io.airbyte.api.server.constants.DESTINATION_TYPE
import io.airbyte.api.server.constants.SOURCE_TYPE

/**
 * Removes the sourceType node from the actor's configuration.
 *
 * @param actor any actor model marked as a configurable actor via the x-implements extension in the
 * api.yaml.
 */
fun removeSourceTypeNode(actor: ConfigurableActor) {
  removeConfigurationNode(actor, SOURCE_TYPE)
}

fun removeConfigurationNode(
  actor: ConfigurableActor,
  node: String,
) {
  val configuration = actor.configuration as ObjectNode
  configuration.remove(node)
}

/**
 * Removes the destinationType node from the actor's configuration.
 *
 * @param actor any actor model marked as a configurable actor via the x-implements extension in the
 * api.yaml.
 */
fun removeDestinationType(actor: ConfigurableActor) {
  removeConfigurationNode(actor, DESTINATION_TYPE)
}
