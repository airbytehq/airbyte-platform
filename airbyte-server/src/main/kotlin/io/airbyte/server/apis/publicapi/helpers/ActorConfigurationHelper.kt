/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.common.ConfigurableActor
import io.airbyte.server.apis.publicapi.constants.DESTINATION_TYPE
import io.airbyte.server.apis.publicapi.constants.SOURCE_TYPE
import java.util.Optional

/**
 * Removes the sourceType node from the actor's configuration.
 *
 * @param actor any actor model marked as a configurable actor via the x-implements extension in the
 * api.yaml.
 */
fun removeSourceTypeNode(actor: ConfigurableActor) {
  removeConfigurationNode(actor, SOURCE_TYPE)
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

fun removeConfigurationNode(
  actor: ConfigurableActor,
  node: String,
) {
  if (actor.configuration == null) {
    return
  }

  val configuration = Optional.ofNullable((actor.configuration as ObjectNode))
  configuration.ifPresent { config: ObjectNode ->
    config.remove(
      node,
    )
  }
}
