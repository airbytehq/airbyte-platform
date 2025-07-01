/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import io.airbyte.config.ActorType
import java.io.Serial
import java.util.UUID

/**
 * Exception when the requested registry definition cannot be found.
 */
class RegistryDefinitionNotFoundException(
  type: ActorType?,
  id: String?,
) : Exception(String.format("type: %s definition / id: %s", type, id)) {
  constructor(type: ActorType?, uuid: UUID) : this(type, uuid.toString())

  companion object {
    @Serial
    private const val serialVersionUID = 3952310152259568607L
  }
}
