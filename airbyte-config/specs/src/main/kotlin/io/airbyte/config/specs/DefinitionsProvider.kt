/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import java.util.UUID

/**
 * Interface to hide how source and destination definitions are fetched.
 */
interface DefinitionsProvider {
  @Throws(RegistryDefinitionNotFoundException::class)
  fun getSourceDefinition(definitionId: UUID): ConnectorRegistrySourceDefinition

  fun getSourceDefinitions(): List<ConnectorRegistrySourceDefinition>

  @Throws(RegistryDefinitionNotFoundException::class)
  fun getDestinationDefinition(definitionId: UUID): ConnectorRegistryDestinationDefinition

  fun getDestinationDefinitions(): List<ConnectorRegistryDestinationDefinition>
}
