/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs;

import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import java.util.List;
import java.util.UUID;

/**
 * Interface to hide how source and destination definitions are fetched.
 */
public interface DefinitionsProvider {

  ConnectorRegistrySourceDefinition getSourceDefinition(final UUID definitionId) throws RegistryDefinitionNotFoundException;

  List<ConnectorRegistrySourceDefinition> getSourceDefinitions();

  ConnectorRegistryDestinationDefinition getDestinationDefinition(final UUID definitionId) throws RegistryDefinitionNotFoundException;

  List<ConnectorRegistryDestinationDefinition> getDestinationDefinitions();

}
