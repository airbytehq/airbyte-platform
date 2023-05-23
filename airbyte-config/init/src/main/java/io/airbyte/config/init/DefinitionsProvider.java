/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import java.util.List;
import java.util.UUID;

/**
 * Interface to hide how source and destination definitions are fetched.
 */
public interface DefinitionsProvider {

  ConnectorRegistrySourceDefinition getSourceDefinition(final UUID definitionId) throws ConfigNotFoundException;

  List<ConnectorRegistrySourceDefinition> getSourceDefinitions();

  ConnectorRegistryDestinationDefinition getDestinationDefinition(final UUID definitionId) throws ConfigNotFoundException;

  List<ConnectorRegistryDestinationDefinition> getDestinationDefinitions();

}
