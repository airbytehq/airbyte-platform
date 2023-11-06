/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType;
import io.airbyte.config.SecretPersistenceCoordinate;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public interface SecretPersistenceConfigService {

  Optional<SecretPersistenceCoordinate> getSecretPersistenceConfig(ScopeType scope, UUID scopeId) throws IOException;

  Optional<SecretPersistenceCoordinate> getSecretPersistenceConfig(UUID workspaceId, UUID organizationId) throws IOException;

  Optional<SecretPersistenceCoordinate> createOrUpdateSecretPersistenceConfig(ScopeType scope,
                                                                              UUID scopeId,
                                                                              SecretPersistenceType secretPersistenceType,
                                                                              String secretPersistenceConfigCoordinate)
      throws IOException;

}
