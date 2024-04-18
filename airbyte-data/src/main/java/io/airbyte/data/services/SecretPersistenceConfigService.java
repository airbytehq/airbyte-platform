/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType;
import io.airbyte.config.SecretPersistenceCoordinate;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public interface SecretPersistenceConfigService {

  SecretPersistenceConfig getSecretPersistenceConfig(ScopeType scope, UUID scopeId) throws IOException, ConfigNotFoundException;

  Optional<SecretPersistenceCoordinate> createOrUpdateSecretPersistenceConfig(ScopeType scope,
                                                                              UUID scopeId,
                                                                              SecretPersistenceType secretPersistenceType,
                                                                              String secretPersistenceConfigCoordinate)
      throws IOException;

}
