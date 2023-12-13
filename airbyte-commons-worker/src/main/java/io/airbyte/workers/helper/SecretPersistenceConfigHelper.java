/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import io.airbyte.api.client.model.generated.SecretPersistenceConfig;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ScopeType;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;

public class SecretPersistenceConfigHelper {

  public static RuntimeSecretPersistence fromApiSecretPersistenceConfig(final SecretPersistenceConfig apiSecretPersistenceConfig) {
    final io.airbyte.config.SecretPersistenceConfig secretPersistenceConfig = new io.airbyte.config.SecretPersistenceConfig()
        .withScopeType(Enums.convertTo(apiSecretPersistenceConfig.getScopeType(), ScopeType.class))
        .withScopeId(apiSecretPersistenceConfig.getScopeId())
        .withConfiguration(Jsons.deserializeToStringMap(apiSecretPersistenceConfig.getConfiguration()))
        .withSecretPersistenceType(
            Enums.convertTo(
                apiSecretPersistenceConfig.getSecretPersistenceType(),
                io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType.class));

    return new RuntimeSecretPersistence(secretPersistenceConfig);
  }

}
