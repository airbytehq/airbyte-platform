/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsHelpers;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import jakarta.inject.Singleton;
import java.util.UUID;

@Singleton
public class SecretPersistenceConfigHandler {

  private final SecretsRepositoryWriter secretsRepositoryWriter;

  public SecretPersistenceConfigHandler(final SecretsRepositoryWriter secretsRepositoryWriter) {
    this.secretsRepositoryWriter = secretsRepositoryWriter;
  }

  @SuppressWarnings("LineLength")
  public io.airbyte.api.model.generated.SecretPersistenceConfig buildSecretPersistenceConfigResponse(
                                                                                                     final SecretPersistenceConfig secretPersistenceConfig) {
    return new io.airbyte.api.model.generated.SecretPersistenceConfig()
        .secretPersistenceType(
            Enums.toEnum(secretPersistenceConfig.getSecretPersistenceType().value(), io.airbyte.api.model.generated.SecretPersistenceType.class)
                .orElseThrow())
        ._configuration(Jsons.jsonNode(secretPersistenceConfig.getConfiguration()))
        .scopeType(Enums.toEnum(secretPersistenceConfig.getScopeType().value(), io.airbyte.api.model.generated.ScopeType.class).orElseThrow())
        .scopeId(secretPersistenceConfig.getScopeId());
  }

  public String writeToEnvironmentSecretPersistence(final SecretCoordinate secretCoordinate, final String payload) {
    secretsRepositoryWriter.storeSecretToDefaultSecretPersistence(secretCoordinate, payload);
    return secretCoordinate.getFullCoordinate();
  }

  public SecretCoordinate buildRsmCoordinate(final ScopeType scope, final UUID scopeId) {
    final String coordinateBase = SecretsHelpers.INSTANCE.getCoordinatorBase(
        String.format("airbyte_rsm_%s_", scope.name()),
        scopeId,
        UUID::randomUUID);
    return new SecretCoordinate(coordinateBase, 1);
  }

}
