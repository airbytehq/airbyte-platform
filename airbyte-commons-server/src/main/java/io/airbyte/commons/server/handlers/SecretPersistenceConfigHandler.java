/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceCoordinate;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsHelpers;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import jakarta.inject.Singleton;
import java.util.UUID;

@Singleton
public class SecretPersistenceConfigHandler {

  private final SecretsRepositoryReader secretsRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;

  public SecretPersistenceConfigHandler(final SecretsRepositoryReader secretsRepositoryReader,
                                        final SecretsRepositoryWriter secretsRepositoryWriter) {
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
  }

  @SuppressWarnings("LineLength")
  public io.airbyte.api.model.generated.SecretPersistenceConfig buildSecretPersistenceConfigResponse(
                                                                                                     final SecretPersistenceCoordinate secretPersistenceCoordinate)
      throws ConfigNotFoundException {
    final JsonNode config = secretsRepositoryReader.fetchSecret(
        SecretCoordinate.Companion.fromFullCoordinate(secretPersistenceCoordinate.getCoordinate()));

    if (config == null || config.isEmpty()) {
      throw new ConfigNotFoundException("secret_coordinate", secretPersistenceCoordinate.getCoordinate());
    }

    return new io.airbyte.api.model.generated.SecretPersistenceConfig()
        .secretPersistenceType(
            Enums.toEnum(secretPersistenceCoordinate.getSecretPersistenceType().value(), io.airbyte.api.model.generated.SecretPersistenceType.class)
                .orElseThrow())
        ._configuration(config)
        .scopeType(Enums.toEnum(secretPersistenceCoordinate.getScopeType().value(), io.airbyte.api.model.generated.ScopeType.class).orElseThrow())
        .scopeId(secretPersistenceCoordinate.getScopeId());
  }

  public String writeToEnvironmentSecretPersistence(final SecretCoordinate secretCoordinate, final String payload) {
    secretsRepositoryWriter.storeSecret(secretCoordinate, payload);
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
