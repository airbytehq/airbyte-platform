/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.SecretPersistenceType
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsRepositoryWriter
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class SecretPersistenceConfigHandler(
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
) {
  fun buildSecretPersistenceConfigResponse(
    secretPersistenceConfig: SecretPersistenceConfig,
  ): io.airbyte.api.model.generated.SecretPersistenceConfig =
    io.airbyte.api.model.generated
      .SecretPersistenceConfig()
      .secretPersistenceType(
        Enums
          .toEnum(
            secretPersistenceConfig.secretPersistenceType.value(),
            SecretPersistenceType::class.java,
          ).orElseThrow(),
      )._configuration(Jsons.jsonNode(secretPersistenceConfig.configuration))
      .scopeType(Enums.toEnum(secretPersistenceConfig.scopeType.value(), io.airbyte.api.model.generated.ScopeType::class.java).orElseThrow())
      .scopeId(secretPersistenceConfig.scopeId)

  fun writeToEnvironmentSecretPersistence(
    secretCoordinate: AirbyteManagedSecretCoordinate,
    payload: String,
  ): String {
    secretsRepositoryWriter.storeInDefaultPersistence(secretCoordinate, payload)
    return secretCoordinate.fullCoordinate
  }

  fun buildRsmCoordinate(
    scope: ScopeType,
    scopeId: UUID,
  ): AirbyteManagedSecretCoordinate =
    AirbyteManagedSecretCoordinate(
      String.format("rsm_%s_", scope.name),
      scopeId,
      AirbyteManagedSecretCoordinate.DEFAULT_VERSION,
    ) { UUID.randomUUID() }
}
