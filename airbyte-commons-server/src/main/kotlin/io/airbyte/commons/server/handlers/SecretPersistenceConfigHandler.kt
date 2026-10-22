/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.SecretPersistenceType
import io.airbyte.commons.enums.toEnum
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
  ): io.airbyte.api.server.generated.models.SecretPersistenceConfig =
    io.airbyte.api.server.generated.models.SecretPersistenceConfig(
      secretPersistenceType = secretPersistenceConfig.secretPersistenceType.value().toEnum<SecretPersistenceType>()!!,
      configuration = Jsons.jsonNode(secretPersistenceConfig.configuration),
      scopeType = secretPersistenceConfig.scopeType.value().toEnum<io.airbyte.api.server.generated.models.ScopeType>()!!,
      scopeId = secretPersistenceConfig.scopeId,
    )

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
