/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.config.secrets.SecretsHelpers
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository

@Singleton
class SecretReferenceService(
  private val secretReferenceRepository: SecretReferenceRepository,
) {
  private fun assertConfigReferenceIdsExist(
    config: JsonNode,
    existingReferenceIds: Set<SecretReferenceId>,
  ) {
    val configReferenceIds = SecretsHelpers.SecretReferenceHelpers.getSecretReferenceIdsFromConfig(config)
    for (id in configReferenceIds) {
      // This is pretty naive since the ref ids could be in spots that don't match the reference's hydrationPath.
      if (!existingReferenceIds.contains(SecretReferenceId(id))) {
        throw IllegalArgumentException("Secret reference $id does not exist but is referenced in the config")
      }
    }
  }

  fun getConfigWithSecretReferences(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
    config: JsonNode,
  ): ConfigWithSecretReferences {
    val refsForScope = secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(scopeType, scopeId)
    assertConfigReferenceIdsExist(config, refsForScope.map { it.secretReference.id }.toSet())

    val legacyCoordRefMap = SecretsHelpers.SecretReferenceHelpers.getReferenceMapFromConfig(InlinedConfigWithSecretRefs(config))
    val persistedRefMap =
      refsForScope.filter { it.secretReference.hydrationPath != null }.associateBy(
        { it.secretReference.hydrationPath!! },
        {
          SecretReferenceConfig(
            secretCoordinate = SecretCoordinate.fromFullCoordinate(it.secretConfig.externalCoordinate),
            secretStorageId = it.secretConfig.secretStorageId,
          )
        },
      )

    val referencedSecrets = legacyCoordRefMap + persistedRefMap
    return ConfigWithSecretReferences(config, referencedSecrets)
  }
}
