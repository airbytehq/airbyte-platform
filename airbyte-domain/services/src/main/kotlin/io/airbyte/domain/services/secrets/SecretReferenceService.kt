/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.config.secrets.ConfigWithProcessedSecrets
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.ConfigWithSecretReferenceIdReplacements
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.data.services.SecretConfigService as SecretConfigRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository

private val logger = KotlinLogging.logger {}

/**
 * Service for performing operations related to Airbyte's SecretReference domain model.
 */
@Singleton
class SecretReferenceService(
  private val secretReferenceRepository: SecretReferenceRepository,
  private val secretConfigRepository: SecretConfigRepository,
) {
  /**
   * Given an [actorConfig], create SecretConfig/SecretReference records for each secret
   * coordinate and replace them with their respective secret reference IDs in the returned config.
   *
   * Note: This method also deletes any dangling secret references that are no longer referenced
   * in the config. This cleans up any secret references that are no longer relevant after config
   * updates.
   *
   * @return an updated [JsonNode] config with secret nodes replaced with objects that
   * contain a secretReferenceId and secretStorageId.
   */
  @JvmName("createAndInsertSecretReferencesWithStorageId")
  fun createAndInsertSecretReferencesWithStorageId(
    actorConfig: ConfigWithProcessedSecrets,
    actorId: ActorId,
    secretStorageId: SecretStorageId,
    currentUserId: UserId,
  ): ConfigWithSecretReferenceIdReplacements {
    val createdSecretRefIdByPath = mutableMapOf<String, SecretReferenceId>()
    actorConfig.processedSecrets.forEach { path, secretNode ->
      if (secretNode.secretReferenceId != null) {
        return@forEach
      }
      val coordinate = secretNode.secretCoordinate
      if (coordinate == null) {
        throw IllegalStateException("Secret node at path $path does not have a secret coordinate. This is unexpected and likely indicates a bug.")
      }
      val secretRefId =
        createSecretConfigAndReference(
          secretStorageId = secretStorageId,
          externalCoordinate = coordinate.fullCoordinate,
          airbyteManaged = coordinate is AirbyteManagedSecretCoordinate,
          currentUserId = currentUserId,
          hydrationPath = path,
          scopeType = SecretReferenceScopeType.ACTOR,
          scopeId = actorId.value,
        )
      createdSecretRefIdByPath[path] = secretRefId
    }

    cleanupDanglingSecretReferences(actorId, actorConfig)

    return SecretReferenceHelpers.replaceSecretNodesWithSecretReferenceIds(
      actorConfig.originalConfig,
      createdSecretRefIdByPath,
    )
  }

  /**
   * For the given actorId, delete any secret references that are no longer referenced in its
   * provided config.
   */
  private fun cleanupDanglingSecretReferences(
    actorId: ActorId,
    config: ConfigWithProcessedSecrets,
  ) {
    val secretPathsFromConfig = config.processedSecrets.keys.toSet()
    val secretPathsFromExistingReferences =
      secretReferenceRepository
        .listByScopeTypeAndScopeId(SecretReferenceScopeType.ACTOR, actorId.value)
        .mapNotNull { it.hydrationPath }
        .toSet()
    val danglingSecretPaths = secretPathsFromExistingReferences - secretPathsFromConfig
    if (danglingSecretPaths.isNotEmpty()) {
      logger.info { "Deleting dangling secret references for actor $actorId: $danglingSecretPaths" }
      danglingSecretPaths.forEach {
        secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(
          scopeType = SecretReferenceScopeType.ACTOR,
          scopeId = actorId.value,
          hydrationPath = it,
        )
      }
    }
  }

  fun createSecretConfigAndReference(
    secretStorageId: SecretStorageId,
    externalCoordinate: String,
    airbyteManaged: Boolean,
    currentUserId: UserId,
    hydrationPath: String,
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): SecretReferenceId {
    // Create a secret config for this storage and coordinate if one does not already exist
    var secretConfig = secretConfigRepository.findByStorageIdAndExternalCoordinate(secretStorageId, externalCoordinate)
    if (secretConfig == null) {
      secretConfig =
        secretConfigRepository.create(
          SecretConfigCreate(
            secretStorageId = secretStorageId,
            descriptor = externalCoordinate,
            externalCoordinate = externalCoordinate,
            airbyteManaged = airbyteManaged,
            createdBy = currentUserId,
          ),
        )
      logger.info { "Created secret config ${secretConfig.id} for coordinate $externalCoordinate in secret storage $secretStorageId" }
    }
    // Create a secret reference for the secret config using the prefixed reference value's path
    val secretRef =
      secretReferenceRepository.createAndReplace(
        SecretReferenceCreate(
          secretConfigId = secretConfig.id,
          hydrationPath = hydrationPath,
          scopeType = scopeType,
          scopeId = scopeId,
        ),
      )
    logger.info { "Created secret reference ${secretRef.id} for secret config ${secretConfig.id} and $scopeType $scopeId" }
    return secretRef.id
  }

  private fun assertConfigReferenceIdsExist(
    config: JsonNode,
    existingReferenceIds: Set<SecretReferenceId>,
  ) {
    val configReferenceIds = SecretReferenceHelpers.getSecretReferenceIdsFromConfig(config)
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

    val secretRefsInConfig = SecretReferenceHelpers.getReferenceMapFromConfig(InlinedConfigWithSecretRefs(config))

    val persistedSecretRefs =
      refsForScope.filter { it.secretReference.hydrationPath != null }.associateBy(
        { it.secretReference.hydrationPath!! },
        {
          SecretReferenceConfig(
            secretCoordinate = SecretCoordinate.fromFullCoordinate(it.secretConfig.externalCoordinate),
            secretStorageId = it.secretConfig.secretStorageId,
            secretReferenceId = it.secretReference.id.value,
          )
        },
      )

    // for any secret reference in the config, replace the corresponding persisted secret
    // (if it exists) because it may have been updated in the incoming config
    val secretRefs = persistedSecretRefs + secretRefsInConfig
    return ConfigWithSecretReferences(config, secretRefs)
  }

  @JvmName("deleteActorSecretReferences")
  fun deleteActorSecretReferences(actorId: ActorId) {
    secretReferenceRepository.deleteByScopeTypeAndScopeId(
      scopeType = SecretReferenceScopeType.ACTOR,
      scopeId = actorId.value,
    )
  }
}
