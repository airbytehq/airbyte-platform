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
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.ConfigWithSecretReferenceIdsInjected
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.PersistSecretConfigsAndReferences
import io.airbyte.featureflag.ReadSecretReferenceIdsInConfigs
import io.airbyte.featureflag.Workspace
import io.airbyte.persistence.job.WorkspaceHelper
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
  private val featureFlagClient: FeatureFlagClient,
  private val workspaceHelper: WorkspaceHelper,
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
    workspaceId: WorkspaceId,
    secretStorageId: SecretStorageId,
    currentUserId: UserId?,
  ): ConfigWithSecretReferenceIdsInjected {
    // If the feature flag to persist secret configs and references is not enabled,
    // return the original config without any changes.
    // Note: we cannot look up the workspace from the actorId, because the actor may not be
    // persisted yet.
    val orgId = workspaceHelper.getOrganizationForWorkspace(workspaceId.value)
    if (!featureFlagClient.boolVariation(
        PersistSecretConfigsAndReferences,
        Multi(listOf(Workspace(workspaceId.value), Organization(orgId))),
      )
    ) {
      return ConfigWithSecretReferenceIdsInjected(actorConfig.originalConfig)
    }

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

    return SecretReferenceHelpers.updateSecretNodesWithSecretReferenceIds(
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
    currentUserId: UserId?,
    hydrationPath: String?,
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
      if (!existingReferenceIds.contains(SecretReferenceId(id))) {
        throw IllegalArgumentException("Secret reference $id does not exist but is referenced in the config")
      }
    }
  }

  @JvmName("getConfigWithSecretReferences")
  fun getConfigWithSecretReferences(
    actorId: ActorId,
    config: JsonNode,
    workspaceId: WorkspaceId,
  ): ConfigWithSecretReferences {
    // If the feature flag to read secret reference IDs in configs is enabled, look up the
    // secret references for the actorId and "hydrate" them into the config. Otherwise,
    // skip the secret reference lookup and just use the secrets in the config as-is.
    // Note: we cannot look up the workspace from the actorId, because the actor may not be
    // persisted yet.
    val orgId = workspaceHelper.getOrganizationForWorkspace(workspaceId.value)
    val refsForScope =
      if (featureFlagClient.boolVariation(
          ReadSecretReferenceIdsInConfigs,
          Multi(listOf(Workspace(workspaceId.value), Organization(orgId))),
        )
      ) {
        val result = secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(SecretReferenceScopeType.ACTOR, actorId.value)
        assertConfigReferenceIdsExist(config, result.map { it.secretReference.id }.toSet())
        result
      } else {
        emptyList()
      }

    val nonPersistedSecretRefsInConfig =
      SecretReferenceHelpers.getReferenceMapFromConfig(InlinedConfigWithSecretRefs(config)).filter {
        it.value.secretReferenceId == null
      }

    val persistedSecretRefs =
      refsForScope
        .filter {
          it.secretReference.hydrationPath != null &&
            SecretReferenceHelpers.getSecretReferenceIdAtPath(config, it.secretReference.hydrationPath!!) == it.secretReference.id
        }.associateBy(
          { it.secretReference.hydrationPath!! },
          {
            SecretReferenceConfig(
              secretCoordinate = SecretCoordinate.fromFullCoordinate(it.secretConfig.externalCoordinate),
              secretStorageId = it.secretConfig.secretStorageId,
              secretReferenceId = it.secretReference.id.value,
            )
          },
        )

    // apply the non-persisted secret references over the persisted ones, because the non-persisted refs
    // are updated
    val secretRefs = persistedSecretRefs + nonPersistedSecretRefsInConfig
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
