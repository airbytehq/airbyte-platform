/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
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
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ReadSecretReferenceIdsInConfigs
import io.airbyte.featureflag.Workspace
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
  private val secretPersistenceService: SecretPersistenceService,
  private val secretsRepositoryReader: SecretsRepositoryReader,
) {
  @JvmName("createAndInsertSecretReferencesWithStorageId")
  fun createAndInsertSecretReferencesWithStorageId(
    config: ConfigWithProcessedSecrets,
    scopeId: UUID,
    scopeType: SecretReferenceScopeType,
    secretStorageId: SecretStorageId,
    currentUserId: UserId?,
  ): ConfigWithSecretReferenceIdsInjected {
    val createdSecretRefIdByPath = mutableMapOf<String, SecretReferenceId>()
    config.processedSecrets.forEach { (path, secretNode) ->
      if (secretNode.secretReferenceId != null) {
        return@forEach
      }
      val coordinate =
        secretNode.secretCoordinate ?: throw IllegalStateException(
          "Secret node at path $path does not have a secret coordinate. This is unexpected and likely indicates a bug.",
        )
      val secretRefId =
        createSecretConfigAndReference(
          secretStorageId = secretStorageId,
          externalCoordinate = coordinate.fullCoordinate,
          airbyteManaged = coordinate is AirbyteManagedSecretCoordinate,
          currentUserId = currentUserId,
          hydrationPath = path,
          scopeType = scopeType,
          scopeId = scopeId,
        )
      createdSecretRefIdByPath[path] = secretRefId
    }
    cleanupDanglingSecretReferences(scopeId, scopeType, config)

    return SecretReferenceHelpers.updateSecretNodesWithSecretReferenceIds(
      config.originalConfig,
      createdSecretRefIdByPath,
    )
  }

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
    currentUserId: UserId?,
  ): ConfigWithSecretReferenceIdsInjected =
    createAndInsertSecretReferencesWithStorageId(
      config = actorConfig,
      scopeId = actorId.value,
      scopeType = SecretReferenceScopeType.ACTOR,
      secretStorageId = secretStorageId,
      currentUserId = currentUserId,
    )

  /**
   * For the given actorId, delete any secret references that are no longer referenced in its
   * provided config.
   */
  private fun cleanupDanglingSecretReferences(
    scopeId: UUID,
    scopeType: SecretReferenceScopeType,
    config: ConfigWithProcessedSecrets,
  ) {
    val secretPathsFromConfig = config.processedSecrets.keys.toSet()
    val secretPathsFromExistingReferences =
      secretReferenceRepository
        .listByScopeTypeAndScopeId(scopeType, scopeId)
        .mapNotNull { it.hydrationPath }
        .toSet()
    val danglingSecretPaths = secretPathsFromExistingReferences - secretPathsFromConfig
    if (danglingSecretPaths.isNotEmpty()) {
      logger.info { "Deleting dangling secret references for $scopeType $scopeId: $danglingSecretPaths" }
      danglingSecretPaths.forEach {
        secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(
          scopeType = scopeType,
          scopeId = scopeId,
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

  fun getConfigWithSecretReferences(
    actorId: ActorId,
    config: JsonNode,
    workspaceId: WorkspaceId,
  ): ConfigWithSecretReferences =
    getConfigWithSecretReferences(
      scopeId = actorId.value,
      scopeType = SecretReferenceScopeType.ACTOR,
      config = config,
      workspaceId = workspaceId,
    )

  fun getConfigWithSecretReferences(
    scopeId: UUID,
    scopeType: SecretReferenceScopeType,
    config: JsonNode,
    workspaceId: WorkspaceId,
  ): ConfigWithSecretReferences {
    // If the feature flag to read secret reference IDs in configs is enabled, look up the
    // secret references for the actorId and "hydrate" them into the config. Otherwise,
    // skip the secret reference lookup and just use the secrets in the config as-is.
    // Note: we cannot look up the workspace from the actorId, because the actor may not be
    // persisted yet.
    //
    // Details -
    //
    // There are 2 types of secrets that may be in the config: persisted and non-persisted.
    //
    // Non-persisted secrets are always stored in `_secret` nodes. This is also how legacy secret references were stored.
    // Now, persisted secret references are stored in `_secret_reference_id`.
    // Both persisted and non-persisted secrets may be present at the same time. When this happens, the non-persisted secrets should take precedence.
    //
    // There are 3 cases where we expect `_secrets` to be populated:
    // 1. Secret references were persisted in the legacy format. In this case, we see a `_secret` key and no `_secret_reference_id`.
    // 2. Secrets were stored via dual-writes in the old & new format (`_secret` and `_secret_reference_id` are present.)
    // 3. An actor is being created or updated in the UI, and we're running `check` before anything has been persisted. (Only `_secret` is present.)
    //
    // When `_secret` is present we assume that they're newer than or were created at the same time as any secrets referenced by `_secret_reference_id`.
    // Therefore, we can safely always let `_secret` take precedence over secrets referenced by `_secret_reference_id`.
    var nonPersistedSecretRefsInConfig: Map<String, SecretReferenceConfig>
    var refsForScope: List<SecretReferenceWithConfig>
    val orgId = workspaceHelper.getOrganizationForWorkspace(workspaceId.value)

    if (featureFlagClient.boolVariation(
        ReadSecretReferenceIdsInConfigs,
        Multi(listOf(Workspace(workspaceId.value), Organization(orgId))),
      )
    ) {
      // Get all persisted secret refs
      val result = secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(scopeType, scopeId)
      assertConfigReferenceIdsExist(config, result.map { it.secretReference.id }.toSet())
      refsForScope = result

      // Gather all non-persisted secret refs separately from the persisted ones
      nonPersistedSecretRefsInConfig =
        SecretReferenceHelpers.getReferenceMapFromConfig(InlinedConfigWithSecretRefs(config)).filter {
          it.value.secretReferenceId == null
        }
    } else {
      // Handle persisted and non-persisted secret refs together (downstream we'll only look at the non-persisted refs since the flag is off)
      refsForScope = emptyList()
      nonPersistedSecretRefsInConfig =
        SecretReferenceHelpers.getReferenceMapFromConfig(InlinedConfigWithSecretRefs(config))
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

  fun getHydratedConfiguration(
    config: ConfigWithSecretReferences,
    workspaceId: WorkspaceId,
  ): JsonNode {
    val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId.value)
    val hydrationContext = SecretHydrationContext(OrganizationId(organizationId), workspaceId)
    val secretPersistenceMap: Map<UUID?, SecretPersistence> = secretPersistenceService.getPersistenceMapFromConfig(config, hydrationContext)
    return secretsRepositoryReader.hydrateConfig(config, secretPersistenceMap)
  }

  fun getHydratedConfiguration(
    scopeId: UUID,
    scopeType: SecretReferenceScopeType,
    config: JsonNode,
    workspaceId: WorkspaceId,
  ): JsonNode {
    val configWithSecretRefs =
      getConfigWithSecretReferences(
        scopeId = scopeId,
        scopeType = scopeType,
        config = config,
        workspaceId = workspaceId,
      )
    return getHydratedConfiguration(configWithSecretRefs, workspaceId)
  }

  @JvmName("deleteActorSecretReferences")
  fun deleteActorSecretReferences(actorId: ActorId) {
    secretReferenceRepository.deleteByScopeTypeAndScopeId(
      scopeType = SecretReferenceScopeType.ACTOR,
      scopeId = actorId.value,
    )
  }
}
