/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate.Companion.DEFAULT_VERSION
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.domain.models.PatchField.Companion.toPatch
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorage.Companion.DEFAULT_SECRET_STORAGE_ID
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.EnableDefaultSecretStorage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.data.services.OrganizationService as OrganizationRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository
import io.airbyte.data.services.SecretStorageService as SecretStorageRepository

private val logger = KotlinLogging.logger {}

/**
 * Domain service for performing operations related to Airbyte's SecretStorage domain model.
 */
@Singleton
open class SecretStorageService(
  private val secretStorageRepository: SecretStorageRepository,
  private val organizationRepository: OrganizationRepository,
  private val secretReferenceRepository: SecretReferenceRepository,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  private val secretConfigService: SecretConfigService,
  private val secretReferenceService: SecretReferenceService,
  private val featureFlagClient: FeatureFlagClient,
) {
  /**
   * Get the secret storage for a given ID.
   *
   * @param id the ID of the secret storage to get
   * @return the secret storage for the given ID, or null if none exists
   */
  fun getById(id: SecretStorageId): SecretStorage =
    secretStorageRepository.findById(id)
      ?: throw ResourceNotFoundProblem(ProblemResourceData().resourceType(SecretStorage::class.simpleName).resourceId(id.value.toString()))

  /**
   * Writes the credentials for the new secret storage to the default secret persistence.
   * All credentials for custom secret storages are stored in the default secret persistence.
   */
  private fun writeStorageCredentials(
    secretStorageCreate: SecretStorageCreate,
    storageConfig: JsonNode?,
    secretStorageId: SecretStorageId,
    currentUserId: UserId?,
  ) {
    if (storageConfig == null) {
      // No config to store
      return
    }

    val secretCoordinate =
      secretsRepositoryWriter.storeInDefaultPersistence(
        SecretCoordinate.AirbyteManagedSecretCoordinate(
          "storage_${secretStorageCreate.scopeType.name.lowercase()}",
          secretStorageCreate.scopeId,
          DEFAULT_VERSION,
        ),
        Jsons.serialize(storageConfig),
      )

    secretReferenceService.createSecretConfigAndReference(
      DEFAULT_SECRET_STORAGE_ID,
      externalCoordinate = secretCoordinate.fullCoordinate,
      airbyteManaged = true,
      currentUserId = currentUserId,
      scopeType = SecretReferenceScopeType.SECRET_STORAGE,
      scopeId = secretStorageId.value,
      hydrationPath = null,
    )
  }

  /**
   * Create a new secret storage.
   */
  fun createSecretStorage(
    secretStorageCreate: SecretStorageCreate,
    storageConfig: JsonNode?,
  ): SecretStorage {
    if (!secretStorageCreate.configuredFromEnvironment && storageConfig == null) {
      throw IllegalArgumentException("Storage config must be provided when `configuredFromEnvironment` is false")
    }

    val secretStorage = secretStorageRepository.create(secretStorageCreate)
    writeStorageCredentials(secretStorageCreate, storageConfig, secretStorage.id, secretStorageCreate.createdBy)
    return secretStorage
  }

  /**
   * Soft-delete a secret storage by marking it as tombstoned. This will prevent it from being used for new secrets, but existing references pointing to it will continue working.
   */
  fun deleteSecretStorage(
    secretStorageId: SecretStorageId,
    currentUserId: UserId,
  ): SecretStorage {
    if (secretStorageId == DEFAULT_SECRET_STORAGE_ID) {
      throw IllegalArgumentException("Cannot disable the default secret storage")
    }

    val updatedStorage =
      secretStorageRepository.patch(
        secretStorageId,
        tombstone = true.toPatch(),
        updatedBy = currentUserId,
      )

    return updatedStorage
  }

  /**
   * List all active secret storages for a given scope type and scope ID.
   */
  fun listSecretStorage(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage> =
    secretStorageRepository
      .listByScopeTypeAndScopeId(scopeType, scopeId)
      .filterNot { it.tombstone }

  /**
   * Get the secret storage that a workspace is configured to use for storing secrets.
   *
   * If the workspace's organization has the runtime secret persistence feature flag enabled and there is no scoped secret storage,
   * return null so that the default secret storage is not used.
   *
   * @param workspaceId the workspace to get the secret storage for
   * @return the secret storage for the workspace, or null if none exists
   */
  @JvmName("getByWorkspaceId")
  fun getByWorkspaceId(workspaceId: WorkspaceId): SecretStorage? {
    // Start by looking for a secret storage that is scoped to the workspace. If none exists, look for
    // one that is scoped to the organization. If none exists, fall back to the default secret storage.
    val workspaceStorage =
      secretStorageRepository
        .listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
        .filterNot { it.tombstone }
        .firstOrNull()
    if (workspaceStorage != null) {
      return workspaceStorage
    }

    val orgId = organizationRepository.getOrganizationForWorkspaceId(workspaceId.value).orElseThrow().organizationId
    if (!featureFlagClient.boolVariation(EnableDefaultSecretStorage, Multi(listOf(Workspace(workspaceId.value), Organization(orgId))))) {
      return null
    }

    val orgStorage =
      secretStorageRepository
        .listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, orgId)
        .filterNot { it.tombstone }
        .firstOrNull()
    if (orgStorage != null) {
      return orgStorage
    }

    if (featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(orgId))) {
      logger.info { "Runtime secret persistence flag is enabled for organization $orgId. Skipping default secret storage lookup." }
      return null
    }

    return secretStorageRepository.findById(SecretStorage.DEFAULT_SECRET_STORAGE_ID)
      ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType(SecretStorage::class.simpleName)
          .resourceId(SecretStorage.DEFAULT_SECRET_STORAGE_ID.toString()),
      )
  }

  /**
   * Hydrate a secret storage with its configuration.
   *
   * @param secretStorage the secret storage whose configuration should be hydrated
   * @return the secret storage with its configuration hydrated
   */
  fun hydrateStorageConfig(secretStorage: SecretStorage): SecretStorageWithConfig {
    if (secretStorage.configuredFromEnvironment) {
      // For now, we just don't support this code path because configuredFromEnvironment dataplanes
      // can only be hydrated/instantiated in their local environment, not through the API.
      throw UnsupportedOperationException("Cannot hydrate a secret storage that is configured from the environment")
    }
    val secretReferences =
      secretReferenceRepository.listByScopeTypeAndScopeId(
        SecretReferenceScopeType.SECRET_STORAGE,
        scopeId = secretStorage.id.value,
      )
    val secretReference =
      when (secretReferences.size) {
        0 -> throw ResourceNotFoundProblem(
          ProblemResourceData().resourceType(SecretReference::class.simpleName).resourceId(secretStorage.id.value.toString()),
        )
        1 -> secretReferences.first()
        else -> throw IllegalStateException("Multiple secret references found for secret storage ${secretStorage.id}")
      }
    val secretConfig =
      secretConfigService.getById(secretReference.secretConfigId)

    val secretCoordinate = secretConfig.externalCoordinate
    // Note: this assumes that the secret is stored in the default secret persistence. Technically,
    // the secretStorage's secretConfig should specify an explicit secretStorageId, but we don't yet
    // represent the default secret persistence as a secretStorage in the control plane so we
    // instead just assume the coordinate resides in the default secret persistence.
    val config = secretsRepositoryReader.fetchJsonSecretFromDefaultSecretPersistence(SecretCoordinate.fromFullCoordinate(secretCoordinate))
    return SecretStorageWithConfig(
      secretStorage,
      config,
    )
  }
}
