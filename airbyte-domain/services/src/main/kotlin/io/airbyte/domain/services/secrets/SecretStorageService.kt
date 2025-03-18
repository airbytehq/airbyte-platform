/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageWithConfig
import io.airbyte.domain.models.WorkspaceId
import jakarta.inject.Singleton
import io.airbyte.data.services.OrganizationService as OrganizationRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository
import io.airbyte.data.services.SecretStorageService as SecretStorageRepository

/**
 * Domain service for performing operations related to Airbyte's SecretStorage domain model.
 */
interface SecretStorageService {
  /**
   * Get the secret storage for a given ID.
   *
   * @param id the ID of the secret storage to get
   * @return the secret storage for the given ID, or null if none exists
   */
  fun getById(id: SecretStorageId): SecretStorage

  /**
   * Get the secret storage for a workspace.
   *
   * @param workspaceId the workspace to get the secret storage for
   * @return the secret storage for the workspace, or null if none exists
   */
  fun getByWorkspaceId(workspaceId: WorkspaceId): SecretStorage?

  /**
   * Hydrate a secret storage with its configuration.
   *
   * @param secretStorage the secret storage whose configuration should be hydrated
   * @return the secret storage with its configuration hydrated
   */
  fun hydrateStorageConfig(secretStorage: SecretStorage): SecretStorageWithConfig
}

@Singleton
open class SecretStorageServiceImpl(
  private val secretStorageRepository: SecretStorageRepository,
  private val organizationRepository: OrganizationRepository,
  private val secretReferenceRepository: SecretReferenceRepository,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val secretConfigService: SecretConfigService,
) : SecretStorageService {
  override fun getById(id: SecretStorageId): SecretStorage =
    secretStorageRepository.findById(id)
      ?: throw ResourceNotFoundProblem(ProblemResourceData().resourceType(SecretStorage::class.simpleName).resourceId(id.value.toString()))

  override fun getByWorkspaceId(workspaceId: WorkspaceId): SecretStorage? {
    // If the workspace has a workspace-scoped secret storage, return the first one found.
    // Otherwise, return the first org-level secret storage found.
    // Note: At some point, we may want to support multiple secret storages per workspace, with
    // some kind of default to prioritize.
    return secretStorageRepository
      .listByScopeTypeAndScopeId(SecretStorageScopeType.WORKSPACE, workspaceId.value)
      .firstOrNull()
      ?: secretStorageRepository
        .listByScopeTypeAndScopeId(
          SecretStorageScopeType.ORGANIZATION,
          organizationRepository.getOrganizationForWorkspaceId(workspaceId.value).orElseThrow().organizationId,
        ).firstOrNull()
  }

  override fun hydrateStorageConfig(secretStorage: SecretStorage): SecretStorageWithConfig {
    if (secretStorage.configuredFromEnvironment) {
      // For now, we just don't support this code path because configuredFromEnvironment dataplanes
      // can only be hydrated/instantiated in their local environment, not through the API.
      throw UnsupportedOperationException("Cannot hydrate a secret storage that is configured from the environment")
    }
    val secretReferences = secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.SECRET_STORAGE, secretStorage.id!!.value)
    val secretReference =
      when (secretReferences.size) {
        0 -> throw ResourceNotFoundProblem(
          ProblemResourceData().resourceType(SecretReference::class.simpleName).resourceId(secretStorage.id!!.value.toString()),
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
    val config = secretsRepositoryReader.fetchSecretFromDefaultSecretPersistence(SecretCoordinate.fromFullCoordinate(secretCoordinate))
    return SecretStorageWithConfig(
      secretStorage,
      config,
    )
  }
}
