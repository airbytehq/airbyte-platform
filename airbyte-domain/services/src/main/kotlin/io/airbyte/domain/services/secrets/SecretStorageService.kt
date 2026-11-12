/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.AwsSecretsManagerRuntimeConfiguration
import io.airbyte.config.secrets.persistence.GoogleSecretsManagerRuntimeConfig
import io.airbyte.config.secrets.persistence.RuntimeConfigError
import io.airbyte.config.secrets.persistence.VaultSecretsManagerRuntimeConfiguration
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.PatchField.Companion.toPatch
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorage.Companion.DEFAULT_SECRET_STORAGE_ID
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
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
import secrets.persistence.AzureKeyVaultRuntimeConfiguration
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
  private val secretConfigService: SecretConfigService,
  private val featureFlagClient: FeatureFlagClient,
  private val workspaceRepository: WorkspaceRepository,
  private val dataplaneRepository: DataplaneRepository,
  private val dataplaneGroupRepository: DataplaneGroupRepository,
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
   * Create a new secret storage.
   * Note: This method only creates the secret storage metadata. The caller is responsible for
   * storing the credentials using SecretStorageCredentialsService.writeStorageCredentials().
   */
  fun createSecretStorage(
    secretStorageCreate: SecretStorageCreate,
    storageConfig: JsonNode?,
  ): SecretStorage {
    if (!secretStorageCreate.configuredFromEnvironment && storageConfig == null) {
      throw IllegalArgumentException("Storage config must be provided when `configuredFromEnvironment` is false")
    }
    storageConfig?.let { validateStorageConfig(secretStorageCreate.storageType, it) }

    return secretStorageRepository.create(secretStorageCreate)
  }

  /**
   * The config is persisted verbatim and only parsed when the storage is first used, where every
   * storage type hydrates it via [io.airbyte.commons.json.Jsons.deserializeToStringMap] into a flat
   * string map. Rejecting malformed configs here surfaces the error to the caller at creation time
   * instead of failing every later secret operation scoped to this storage.
   */
  private fun validateStorageConfig(
    storageType: SecretStorageType,
    storageConfig: JsonNode,
  ) {
    if (!storageConfig.isObject) {
      throw BadRequestProblem(ProblemMessageData().message("Secret storage config must be a JSON object."))
    }

    val nonStringKeys =
      storageConfig
        .fields()
        .asSequence()
        .filter { !it.value.isTextual }
        .map { it.key }
        .sorted()
        .toList()
    if (nonStringKeys.isNotEmpty()) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Secret storage config must be a flat map of string values. Keys with non-string values: $nonStringKeys",
        ),
      )
    }

    val validator =
      when (storageType) {
        SecretStorageType.AZURE_KEY_VAULT -> AzureKeyVaultRuntimeConfiguration
        SecretStorageType.GOOGLE_SECRET_MANAGER -> GoogleSecretsManagerRuntimeConfig
        SecretStorageType.VAULT -> VaultSecretsManagerRuntimeConfiguration
        SecretStorageType.AWS_SECRETS_MANAGER -> AwsSecretsManagerRuntimeConfiguration
        SecretStorageType.LOCAL_TESTING -> null
      }
    val configMap = storageConfig.fields().asSequence().associate { it.key to it.value.asText() }
    when (val error = validator?.validate(configMap)) {
      is RuntimeConfigError.MissingKeys ->
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Secret storage config for $storageType is missing required keys: ${error.keys}",
          ),
        )
      is RuntimeConfigError.InvalidValue ->
        throw BadRequestProblem(
          ProblemMessageData().message(
            "Secret storage config has invalid ${error.key} '${error.value}'. Valid values: ${error.validValues}",
          ),
        )
      null -> {}
    }
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

    // If there's no workspace-specific secret storage, look for an organization-specific one.
    // This also handles falling back to the default secret storage if no organization-specific storage exists.
    return getByOrganizationId(OrganizationId(orgId))
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

  /**
   * Get the secret storage that an org is configured to use for storing secrets.
   *
   * If the organization has the runtime secret persistence feature flag enabled and there is no scoped secret storage,
   * return null so that the default secret storage is not used.
   *
   * @param organizationId the org to get the secret storage for
   * @return the secret storage for the organization, or null if none exists
   */
  @JvmName("getByOrganizationId")
  fun getByOrganizationId(organizationId: OrganizationId): SecretStorage {
    val orgStorage =
      secretStorageRepository
        .listByScopeTypeAndScopeId(SecretStorageScopeType.ORGANIZATION, organizationId.value)
        .filterNot { it.tombstone }
        .firstOrNull()
    if (orgStorage != null) {
      return orgStorage
    }

    return secretStorageRepository.findById(DEFAULT_SECRET_STORAGE_ID)
      ?: throw ResourceNotFoundProblem(
        ProblemResourceData()
          .resourceType(SecretStorage::class.simpleName)
          .resourceId(DEFAULT_SECRET_STORAGE_ID.toString()),
      )
  }

  /**
   * Returns the secret storages used by the workspaces in the data plane's region, without duplicates.
   * If the region has no workspaces, returns the organization's storage.
   * Leaves out the deployment default unless the region is Airbyte-managed.
   */
  fun listForDataplane(serviceAccountId: UUID): List<SecretStorage> {
    val dataplane =
      dataplaneRepository.findByServiceAccountId(serviceAccountId)
        ?: throw ResourceNotFoundProblem(ProblemResourceData().resourceType("Dataplane").resourceId(serviceAccountId.toString()))
    val dataplaneGroup =
      dataplaneGroupRepository.findById(dataplane.dataplaneGroupId).orElseThrow {
        ResourceNotFoundProblem(ProblemResourceData().resourceType("DataplaneGroup").resourceId(dataplane.dataplaneGroupId.toString()))
      }

    val secretStorages = listForDataplaneGroup(OrganizationId(dataplaneGroup.organizationId), dataplane.dataplaneGroupId)

    // A customer's own data plane has only the customer's credentials, so it can't open the deployment default.
    if (dataplaneGroup.organizationId == DEFAULT_ORGANIZATION_ID) {
      return secretStorages
    }
    return secretStorages.filterNot { it.id == DEFAULT_SECRET_STORAGE_ID }
  }

  /**
   * Returns the names of the given secret storages that belong to the organization or are the deployment default.
   */
  fun getNamesForOrganization(
    organizationId: OrganizationId,
    ids: Set<SecretStorageId>,
  ): Map<SecretStorageId, String> =
    ids
      .mapNotNull { secretStorageRepository.findById(it) }
      .filter { !it.tombstone && belongsTo(it, organizationId) }
      .associate { it.id to it.descriptor }

  private fun belongsTo(
    secretStorage: SecretStorage,
    organizationId: OrganizationId,
  ): Boolean =
    when {
      secretStorage.isDefault() -> true
      secretStorage.scopeType == SecretStorageScopeType.ORGANIZATION -> secretStorage.scopeId == organizationId.value
      else ->
        organizationRepository
          .getOrganizationForWorkspaceId(secretStorage.scopeId)
          .map { it.organizationId == organizationId.value }
          .orElse(false)
    }

  private fun listForDataplaneGroup(
    organizationId: OrganizationId,
    dataplaneGroupId: UUID,
  ): List<SecretStorage> {
    val workspaces = workspaceRepository.findByOrganizationIdAndDataplaneGroupIdAndTombstoneFalse(organizationId.value, dataplaneGroupId)
    if (workspaces.isEmpty()) {
      return listOf(getByOrganizationId(organizationId))
    }

    // No storage means the deployment default, except on the legacy runtime persistence, which has no storage ID.
    val fallback by lazy {
      if (featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId.value))) {
        null
      } else {
        secretStorageRepository.findById(DEFAULT_SECRET_STORAGE_ID)
      }
    }

    return workspaces
      .mapNotNull { getByWorkspaceId(WorkspaceId(checkNotNull(it.id))) ?: fallback }
      .distinctBy { it.id }
  }
}
