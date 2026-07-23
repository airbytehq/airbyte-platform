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
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.featureflag.CleanupDanglingSecretConfigs
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ReadSecretReferenceIdsInConfigs
import io.airbyte.featureflag.SecretStorage
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.data.services.SecretConfigService as SecretConfigRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository

private val logger = KotlinLogging.logger {}

// Matches the recovery window used by the OrphanedSecretConfigCleanup cron. In AWS this deprecates
// the secret and schedules deletion after the window; other stores delete immediately.
private const val SECRET_DELETION_RECOVERY_WINDOW_IN_DAYS = 7L

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
  private val metricClient: MetricClient,
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
    return SecretReferenceHelpers.updateSecretNodesWithSecretReferenceIds(
      config.originalConfig,
      createdSecretRefIdByPath,
    )
  }

  /**
   * Given an [actorConfig], create SecretConfig/SecretReference records for each secret
   * coordinate and replace them with their respective secret reference IDs in the returned config.
   *
   * Note: This method does not clean up dangling secret references from prior config versions.
   * Callers must call [cleanupDanglingSecretReferences] after the config has been successfully
   * persisted. This ordering is important because the reference creation and config persistence
   * use separate database transactions (Micronaut Data and Jooq respectively). Deleting old
   * references before the config write would cause orphaned reference IDs in the config if the
   * write fails, e.g. due to process termination during a Kubernetes pod rollout or crash.
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
   * Deletes any secret reference rows for the given scope whose hydration paths are not in the
   * provided config. This cleans up references from prior config versions, as well as any stale
   * references left behind by a previously interrupted save.
   *
   * Must be called after the config has been successfully persisted. If old references are
   * deleted before the config write and the write fails (e.g. process killed during a Kubernetes
   * pod rollout), the config will still contain reference IDs that no longer exist in the
   * database, causing 500 errors on read.
   *
   * See [createAndInsertSecretReferencesWithStorageId] for details.
   */
  fun cleanupDanglingSecretReferences(
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

  private fun warnOnOrphanedConfigReferenceIds(
    config: JsonNode,
    existingReferenceIds: Set<SecretReferenceId>,
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ) {
    val configReferenceIds = SecretReferenceHelpers.getSecretReferenceIdsFromConfig(config)
    for (id in configReferenceIds) {
      if (!existingReferenceIds.contains(SecretReferenceId(id))) {
        logger.warn { "Orphaned secret reference $id found in config for $scopeType $scopeId: reference row does not exist in DB" }
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
      warnOnOrphanedConfigReferenceIds(config, result.map { it.secretReference.id }.toSet(), scopeType, scopeId)
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

  /**
   * Returns the set of secret config IDs currently referenced by the given scope. Capture this
   * before mutating a scope's config/references so that [deleteOrphanedAirbyteManagedSecrets] can
   * later reclaim the configs that this operation leaves unreferenced.
   */
  fun getReferencedSecretConfigIds(
    scopeId: UUID,
    scopeType: SecretReferenceScopeType,
  ): Set<SecretConfigId> =
    secretReferenceRepository
      .listByScopeTypeAndScopeId(scopeType, scopeId)
      .map { it.secretConfigId }
      .toSet()

  /**
   * Deletes Airbyte-managed secrets that were previously referenced by a scope but are no longer
   * referenced after an update or deletion, both from the secret store and the secret_config table.
   *
   * This MUST be called only after the updated config has been durably persisted, for the same
   * crash-safety reason described on [cleanupDanglingSecretReferences]: if a secret is deleted from
   * the store before the config write and that write fails, the persisted config would point at a
   * deleted secret. On the happy path this reclaims orphaned secrets inline rather than waiting for
   * (or relying entirely on) the OrphanedSecretConfigCleanup cron, which remains the backstop for
   * partial failures. Gated per storage by [CleanupDanglingSecretConfigs], matching the cron.
   *
   * @param candidateSecretConfigIds configs referenced by the scope before the operation (see
   *   [getReferencedSecretConfigIds]); each is deleted only if no reference points at it anymore.
   * @param secretStorageId the storage the scope's secrets live in
   */
  fun deleteOrphanedAirbyteManagedSecrets(
    candidateSecretConfigIds: Collection<SecretConfigId>,
    secretStorageId: SecretStorageId,
  ) {
    if (candidateSecretConfigIds.isEmpty()) {
      return
    }
    if (!featureFlagClient.boolVariation(CleanupDanglingSecretConfigs, SecretStorage(secretStorageId.value.toString()))) {
      return
    }

    val secretPersistence = secretPersistenceService.getPersistenceByStorageId(secretStorageId)
    val configIdsToDelete = mutableListOf<SecretConfigId>()
    for (configId in candidateSecretConfigIds) {
      try {
        // Skip configs that are still referenced (e.g. re-pointed to themselves, or shared elsewhere).
        if (secretReferenceRepository.existsBySecretConfigId(configId)) {
          continue
        }
        val secretConfig = secretConfigRepository.findById(configId) ?: continue
        // Never delete externally/customer-managed secrets - we only own airbyte-managed ones.
        if (!secretConfig.airbyteManaged) {
          continue
        }
        val coordinate = AirbyteManagedSecretCoordinate.fromFullCoordinate(secretConfig.externalCoordinate)
        if (coordinate == null) {
          logger.warn { "Skipping inline deletion for invalid coordinate ${secretConfig.externalCoordinate} in storage $secretStorageId" }
          continue
        }
        secretPersistence.deleteWithRecoveryWindow(coordinate, SECRET_DELETION_RECOVERY_WINDOW_IN_DAYS)
        configIdsToDelete.add(configId)
        metricClient.count(
          metric = OssMetricsRegistry.DELETE_SECRET,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.SUCCESS, "true"),
              MetricAttribute(MetricTags.SECRET_STORAGE_ID, secretStorageId.value.toString()),
              MetricAttribute(MetricTags.SECRET_DELETION_TRIGGER, MetricTags.SECRET_DELETION_TRIGGER_INLINE),
            ),
        )
      } catch (e: Exception) {
        logger.error(e) { "Failed to inline-delete orphaned secret config $configId in storage $secretStorageId" }
        metricClient.count(
          metric = OssMetricsRegistry.DELETE_SECRET,
          attributes =
            arrayOf(
              MetricAttribute(MetricTags.SUCCESS, "false"),
              MetricAttribute(MetricTags.SECRET_STORAGE_ID, secretStorageId.value.toString()),
              MetricAttribute(MetricTags.SECRET_DELETION_TRIGGER, MetricTags.SECRET_DELETION_TRIGGER_INLINE),
            ),
        )
      }
    }
    if (configIdsToDelete.isNotEmpty()) {
      secretConfigRepository.deleteByIds(configIdsToDelete)
      logger.info { "Inline-deleted ${configIdsToDelete.size} orphaned airbyte-managed secret config(s) in storage $secretStorageId" }
    }
  }
}
