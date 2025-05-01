/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository

@Singleton
class SecretMigrationService(
  private val secretStorageService: SecretStorageService,
  private val secretReferenceRepository: SecretReferenceRepository,
  private val secretReferenceService: SecretReferenceService,
  private val secretPersistenceService: SecretPersistenceService,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  private val workspacePersistence: WorkspacePersistence,
  private val workspaceRepository: WorkspaceService,
  private val sourceRepository: SourceService,
  private val destinationRepository: DestinationService,
) {
  private val log = KotlinLogging.logger {}

  data class ActorToMigrate(
    val actorId: ActorId,
    val actorType: ActorType,
    val secretReferences: List<SecretReferenceWithConfig>,
    val sourceConnection: SourceConnection? = null,
    val destinationConnection: DestinationConnection? = null,
  )

  private fun getActorsToMigrateForWorkspace(
    workspaceId: WorkspaceId,
    secretStorageId: SecretStorageId,
  ): List<ActorToMigrate> {
    val sources = sourceRepository.listWorkspaceSourceConnection(workspaceId.value)
    val destinations = destinationRepository.listWorkspaceDestinationConnection(workspaceId.value)
    val actorIds = sources.map { it.sourceId } + destinations.map { it.destinationId }

    val refsByActorId =
      secretReferenceRepository
        .listWithConfigByScopeTypeAndScopeIds(SecretReferenceScopeType.ACTOR, actorIds)
        .filter { it.secretConfig.secretStorageId == secretStorageId.value }
        .groupBy { ActorId(it.secretReference.scopeId) }

    val configsByActorId =
      sources.associateBy({ ActorId(it.sourceId) }, { it.configuration }) +
        destinations.associateBy({ ActorId(it.destinationId) }, { it.configuration })

    return sources.mapNotNull {
      val actorId = ActorId(it.sourceId)
      val secretReferences = refsByActorId[actorId]
      val persistedConfig = configsByActorId[actorId]
      if (secretReferences != null && persistedConfig != null) {
        ActorToMigrate(
          actorId = actorId,
          actorType = ActorType.SOURCE,
          secretReferences = secretReferences,
          sourceConnection = it,
        )
      } else {
        null
      }
    } +
      destinations.mapNotNull {
        val actorId = ActorId(it.destinationId)
        val secretReferences = refsByActorId[actorId]
        val persistedConfig = configsByActorId[actorId]
        if (secretReferences != null && persistedConfig != null) {
          ActorToMigrate(
            actorId = actorId,
            actorType = ActorType.DESTINATION,
            secretReferences = secretReferences,
            destinationConnection = it,
          )
        } else {
          null
        }
      }
  }

  private fun migrateActorSecrets(
    actorToMigrate: ActorToMigrate,
    toSecretStorageId: SecretStorageId,
    fromPersistence: SecretPersistence,
    toPersistence: SecretPersistence,
  ) {
    val nonAirbyteManagedConfigs = actorToMigrate.secretReferences.filter { !it.secretConfig.airbyteManaged }
    if (nonAirbyteManagedConfigs.isNotEmpty()) {
      log.warn {
        "Will not migrate secret references for actor ${actorToMigrate.actorId.value} since it contains non-airbyteManaged secrets, which is not supported"
      }
      return
    }

    val newRefIdByPath = mutableMapOf<String, SecretReferenceId>()
    for (ref in actorToMigrate.secretReferences) {
      val oldReference = ref.secretReference
      val oldSecretConfig = ref.secretConfig

      val coordinate = SecretCoordinate.fromFullCoordinate(oldSecretConfig.externalCoordinate)
      val secretValue = secretsRepositoryReader.fetchSecretFromSecretPersistence(coordinate, fromPersistence).textValue()

      val managedCoordinate = coordinate as SecretCoordinate.AirbyteManagedSecretCoordinate
      val newCoordinate = secretsRepositoryWriter.store(managedCoordinate, secretValue, toPersistence)

      val newRefId =
        secretReferenceService.createSecretConfigAndReference(
          secretStorageId = toSecretStorageId,
          externalCoordinate = newCoordinate.fullCoordinate,
          airbyteManaged = oldSecretConfig.airbyteManaged,
          currentUserId = oldSecretConfig.createdBy?.let { UserId(it) },
          hydrationPath = oldReference.hydrationPath!!,
          scopeType = oldReference.scopeType,
          scopeId = oldReference.scopeId,
        )

      log.info { "Migrated secret reference from id ${ref.secretReference.id} to id $newRefId with coordinate ${newCoordinate.fullCoordinate}" }
      newRefIdByPath[oldReference.hydrationPath!!] = newRefId
    }

    val oldActorConfig =
      when (actorToMigrate.actorType) {
        ActorType.SOURCE -> actorToMigrate.sourceConnection!!.configuration
        ActorType.DESTINATION -> actorToMigrate.destinationConnection!!.configuration
      }

    val updatedActorConfig =
      SecretReferenceHelpers.updateSecretNodesWithSecretReferenceIds(
        oldActorConfig,
        newRefIdByPath,
      )

    when (actorToMigrate.actorType) {
      ActorType.SOURCE -> {
        val newSourceConnection = Jsons.clone(actorToMigrate.sourceConnection!!)
        newSourceConnection.configuration = updatedActorConfig.value
        sourceRepository.writeSourceConnectionNoSecrets(newSourceConnection)
      }
      ActorType.DESTINATION -> {
        val newDestinationConnection = Jsons.clone(actorToMigrate.destinationConnection!!)
        newDestinationConnection.configuration = updatedActorConfig.value
        destinationRepository.writeDestinationConnectionNoSecrets(newDestinationConnection)
      }
    }
    log.info { "Migrated ${newRefIdByPath.size} referenced secrets for actor config with id ${actorToMigrate.actorId}" }
  }

  private fun getOrgForSecretStorage(secretStorageId: SecretStorageId): OrganizationId {
    val secretStorage = secretStorageService.getById(secretStorageId)
    return when (secretStorage.scopeType) {
      SecretStorageScopeType.ORGANIZATION -> OrganizationId(secretStorage.scopeId)
      SecretStorageScopeType.WORKSPACE -> OrganizationId(workspaceRepository.getOrganizationIdFromWorkspaceId(secretStorage.scopeId).get())
    }
  }

  private fun validateSecretStorageOwnership(
    fromSecretStorageId: SecretStorageId,
    toSecretStorageId: SecretStorageId,
    organizationId: OrganizationId,
  ) {
    if (toSecretStorageId == SecretStorage.DEFAULT_SECRET_STORAGE_ID) {
      throw IllegalArgumentException("Cannot migrate secrets to the default secret storage")
    }

    if (fromSecretStorageId != SecretStorage.DEFAULT_SECRET_STORAGE_ID) {
      val fromStorageOrg = getOrgForSecretStorage(fromSecretStorageId)
      if (fromStorageOrg != organizationId) {
        throw IllegalArgumentException(
          "Cannot migrate secrets from secret storage $fromSecretStorageId since it is not owned by the requested scope (organizationId=$organizationId)",
        )
      }
    }

    val toStorageOrg = getOrgForSecretStorage(toSecretStorageId)
    if (toStorageOrg != organizationId) {
      throw IllegalArgumentException(
        "Cannot migrate secrets to secret storage $toSecretStorageId since it is not owned by the requested scope (organizationId=$organizationId)",
      )
    }
  }

  fun migrateSecrets(
    fromSecretStorageId: SecretStorageId,
    toSecretStorageId: SecretStorageId,
    scopeType: ScopeType,
    scopeId: UUID,
  ) {
    log.info {
      "Will migrate secret references for actors in $scopeType $scopeId from storageId=${fromSecretStorageId.value} to storageId=${toSecretStorageId.value}"
    }

    val organizationId =
      when (scopeType) {
        ScopeType.WORKSPACE -> OrganizationId(workspaceRepository.getOrganizationIdFromWorkspaceId(scopeId).get())
        ScopeType.ORGANIZATION -> OrganizationId(scopeId)
      }

    validateSecretStorageOwnership(fromSecretStorageId, toSecretStorageId, organizationId)

    val workspaceIds =
      when (scopeType) {
        ScopeType.WORKSPACE -> listOf(WorkspaceId(scopeId))
        ScopeType.ORGANIZATION ->
          workspacePersistence
            .listWorkspacesByOrganizationId(
              scopeId,
              false,
              Optional.empty(),
            ).map { WorkspaceId(it.workspaceId) }
      }

    for (workspaceId in workspaceIds) {
      val actorsToMigrate = getActorsToMigrateForWorkspace(workspaceId, fromSecretStorageId)
      if (actorsToMigrate.isEmpty()) {
        log.info { "No actors to migrate secrets for in workspace ${workspaceId.value}" }
        continue
      }

      val fromPersistence = secretPersistenceService.getPersistenceByStorageId(fromSecretStorageId)
      val toPersistence = secretPersistenceService.getPersistenceByStorageId(toSecretStorageId)
      log.info { "Migrating secrets for ${actorsToMigrate.size} actors in workspace ${workspaceId.value}" }
      for (actorToMigrate in actorsToMigrate) {
        migrateActorSecrets(actorToMigrate, toSecretStorageId, fromPersistence, toPersistence)
      }
    }
  }
}
