/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationConnection
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.persistence.WorkspacePersistence
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.UserId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.DestinationService as DestinationRepository
import io.airbyte.data.services.SecretReferenceService as SecretReferenceRepository
import io.airbyte.data.services.SourceService as SourceRepository
import io.airbyte.data.services.WorkspaceService as WorkspaceRepository

class SecretMigrationServiceTest {
  private val secretStorageService = mockk<SecretStorageService>()
  private val secretReferenceService = mockk<SecretReferenceService>()
  private val secretReferenceRepository = mockk<SecretReferenceRepository>()
  private val secretPersistenceService = mockk<SecretPersistenceService>()
  private val secretsRepositoryReader = mockk<SecretsRepositoryReader>()
  private val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>()
  private val workspacePersistence = mockk<WorkspacePersistence>()
  private val workspaceRepository = mockk<WorkspaceRepository>()
  private val sourceRepository = mockk<SourceRepository>(relaxed = true)
  private val destinationRepository = mockk<DestinationRepository>(relaxed = true)

  private val service =
    SecretMigrationService(
      secretStorageService,
      secretReferenceRepository,
      secretReferenceService,
      secretPersistenceService,
      secretsRepositoryReader,
      secretsRepositoryWriter,
      workspacePersistence,
      workspaceRepository,
      sourceRepository,
      destinationRepository,
    )

  private fun generateConfigWithRef(refId: SecretReferenceId) =
    Jsons.jsonNode(
      mapOf(
        "password" to
          mapOf(
            "_secret_reference_id" to refId.value.toString(),
          ),
      ),
    )

  @Test
  fun `test cannot migrate to default storage`() {
    assertThrows<IllegalArgumentException> {
      service.migrateSecrets(
        SecretStorageId(UUID.randomUUID()),
        SecretStorage.DEFAULT_SECRET_STORAGE_ID,
        ScopeType.ORGANIZATION,
        UUID.randomUUID(),
      )
    }
  }

  @Test
  fun `test must migrate from storage within same org`() {
    val scopedOrgId = UUID.randomUUID()

    val fromSecretStorage = mockk<SecretStorage>()
    val toSecretStorage = mockk<SecretStorage>()
    every { fromSecretStorage.scopeType } returns SecretStorageScopeType.ORGANIZATION
    every { fromSecretStorage.scopeId } returns UUID.randomUUID()
    every { toSecretStorage.scopeType } returns SecretStorageScopeType.ORGANIZATION
    every { toSecretStorage.scopeId } returns scopedOrgId

    every {
      secretStorageService.getById(any())
    } returns fromSecretStorage andThen toSecretStorage

    assertThrows<IllegalArgumentException> {
      service.migrateSecrets(
        SecretStorageId(UUID.randomUUID()),
        SecretStorageId(UUID.randomUUID()),
        ScopeType.ORGANIZATION,
        scopedOrgId,
      )
    }
  }

  @Test
  fun `test must migrate to storage within same org`() {
    val scopedOrgId = UUID.randomUUID()

    val fromSecretStorage = mockk<SecretStorage>()
    val toSecretStorage = mockk<SecretStorage>()
    every { fromSecretStorage.scopeType } returns SecretStorageScopeType.ORGANIZATION
    every { fromSecretStorage.scopeId } returns scopedOrgId
    every { toSecretStorage.scopeType } returns SecretStorageScopeType.ORGANIZATION
    every { toSecretStorage.scopeId } returns UUID.randomUUID()

    every {
      secretStorageService.getById(any())
    } returns fromSecretStorage andThen toSecretStorage

    assertThrows<IllegalArgumentException> {
      service.migrateSecrets(
        SecretStorageId(UUID.randomUUID()),
        SecretStorageId(UUID.randomUUID()),
        ScopeType.ORGANIZATION,
        scopedOrgId,
      )
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testMigrateSecrets(fromDefaultStorage: Boolean) {
    val scopeType = ScopeType.ORGANIZATION
    val organizationId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()

    every {
      workspacePersistence.listWorkspacesByOrganizationId(organizationId, false, Optional.empty())
    } returns listOf(StandardWorkspace().withWorkspaceId(workspaceId))

    val sourceSecretRefId = SecretReferenceId(UUID.randomUUID())
    val destinationSecretRefId = SecretReferenceId(UUID.randomUUID())
    val source =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withConfiguration(generateConfigWithRef(sourceSecretRefId))
    val sourceFromDifferentStorage =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withConfiguration(Jsons.emptyObject())
    val destination =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withConfiguration(generateConfigWithRef(destinationSecretRefId))

    every {
      sourceRepository.listWorkspaceSourceConnection(workspaceId)
    } returns listOf(source, sourceFromDifferentStorage)

    every {
      destinationRepository.listWorkspaceDestinationConnection(workspaceId)
    } returns listOf(destination)

    val fromSecretStorageId =
      if (fromDefaultStorage) {
        SecretStorage.DEFAULT_SECRET_STORAGE_ID
      } else {
        SecretStorageId(UUID.randomUUID())
      }
    val toSecretStorageId = SecretStorageId(UUID.randomUUID())

    val sourceSecretConfig =
      SecretConfig(
        id = SecretConfigId(UUID.randomUUID()),
        secretStorageId = fromSecretStorageId.value,
        descriptor = "my source secret",
        externalCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate().fullCoordinate,
        airbyteManaged = true,
        createdBy = UUID.randomUUID(),
        updatedBy = UUID.randomUUID(),
        createdAt = null,
        updatedAt = null,
      )
    val sourceSecretRef =
      SecretReferenceWithConfig(
        secretReference =
          SecretReference(
            id = sourceSecretRefId,
            secretConfigId = sourceSecretConfig.id,
            scopeType = SecretReferenceScopeType.ACTOR,
            scopeId = source.sourceId,
            hydrationPath = "$.password",
            createdAt = null,
            updatedAt = null,
          ),
        secretConfig = sourceSecretConfig,
      )

    // Refs from other storages should not be migrated
    val foreignSecretConfig = mockk<SecretConfig>()
    every {
      foreignSecretConfig.secretStorageId
    } returns UUID.randomUUID()
    val sourceSecretRefFromDifferentStorage =
      SecretReferenceWithConfig(
        secretReference = mockk<SecretReference>(),
        secretConfig = foreignSecretConfig,
      )

    val destinationSecretConfig =
      SecretConfig(
        id = SecretConfigId(UUID.randomUUID()),
        secretStorageId = fromSecretStorageId.value,
        descriptor = "my destination secret",
        externalCoordinate = SecretCoordinate.AirbyteManagedSecretCoordinate().fullCoordinate,
        airbyteManaged = true,
        createdBy = UUID.randomUUID(),
        updatedBy = UUID.randomUUID(),
        createdAt = null,
        updatedAt = null,
      )
    val destinationSecretRef =
      SecretReferenceWithConfig(
        secretReference =
          SecretReference(
            id = destinationSecretRefId,
            secretConfigId = destinationSecretConfig.id,
            scopeType = SecretReferenceScopeType.ACTOR,
            scopeId = destination.destinationId,
            hydrationPath = "$.password",
            createdAt = null,
            updatedAt = null,
          ),
        secretConfig = destinationSecretConfig,
      )

    every {
      secretReferenceRepository.listWithConfigByScopeTypeAndScopeIds(
        SecretReferenceScopeType.ACTOR,
        listOf(source.sourceId, sourceFromDifferentStorage.sourceId, destination.destinationId),
      )
    } returns listOf(sourceSecretRef, sourceSecretRefFromDifferentStorage, destinationSecretRef)

    val fromPersistence = mockk<SecretPersistence>()
    val toPersistence = mockk<SecretPersistence>()

    val orgStorage = mockk<SecretStorage>()
    every {
      orgStorage.scopeType
    } returns SecretStorageScopeType.ORGANIZATION
    every {
      orgStorage.scopeId
    } returns organizationId

    every {
      secretStorageService.getById(fromSecretStorageId)
    } returns orgStorage
    every {
      secretStorageService.getById(toSecretStorageId)
    } returns orgStorage

    every {
      secretPersistenceService.getPersistenceByStorageId(fromSecretStorageId)
    } returns fromPersistence

    every {
      secretPersistenceService.getPersistenceByStorageId(toSecretStorageId)
    } returns toPersistence

    every {
      secretsRepositoryReader.fetchSecretFromSecretPersistence(
        SecretCoordinate.fromFullCoordinate(sourceSecretConfig.externalCoordinate),
        fromPersistence,
      )
    } returns "hunter1"

    every {
      secretsRepositoryReader.fetchSecretFromSecretPersistence(
        SecretCoordinate.fromFullCoordinate(destinationSecretConfig.externalCoordinate),
        fromPersistence,
      )
    } returns "hunter2"

    every {
      secretsRepositoryWriter.store(
        any(),
        any(),
        toPersistence,
      )
    } answers { firstArg() }

    val migratedSourceRefId = SecretReferenceId(UUID.randomUUID())
    every {
      secretReferenceService.createSecretConfigAndReference(
        toSecretStorageId,
        sourceSecretConfig.externalCoordinate,
        sourceSecretConfig.airbyteManaged,
        UserId(sourceSecretConfig.createdBy!!),
        sourceSecretRef.secretReference.hydrationPath!!,
        SecretReferenceScopeType.ACTOR,
        source.sourceId,
      )
    } returns migratedSourceRefId

    val migratedDestinationRefId = SecretReferenceId(UUID.randomUUID())
    every {
      secretReferenceService.createSecretConfigAndReference(
        toSecretStorageId,
        destinationSecretConfig.externalCoordinate,
        destinationSecretConfig.airbyteManaged,
        UserId(destinationSecretConfig.createdBy!!),
        destinationSecretRef.secretReference.hydrationPath!!,
        SecretReferenceScopeType.ACTOR,
        destination.destinationId,
      )
    } returns migratedDestinationRefId

    // Run the secrets migration for the org
    service.migrateSecrets(
      fromSecretStorageId,
      toSecretStorageId,
      scopeType,
      organizationId,
    )

    // Ensure that secrets were migrated and actors updated
    verify {
      secretsRepositoryReader.fetchSecretFromSecretPersistence(
        SecretCoordinate.fromFullCoordinate(sourceSecretConfig.externalCoordinate),
        fromPersistence,
      )
      secretsRepositoryWriter.store(
        SecretCoordinate.AirbyteManagedSecretCoordinate.fromFullCoordinate(sourceSecretConfig.externalCoordinate)!!,
        "hunter1",
        toPersistence,
      )
      secretReferenceService.createSecretConfigAndReference(
        toSecretStorageId,
        sourceSecretConfig.externalCoordinate,
        sourceSecretConfig.airbyteManaged,
        UserId(sourceSecretConfig.createdBy!!),
        sourceSecretRef.secretReference.hydrationPath!!,
        SecretReferenceScopeType.ACTOR,
        source.sourceId,
      )
      sourceRepository.writeSourceConnectionNoSecrets(
        source.withConfiguration(generateConfigWithRef(migratedSourceRefId)),
      )

      secretsRepositoryReader.fetchSecretFromSecretPersistence(
        SecretCoordinate.fromFullCoordinate(destinationSecretConfig.externalCoordinate),
        fromPersistence,
      )
      secretsRepositoryWriter.store(
        SecretCoordinate.AirbyteManagedSecretCoordinate.fromFullCoordinate(destinationSecretConfig.externalCoordinate)!!,
        "hunter2",
        toPersistence,
      )
      secretReferenceService.createSecretConfigAndReference(
        toSecretStorageId,
        destinationSecretConfig.externalCoordinate,
        destinationSecretConfig.airbyteManaged,
        UserId(destinationSecretConfig.createdBy!!),
        destinationSecretRef.secretReference.hydrationPath!!,
        SecretReferenceScopeType.ACTOR,
        destination.destinationId,
      )
      destinationRepository.writeDestinationConnectionNoSecrets(
        destination.withConfiguration(generateConfigWithRef(migratedDestinationRefId)),
      )
    }
  }
}
