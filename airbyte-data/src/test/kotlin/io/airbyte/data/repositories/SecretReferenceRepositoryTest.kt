/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.SecretConfig
import io.airbyte.data.repositories.entities.SecretReference
import io.airbyte.data.repositories.entities.SecretStorage
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretReferenceScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageType
import io.micronaut.data.exceptions.DataAccessException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class SecretReferenceRepositoryTest : AbstractConfigRepositoryTest() {
  val userId = UUID.randomUUID()

  @AfterEach
  fun tearDown() {
    secretReferenceRepository.deleteAll()
    secretConfigRepository.deleteAll()
    secretStorageRepository.deleteAll()
    organizationRepository.deleteAll()
  }

  @Test
  fun `test db insertion and retrieval`() {
    val organization =
      Organization(
        name = "Airbyte Inc.",
        email = "contact@airbyte.io",
      )
    val persistedOrg = organizationRepository.save(organization)

    val secretStorage =
      SecretStorage(
        scopeType = SecretStorageScopeType.organization,
        scopeId = persistedOrg.id!!,
        descriptor = "test",
        storageType = SecretStorageType.aws_secrets_manager,
        configuredFromEnvironment = true,
        createdBy = userId,
        updatedBy = userId,
      )
    val persistedSecretStorage = secretStorageRepository.save(secretStorage)

    val secretConfig =
      SecretConfig(
        secretStorageId = persistedSecretStorage.id!!,
        descriptor = "test",
        externalCoordinate = "some.coordinate",
        airbyteManaged = true,
        createdBy = userId,
        updatedBy = userId,
      )
    val persistedSecretConfig = secretConfigRepository.save(secretConfig)

    val secretReference =
      SecretReference(
        secretConfigId = persistedSecretConfig.id!!,
        scopeType = SecretReferenceScopeType.actor,
        scopeId = UUID.randomUUID(),
        hydrationPath = "$.foo.bar",
      )

    val countBeforeSave = secretReferenceRepository.count()
    assertEquals(0L, countBeforeSave)

    val persistedSecretReference = secretReferenceRepository.save(secretReference)

    val countAfterSave = secretReferenceRepository.count()
    assertEquals(1L, countAfterSave)

    val retrievedSecretReference = secretReferenceRepository.findById(persistedSecretReference.id!!).get()

    assertNotNull(retrievedSecretReference.createdAt)
    assertNotNull(retrievedSecretReference.updatedAt)
    assertThat(persistedSecretReference)
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(retrievedSecretReference)
  }

  @Test
  fun `test uniqueness constraint`() {
    val organization =
      Organization(
        name = "Airbyte Inc.",
        email = "contact@airbyte.io",
      )
    val persistedOrg = organizationRepository.save(organization)

    val secretStorage =
      SecretStorage(
        scopeType = SecretStorageScopeType.organization,
        scopeId = persistedOrg.id!!,
        descriptor = "test",
        storageType = SecretStorageType.aws_secrets_manager,
        configuredFromEnvironment = true,
        createdBy = userId,
        updatedBy = userId,
      )

    val persistedSecretStorage = secretStorageRepository.save(secretStorage)

    val secretConfig =
      SecretConfig(
        secretStorageId = persistedSecretStorage.id!!,
        descriptor = "test",
        externalCoordinate = "some.coordinate",
        airbyteManaged = true,
        createdBy = userId,
        updatedBy = userId,
      )

    val persistedSecretConfig = secretConfigRepository.save(secretConfig)

    val secretStorageId = UUID.randomUUID()
    val secretReference1 =
      SecretReference(
        secretConfigId = persistedSecretConfig.id!!,
        scopeType = SecretReferenceScopeType.secret_storage,
        scopeId = secretStorageId,
        hydrationPath = null,
      )

    secretReferenceRepository.save(secretReference1) // should not throw

    val secretReference2 =
      SecretReference(
        secretConfigId = persistedSecretConfig.id!!,
        scopeType = SecretReferenceScopeType.secret_storage,
        scopeId = secretStorageId,
        hydrationPath = "$.foo.bar",
      )

    secretReferenceRepository.save(secretReference2) // should not throw

    assertThrows<DataAccessException> {
      secretReferenceRepository.save(
        SecretReference(
          secretConfigId = persistedSecretConfig.id!!,
          scopeType = SecretReferenceScopeType.secret_storage,
          scopeId = secretStorageId,
          hydrationPath = "$.foo.bar",
        ),
      )
    }

    // also make sure NULL is treated as unique
    assertThrows<DataAccessException> {
      secretReferenceRepository.save(
        SecretReference(
          secretConfigId = persistedSecretConfig.id!!,
          scopeType = SecretReferenceScopeType.secret_storage,
          scopeId = secretStorageId,
          hydrationPath = null,
        ),
      )
    }
  }

  @Test
  fun `test listByScopeTypeAndScopeId`() {
    val organization =
      Organization(
        name = "Airbyte Inc.",
        email = "contact@airbyte.io",
      )
    val persistedOrg = organizationRepository.save(organization)

    val secretStorage =
      SecretStorage(
        scopeType = SecretStorageScopeType.organization,
        scopeId = persistedOrg.id!!,
        descriptor = "test",
        storageType = SecretStorageType.aws_secrets_manager,
        configuredFromEnvironment = true,
        createdBy = userId,
        updatedBy = userId,
      )
    val persistedSecretStorage = secretStorageRepository.save(secretStorage)

    val secretConfig =
      SecretConfig(
        secretStorageId = persistedSecretStorage.id!!,
        descriptor = "test",
        externalCoordinate = "some.coordinate",
        airbyteManaged = true,
        createdBy = userId,
        updatedBy = userId,
      )
    val persistedSecretConfig = secretConfigRepository.save(secretConfig)

    val actorId = UUID.randomUUID()
    val secretStorageId = UUID.randomUUID()

    val secretRef1 =
      SecretReference(
        secretConfigId = persistedSecretConfig.id!!,
        scopeType = SecretReferenceScopeType.actor,
        scopeId = actorId,
        hydrationPath = "$.foo.bar",
      )
    val persistedSecretRef1 = secretReferenceRepository.save(secretRef1)

    val secretRef2 =
      SecretReference(
        secretConfigId = persistedSecretConfig.id!!,
        scopeType = SecretReferenceScopeType.actor,
        scopeId = actorId,
        hydrationPath = "$.foo.baz",
      )
    val persistedSecretRef2 = secretReferenceRepository.save(secretRef2)

    val secretRef3 =
      SecretReference(
        secretConfigId = persistedSecretConfig.id!!,
        scopeType = SecretReferenceScopeType.secret_storage,
        scopeId = secretStorageId,
      )
    secretReferenceRepository.save(secretRef3) // persist a reference that should not be returned

    val actor1SecretRefs = secretReferenceRepository.listByScopeTypeAndScopeId(SecretReferenceScopeType.actor, actorId)

    assertEquals(2, actor1SecretRefs.size)
    assertNotNull(actor1SecretRefs.first().createdAt)
    assertNotNull(actor1SecretRefs.first().updatedAt)
    assertNotNull(actor1SecretRefs.last().createdAt)
    assertNotNull(actor1SecretRefs.last().updatedAt)
    assertThat(actor1SecretRefs.first())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(persistedSecretRef1)
    assertThat(actor1SecretRefs.last())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(persistedSecretRef2)
  }
}
