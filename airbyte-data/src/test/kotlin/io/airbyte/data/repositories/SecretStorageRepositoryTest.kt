/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.SecretStorage
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.util.UUID

class SecretStorageRepositoryTest : AbstractConfigRepositoryTest() {
  val userId = UUID.randomUUID()

  @AfterEach
  fun tearDown() {
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

    val countBeforeSave = secretStorageRepository.count()
    assertEquals(0L, countBeforeSave)

    val persistedSecretStorage = secretStorageRepository.save(secretStorage)

    val countAfterSave = secretStorageRepository.count()
    assertEquals(1L, countAfterSave)

    val retrievedSecretStorage = secretStorageRepository.findById(persistedSecretStorage.id!!).get()

    assertNotNull(retrievedSecretStorage.createdAt)
    assertNotNull(retrievedSecretStorage.updatedAt)
    assertThat(persistedSecretStorage)
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(retrievedSecretStorage)
  }

  @Test
  fun `test listByScopeTypeAndScopeId`() {
    val organization =
      Organization(
        name = "Airbyte Inc.",
        email = "contact@airbyte.io",
      )
    val persistedOrg = organizationRepository.save(organization)
    val persistedOtherOrg = organizationRepository.save(Organization(name = "Other", email = "other@airbyte.io"))

    val secretStorage1 =
      SecretStorage(
        scopeType = SecretStorageScopeType.organization,
        scopeId = persistedOrg.id!!,
        descriptor = "test1",
        storageType = SecretStorageType.aws_secrets_manager,
        configuredFromEnvironment = true,
        createdBy = userId,
        updatedBy = userId,
      )
    val secretStorage2 = secretStorage1.copy(descriptor = "test2")
    val secretStorage3 = secretStorage1.copy(scopeId = persistedOtherOrg.id!!, descriptor = "test3")
    secretStorageRepository.saveAll(listOf(secretStorage1, secretStorage2, secretStorage3))

    val secretStorages =
      secretStorageRepository.listByScopeTypeAndScopeId(
        scopeType = SecretStorageScopeType.organization,
        scopeId = persistedOrg.id!!,
      )
    assertEquals(2, secretStorages.size)
    assertNotNull(secretStorages.first().createdAt)
    assertNotNull(secretStorages.first().updatedAt)
    assertNotNull(secretStorages.first().id)
    assertNotNull(secretStorages.last().createdAt)
    assertNotNull(secretStorages.last().updatedAt)
    assertNotNull(secretStorages.last().id)
    assertThat(secretStorages.first()).usingRecursiveComparison().ignoringFields("id", "createdAt", "updatedAt").isEqualTo(secretStorage1)
    assertThat(secretStorages.last()).usingRecursiveComparison().ignoringFields("id", "createdAt", "updatedAt").isEqualTo(secretStorage2)
  }
}
