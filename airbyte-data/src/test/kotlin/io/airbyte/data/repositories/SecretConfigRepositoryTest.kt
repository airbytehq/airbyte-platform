/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.SecretConfig
import io.airbyte.data.repositories.entities.SecretStorage
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageType
import io.kotest.matchers.equality.shouldBeEqualToIgnoringFields
import io.kotest.matchers.nulls.shouldBeNull
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class SecretConfigRepositoryTest : AbstractConfigRepositoryTest() {
  val userId = UUID.randomUUID()

  private lateinit var persistedSecretStorage: SecretStorage

  @BeforeEach
  fun setup() {
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
    persistedSecretStorage = secretStorageRepository.save(secretStorage)
  }

  @AfterEach
  fun tearDown() {
    secretConfigRepository.deleteAll()
    secretStorageRepository.deleteAll()
    organizationRepository.deleteAll()
  }

  @Test
  fun `test db insertion and retrieval`() {
    val secretConfig =
      SecretConfig(
        secretStorageId = persistedSecretStorage.id!!,
        descriptor = "test",
        externalCoordinate = "some.coordinate",
        airbyteManaged = true,
        createdBy = userId,
        updatedBy = userId,
      )

    val countBeforeSave = secretConfigRepository.count()
    assertEquals(0L, countBeforeSave)

    val persistedSecretConfig = secretConfigRepository.save(secretConfig)

    val countAfterSave = secretConfigRepository.count()
    assertEquals(1L, countAfterSave)

    val retrievedSecretConfig = secretConfigRepository.findById(persistedSecretConfig.id).get()

    assertNotNull(retrievedSecretConfig.createdAt)
    assertNotNull(retrievedSecretConfig.updatedAt)
    assertThat(retrievedSecretConfig).usingRecursiveComparison().ignoringFields("createdAt", "updatedAt").isEqualTo(persistedSecretConfig)
  }

  @Test
  fun `test db insertion and retrieval with null users`() {
    val secretConfig =
      SecretConfig(
        secretStorageId = persistedSecretStorage.id!!,
        descriptor = "test",
        externalCoordinate = "some.coordinate",
        airbyteManaged = true,
        createdBy = null,
        updatedBy = null,
      )

    val countBeforeSave = secretConfigRepository.count()
    assertEquals(0L, countBeforeSave)

    val persistedSecretConfig = secretConfigRepository.save(secretConfig)

    val countAfterSave = secretConfigRepository.count()
    assertEquals(1L, countAfterSave)

    val retrievedSecretConfig = secretConfigRepository.findById(persistedSecretConfig.id).get()

    assertNotNull(retrievedSecretConfig.createdAt)
    assertNotNull(retrievedSecretConfig.updatedAt)
    assertThat(retrievedSecretConfig).usingRecursiveComparison().ignoringFields("createdAt", "updatedAt").isEqualTo(persistedSecretConfig)
  }

  @Nested
  inner class FindBySecretStorageIdAndExternalCoordinate {
    @Test
    fun `returns null when no record found`() {
      val externalCoordinate = "some.coordinate"

      secretConfigRepository.save(
        SecretConfig(
          secretStorageId = persistedSecretStorage.id!!,
          descriptor = "test",
          externalCoordinate = "some.other.coordinate",
          airbyteManaged = true,
          createdBy = userId,
          updatedBy = userId,
        ),
      )

      val result = secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(persistedSecretStorage.id!!, externalCoordinate)

      result.shouldBeNull()
    }

    @Test
    fun `returns matching secret config`() {
      val externalCoordinate = "some.coordinate"

      val persistedSecretConfig =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "test",
            externalCoordinate = externalCoordinate,
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      val result = secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(persistedSecretStorage.id!!, externalCoordinate)!!

      result.shouldBeEqualToIgnoringFields(persistedSecretConfig, SecretConfig::createdAt, SecretConfig::updatedAt)
    }
  }
}
