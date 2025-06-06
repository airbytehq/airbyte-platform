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
import java.time.OffsetDateTime
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
    secretReferenceRepository.deleteAll()
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

  @Nested
  inner class FindAirbyteManagedConfigsWithoutReferences {
    @Test
    fun `returns only airbyte-managed configs without references`() {
      // Create airbyte-managed config without reference
      val airbyteManagedConfigWithoutRef =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "airbyte-managed-without-ref",
            externalCoordinate = "airbyte.without.ref",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      // Create airbyte-managed config with reference
      val airbyteManagedConfigWithRef =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "airbyte-managed-with-ref",
            externalCoordinate = "airbyte.with.ref",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      // Create user-managed config without reference
      val userManagedConfigWithoutRef =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "user-managed-without-ref",
            externalCoordinate = "user.without.ref",
            airbyteManaged = false,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      // Create a secret reference for the airbyte-managed config
      secretReferenceRepository.save(
        SecretReference(
          secretConfigId = airbyteManagedConfigWithRef.id!!,
          scopeType = SecretReferenceScopeType.actor,
          scopeId = UUID.randomUUID(),
        ),
      )

      val result = secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(OffsetDateTime.now(), 1000)

      // Should only return the airbyte-managed config without reference
      assertEquals(1, result.size)
      assertEquals(airbyteManagedConfigWithoutRef.id, result[0].id)
    }

    @Test
    fun `excludes recently created configs when date filter is provided`() {
      val twoHoursAgo = OffsetDateTime.now().minusHours(2)
      val oneHourAgo = OffsetDateTime.now().minusHours(1)

      // Create an old airbyte-managed config without reference
      val oldConfig =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "old-config",
            externalCoordinate = "old.config",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      // Create a recent airbyte-managed config without reference
      val recentConfig =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "recent-config",
            externalCoordinate = "recent.config",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      // Manually update the created_at timestamps to simulate old vs recent creation
      // Update the old config to have been created 2 hours ago
      jooqDslContext
        .update(
          org.jooq.impl.DSL
            .table("secret_config"),
        ).set(
          org.jooq.impl.DSL
            .field("created_at"),
          twoHoursAgo,
        ).where(
          org.jooq.impl.DSL
            .field("id")
            .eq(oldConfig.id),
        ).execute()

      // Update the recent config to have been created 30 minutes ago (after oneHourAgo filter)
      jooqDslContext
        .update(
          org.jooq.impl.DSL
            .table("secret_config"),
        ).set(
          org.jooq.impl.DSL
            .field("created_at"),
          OffsetDateTime.now().minusMinutes(30),
        ).where(
          org.jooq.impl.DSL
            .field("id")
            .eq(recentConfig.id),
        ).execute()

      // Test without date filter - should return both
      val allResults = secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(OffsetDateTime.now(), 1000)
      assertEquals(2, allResults.size)

      // Test with date filter excluding configs created after oneHourAgo
      // This should only return the old config (created 2 hours ago)
      val filteredResults = secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(oneHourAgo, 1000)

      assertEquals(1, filteredResults.size)
      assertEquals(oldConfig.id, filteredResults[0].id)
      assertEquals("old-config", filteredResults[0].descriptor)

      // Test with even more restrictive filter - should return empty
      val veryRestrictiveResults =
        secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(
          OffsetDateTime.now().minusHours(3),
          1000,
        )
      assertEquals(0, veryRestrictiveResults.size)
    }

    @Test
    fun `returns empty list when no airbyte-managed configs without references exist`() {
      // Create only user-managed configs
      secretConfigRepository.save(
        SecretConfig(
          secretStorageId = persistedSecretStorage.id!!,
          descriptor = "user-managed",
          externalCoordinate = "user.managed",
          airbyteManaged = false,
          createdBy = userId,
          updatedBy = userId,
        ),
      )

      // Create airbyte-managed config but with a reference
      val configWithRef =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "airbyte-with-ref",
            externalCoordinate = "airbyte.with.ref",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      secretReferenceRepository.save(
        SecretReference(
          secretConfigId = configWithRef.id!!,
          scopeType = SecretReferenceScopeType.actor,
          scopeId = UUID.randomUUID(),
        ),
      )

      val result = secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(OffsetDateTime.now(), 1000)

      assertEquals(0, result.size)
    }
  }

  @Nested
  inner class DeleteByIdIn {
    @Test
    fun `deletes multiple secret configs by their IDs`() {
      // Create multiple secret configs
      val config1 =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "config-1",
            externalCoordinate = "config.1",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      val config2 =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "config-2",
            externalCoordinate = "config.2",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      val config3 =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "config-3",
            externalCoordinate = "config.3",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      // Verify all configs exist
      assertEquals(3, secretConfigRepository.count())

      // Delete two of them
      secretConfigRepository.deleteByIdIn(listOf(config1.id!!, config2.id!!))

      // Verify only one remains
      assertEquals(1, secretConfigRepository.count())

      // Verify the correct one remains
      val remaining = secretConfigRepository.findAll().first()
      assertEquals(config3.id, remaining.id)
      assertEquals("config-3", remaining.descriptor)
    }

    @Test
    fun `handles empty ID list gracefully`() {
      // Create a config to ensure the method doesn't delete everything
      secretConfigRepository.save(
        SecretConfig(
          secretStorageId = persistedSecretStorage.id!!,
          descriptor = "test-config",
          externalCoordinate = "test.config",
          airbyteManaged = true,
          createdBy = userId,
          updatedBy = userId,
        ),
      )

      assertEquals(1, secretConfigRepository.count())

      // Call with empty list - should not delete anything
      secretConfigRepository.deleteByIdIn(emptyList())

      // Verify config still exists
      assertEquals(1, secretConfigRepository.count())
    }

    @Test
    fun `handles non-existent IDs gracefully`() {
      // Create a config
      val config =
        secretConfigRepository.save(
          SecretConfig(
            secretStorageId = persistedSecretStorage.id!!,
            descriptor = "test-config",
            externalCoordinate = "test.config",
            airbyteManaged = true,
            createdBy = userId,
            updatedBy = userId,
          ),
        )

      assertEquals(1, secretConfigRepository.count())

      // Try to delete using non-existent ID
      secretConfigRepository.deleteByIdIn(listOf(UUID.randomUUID(), UUID.randomUUID()))

      // Verify config still exists
      assertEquals(1, secretConfigRepository.count())
      assertEquals(config.id, secretConfigRepository.findAll().first().id)
    }
  }
}
