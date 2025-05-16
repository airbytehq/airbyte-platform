/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretStorageRepository
import io.airbyte.data.services.impls.data.mappers.SecretStorageMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretStorageMapper.toEntity
import io.airbyte.domain.models.PatchField
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.SecretStorageCreate
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.SecretStorageScopeType
import io.airbyte.domain.models.SecretStorageType
import io.airbyte.domain.models.UserId
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifySequence
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class SecretStorageServiceDataImplTest {
  private val secretStorageRepository = mockk<SecretStorageRepository>()
  private var service = SecretStorageServiceDataImpl(secretStorageRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  private fun createDomain(
    id: UUID = UUID.randomUUID(),
    scopeType: SecretStorageScopeType = SecretStorageScopeType.WORKSPACE,
    scopeId: UUID = UUID.randomUUID(),
    descriptor: String = "desc",
    storageType: SecretStorageType = SecretStorageType.LOCAL_TESTING,
    configuredFromEnvironment: Boolean = false,
    tombstone: Boolean = false,
    createdBy: UUID = UUID.randomUUID(),
    updatedBy: UUID = UUID.randomUUID(),
    createdAt: OffsetDateTime = OffsetDateTime.now(),
    updatedAt: OffsetDateTime = OffsetDateTime.now(),
  ): SecretStorage =
    SecretStorage(
      id = SecretStorageId(id),
      scopeType = scopeType,
      scopeId = scopeId,
      descriptor = descriptor,
      storageType = storageType,
      configuredFromEnvironment = configuredFromEnvironment,
      tombstone = tombstone,
      createdBy = createdBy,
      updatedBy = updatedBy,
      createdAt = createdAt,
      updatedAt = updatedAt,
    )

  @Test
  fun `test findById returns domain model when found`() {
    val domain = createDomain()
    val entity = domain.toEntity()
    every { secretStorageRepository.findById(domain.id.value) } returns Optional.of(entity)

    val result = service.findById(domain.id)

    assertEquals(domain, result)
    verify { secretStorageRepository.findById(domain.id.value) }
  }

  @Test
  fun `test findById returns null when not found`() {
    every { secretStorageRepository.findById(any()) } returns Optional.empty()

    val result = service.findById(SecretStorageId(UUID.randomUUID()))

    assertNull(result)
    verify { secretStorageRepository.findById(any()) }
  }

  @Test
  fun `test listByScopeTypeAndScopeId maps entities to domain`() {
    val domain1 = createDomain(scopeType = SecretStorageScopeType.ORGANIZATION)
    val domain2 = createDomain(scopeType = SecretStorageScopeType.ORGANIZATION)
    val entity1 = domain1.toEntity()
    val entity2 = domain2.toEntity()

    every {
      secretStorageRepository.listByScopeTypeAndScopeId(
        domain1.scopeType.toEntity(),
        domain1.scopeId,
      )
    } returns listOf(entity1, entity2)

    val result = service.listByScopeTypeAndScopeId(domain1.scopeType, domain1.scopeId)

    assertEquals(listOf(domain1, domain2), result)
    verify {
      secretStorageRepository.listByScopeTypeAndScopeId(
        domain1.scopeType.toEntity(),
        domain1.scopeId,
      )
    }
  }

  @Test
  fun `test patch throws when target not found`() {
    every { secretStorageRepository.findById(any()) } returns Optional.empty()
    val storageId = SecretStorageId(UUID.randomUUID())

    assertThrows<IllegalArgumentException> {
      service.patch(
        id = storageId,
        updatedBy = UserId(UUID.randomUUID()),
      )
    }
    verify { secretStorageRepository.findById(storageId.value) }
  }

  @Test
  fun `test patch applies changes and saves`() {
    // Existing record
    val originalDomain = createDomain(descriptor = "old-desc")
    val originalEntity = originalDomain.toEntity()
    every { secretStorageRepository.findById(originalDomain.id.value) } returns Optional.of(originalEntity)

    // Fields to patch: only change descriptor
    val newDesc = "new-desc"
    val byUser = UserId(UUID.randomUUID())

    // Expected domain after patch
    val expectedDomain =
      originalDomain.copy(
        descriptor = newDesc,
        updatedBy = byUser.value,
      )
    val expectedEntity = expectedDomain.toEntity()

    every { secretStorageRepository.update(expectedEntity) } returns expectedEntity

    val result =
      service.patch(
        id = originalDomain.id,
        updatedBy = byUser,
        descriptor = PatchField.Present(newDesc),
      )

    assertEquals(expectedDomain, result)
    verifySequence {
      secretStorageRepository.findById(originalDomain.id.value)
      secretStorageRepository.update(expectedEntity)
    }
  }

  @Test
  fun `test create saves and returns new secret storage`() {
    val byUser = UserId(UUID.randomUUID())
    val createDto =
      SecretStorageCreate(
        scopeType = SecretStorageScopeType.WORKSPACE,
        scopeId = UUID.randomUUID(),
        descriptor = "create-desc",
        storageType = SecretStorageType.VAULT,
        configuredFromEnvironment = true,
        createdBy = byUser,
      )

    val inputEntity = createDto.toEntity()

    val generatedId = UUID.randomUUID()
    val savedEntity = inputEntity.copy(id = generatedId)

    every { secretStorageRepository.save(any()) } returns savedEntity

    val expectedDomain = savedEntity.toConfigModel()

    val result = service.create(createDto)

    assertEquals(expectedDomain, result)
    verify { secretStorageRepository.save(any()) }
  }
}
