/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretConfigRepository
import io.airbyte.data.services.impls.data.mappers.SecretConfigMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretConfigMapper.toEntity
import io.airbyte.domain.models.SecretConfig
import io.airbyte.domain.models.SecretConfigCreate
import io.airbyte.domain.models.SecretConfigId
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.data.repositories.entities.SecretConfig as EntitySecretConfig

class SecretConfigServiceDataImplTest {
  private val secretConfigRepository = mockk<SecretConfigRepository>()
  private var service = SecretConfigServiceDataImpl(secretConfigRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  private fun createEntityModel(
    id: UUID = UUID.randomUUID(),
    secretStorageId: UUID = UUID.randomUUID(),
    descriptor: String = "test-descriptor",
    externalCoordinate: String = "test.coordinate",
    tombstone: Boolean = false,
    airbyteManaged: Boolean = true,
    createdBy: UUID? = UUID.randomUUID(),
    updatedBy: UUID? = UUID.randomUUID(),
    createdAt: OffsetDateTime = OffsetDateTime.now(),
    updatedAt: OffsetDateTime = OffsetDateTime.now(),
  ): EntitySecretConfig =
    EntitySecretConfig(
      id = id,
      secretStorageId = secretStorageId,
      descriptor = descriptor,
      externalCoordinate = externalCoordinate,
      tombstone = tombstone,
      airbyteManaged = airbyteManaged,
      createdBy = createdBy,
      updatedBy = updatedBy,
      createdAt = createdAt,
      updatedAt = updatedAt,
    )

  @Test
  fun `test findById returns domain model when found`() {
    val entity = createEntityModel()
    val expectedDomain = entity.toConfigModel()

    every { secretConfigRepository.findById(entity.id!!) } returns Optional.of(entity)

    val result = service.findById(SecretConfigId(entity.id!!))

    assertEquals(expectedDomain, result)
    verify { secretConfigRepository.findById(entity.id) }
  }

  @Test
  fun `test findById returns null when not found`() {
    every { secretConfigRepository.findById(any()) } returns Optional.empty()

    val result = service.findById(SecretConfigId(UUID.randomUUID()))

    assertNull(result)
    verify { secretConfigRepository.findById(any()) }
  }

  @Test
  fun `test findByStorageIdAndExternalCoordinate returns domain model when found`() {
    val storageId = SecretStorageId(UUID.randomUUID())
    val coordinate = "test.coordinate"
    val entity =
      createEntityModel(
        secretStorageId = storageId.value,
        externalCoordinate = coordinate,
      )
    val expectedDomain = entity.toConfigModel()

    every {
      secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(storageId.value, coordinate)
    } returns entity

    val result = service.findByStorageIdAndExternalCoordinate(storageId, coordinate)

    assertEquals(expectedDomain, result)
    verify { secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(storageId.value, coordinate) }
  }

  @Test
  fun `test findByStorageIdAndExternalCoordinate returns null when not found`() {
    val storageId = SecretStorageId(UUID.randomUUID())
    val coordinate = "test.coordinate"

    every {
      secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(storageId.value, coordinate)
    } returns null

    val result = service.findByStorageIdAndExternalCoordinate(storageId, coordinate)

    assertNull(result)
    verify { secretConfigRepository.findBySecretStorageIdAndExternalCoordinate(storageId.value, coordinate) }
  }

  @Test
  fun `test findAirbyteManagedConfigsWithoutReferences without date filter`() {
    val entity1 = createEntityModel(descriptor = "config1", airbyteManaged = true)
    val entity2 = createEntityModel(descriptor = "config2", airbyteManaged = true)
    val expectedDomains = listOf(entity1.toConfigModel(), entity2.toConfigModel())
    val timeFilter = OffsetDateTime.now()

    every { secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(timeFilter, 1000) } returns listOf(entity1, entity2)

    val result = service.findAirbyteManagedConfigsWithoutReferences(timeFilter, 1000)

    assertEquals(expectedDomains, result)
    verify { secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(timeFilter, 1000) }
  }

  @Test
  fun `test findAirbyteManagedConfigsWithoutReferences with date filter`() {
    val dateFilter = OffsetDateTime.now().minusHours(1)
    val entity1 = createEntityModel(descriptor = "old-config", airbyteManaged = true)
    val expectedDomains = listOf(entity1.toConfigModel())

    every { secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(dateFilter, 500) } returns listOf(entity1)

    val result = service.findAirbyteManagedConfigsWithoutReferences(dateFilter, 500)

    assertEquals(expectedDomains, result)
    verify { secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(dateFilter, 500) }
  }

  @Test
  fun `test findAirbyteManagedConfigsWithoutReferences returns empty list when none found`() {
    val timeFilter = OffsetDateTime.now()

    every { secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(timeFilter, 1000) } returns emptyList()

    val result = service.findAirbyteManagedConfigsWithoutReferences(timeFilter, 1000)

    assertEquals(emptyList<SecretConfig>(), result)
    verify { secretConfigRepository.findAirbyteManagedConfigsWithoutReferences(timeFilter, 1000) }
  }

  @Test
  fun `test create saves and returns new secret config`() {
    val createdBy = UserId(UUID.randomUUID())
    val createDto =
      SecretConfigCreate(
        secretStorageId = SecretStorageId(UUID.randomUUID()),
        descriptor = "new-config",
        externalCoordinate = "new.coordinate",
        airbyteManaged = true,
        createdBy = createdBy,
      )

    val inputEntity = createDto.toEntity()
    val generatedId = UUID.randomUUID()
    val savedEntity = inputEntity.copy(id = generatedId)
    val expectedDomain = savedEntity.toConfigModel()

    every { secretConfigRepository.save(any()) } returns savedEntity

    val result = service.create(createDto)

    assertEquals(expectedDomain, result)
    verify { secretConfigRepository.save(any()) }
  }

  @Test
  fun `test deleteByIds calls repository with converted UUIDs`() {
    val id1 = SecretConfigId(UUID.randomUUID())
    val id2 = SecretConfigId(UUID.randomUUID())
    val id3 = SecretConfigId(UUID.randomUUID())
    val ids = listOf(id1, id2, id3)

    every { secretConfigRepository.deleteByIdIn(any()) } returns Unit

    service.deleteByIds(ids)

    verify { secretConfigRepository.deleteByIdIn(listOf(id1.value, id2.value, id3.value)) }
  }

  @Test
  fun `test deleteByIds does not call repository when list is empty`() {
    every { secretConfigRepository.deleteByIdIn(any()) } returns Unit

    service.deleteByIds(emptyList())

    verify(exactly = 0) { secretConfigRepository.deleteByIdIn(any()) }
  }
}
