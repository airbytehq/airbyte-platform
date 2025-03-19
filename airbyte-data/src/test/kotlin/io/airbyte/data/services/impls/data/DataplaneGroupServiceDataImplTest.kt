/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

private val MOCK_ORGANIZATION_ID = UUID.randomUUID()

class DataplaneGroupServiceDataImplTest {
  private val dataplaneGroupRepository = mockk<DataplaneGroupRepository>()
  private var dataplaneGroupServiceDataImpl = DataplaneGroupServiceDataImpl(dataplaneGroupRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get dataplaneGroup by id`() {
    val dataplaneGroupId = UUID.randomUUID()
    val dataplaneGroup = createDataplaneGroup(dataplaneGroupId)

    every { dataplaneGroupRepository.findById(dataplaneGroupId) } returns Optional.of(dataplaneGroup)

    val retrievedDataplaneGroup = dataplaneGroupServiceDataImpl.getDataplaneGroup(dataplaneGroupId)
    assertEquals(dataplaneGroup.toConfigModel(), retrievedDataplaneGroup)

    verify { dataplaneGroupRepository.findById(dataplaneGroupId) }
  }

  @Test
  fun `test get dataplane group by non-existent id throws`() {
    every { dataplaneGroupRepository.findById(any()) } returns Optional.empty()

    assertThrows<ConfigNotFoundException> { dataplaneGroupServiceDataImpl.getDataplaneGroup(UUID.randomUUID()) }

    verify { dataplaneGroupRepository.findById(any()) }
  }

  @Test
  fun `test write new dataplane group`() {
    val dataplaneGroup = createDataplaneGroup(UUID.randomUUID())

    every { dataplaneGroupRepository.existsById(dataplaneGroup.id) } returns false
    every { dataplaneGroupRepository.save(any()) } returns dataplaneGroup

    val retrievedDataplaneGroup = dataplaneGroupServiceDataImpl.writeDataplaneGroup(dataplaneGroup.toConfigModel())
    assertEquals(dataplaneGroup.toConfigModel(), retrievedDataplaneGroup)

    verify { dataplaneGroupRepository.existsById(dataplaneGroup.id) }
    verify { dataplaneGroupRepository.save(any()) }
  }

  @Test
  fun `test update existing dataplane group`() {
    val dataplaneGroup = createDataplaneGroup(UUID.randomUUID())

    every { dataplaneGroupRepository.existsById(dataplaneGroup.id) } returns true
    every { dataplaneGroupRepository.update(any()) } returns dataplaneGroup

    val retrievedDataplaneGroup = dataplaneGroupServiceDataImpl.writeDataplaneGroup(dataplaneGroup.toConfigModel())
    assertEquals(dataplaneGroup.toConfigModel(), retrievedDataplaneGroup)

    verify { dataplaneGroupRepository.existsById(dataplaneGroup.id) }
    verify { dataplaneGroupRepository.update(any()) }
  }

  @Test
  fun `test list dataplane groups by organization id with tombstones`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockDataplaneGroupId1 = UUID.randomUUID()
    val mockDataplaneGroupId2 = UUID.randomUUID()
    every { dataplaneGroupRepository.findAllByOrganizationIdOrderByUpdatedAtDesc(mockOrganizationId) } returns
      listOf(
        createDataplaneGroup(mockDataplaneGroupId1),
        createDataplaneGroup(mockDataplaneGroupId2),
      )

    val retrievedDataplaneGroups = dataplaneGroupServiceDataImpl.listDataplaneGroups(mockOrganizationId, true)

    assertEquals(retrievedDataplaneGroups.size, 2)
  }

  @Test
  fun `test list dataplane groups by organization id without tombstones`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockDataplaneGroupId1 = UUID.randomUUID()
    val mockDataplaneGroupId2 = UUID.randomUUID()
    every { dataplaneGroupRepository.findAllByOrganizationIdAndTombstoneFalseOrderByUpdatedAtDesc(mockOrganizationId) } returns
      listOf(
        createDataplaneGroup(mockDataplaneGroupId1),
        createDataplaneGroup(mockDataplaneGroupId2),
      )

    val retrievedDataplaneGroups = dataplaneGroupServiceDataImpl.listDataplaneGroups(mockOrganizationId, false)

    assertEquals(retrievedDataplaneGroups.size, 2)
  }

  private fun createDataplaneGroup(id: UUID): DataplaneGroup =
    DataplaneGroup(
      id = id,
      organizationId = MOCK_ORGANIZATION_ID,
      name = "Test Dataplane Group",
      enabled = true,
      updatedBy = UUID.randomUUID(),
      tombstone = false,
      createdAt = OffsetDateTime.now(),
      updatedAt = OffsetDateTime.now(),
    )
}
