/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.micronaut.runtime.AirbyteDataplaneGroupsConfig
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
import io.airbyte.config.DataplaneGroup as ConfigDataplaneGroup

private val MOCK_ORGANIZATION_ID = UUID.randomUUID()
private val MOCK_NAME = "test"

class DataplaneGroupServiceDataImplTest {
  private val dataplaneGroupRepository = mockk<DataplaneGroupRepository>()
  private val airbyteDataplaneGroupsConfig = AirbyteDataplaneGroupsConfig()
  private var dataplaneGroupServiceDataImpl = DataplaneGroupServiceDataImpl(dataplaneGroupRepository, airbyteDataplaneGroupsConfig)

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
    every { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) } returns
      emptyList()

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
    every { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) } returns
      emptyList()

    val retrievedDataplaneGroup = dataplaneGroupServiceDataImpl.writeDataplaneGroup(dataplaneGroup.toConfigModel())
    assertEquals(dataplaneGroup.toConfigModel(), retrievedDataplaneGroup)

    verify { dataplaneGroupRepository.existsById(dataplaneGroup.id) }
    verify { dataplaneGroupRepository.update(any()) }
  }

  @Test
  fun `test get dataplane group by organization id and name`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockName = MOCK_NAME
    val mockDataplaneGroupId = UUID.randomUUID()

    val dataplaneGroup =
      DataplaneGroup(
        id = mockDataplaneGroupId,
        organizationId = mockOrganizationId,
        name = mockName,
        enabled = true,
        tombstone = false,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    every { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(mockOrganizationId, mockName) } returns listOf(dataplaneGroup)

    val retrievedDataplaneGroup =
      dataplaneGroupServiceDataImpl.getDataplaneGroupByOrganizationIdAndName(
        mockOrganizationId,
        mockName,
      )
    assertEquals(dataplaneGroup.toConfigModel(), retrievedDataplaneGroup)

    verify { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(mockOrganizationId, mockName) }
  }

  @Test
  fun `test get dataplane group by organization id and name throws when not found`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockName = MOCK_NAME

    every { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(mockOrganizationId, mockName) } returns emptyList()
    every { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(DEFAULT_ORGANIZATION_ID, mockName) } returns emptyList()

    assertThrows<NoSuchElementException> {
      dataplaneGroupServiceDataImpl.getDataplaneGroupByOrganizationIdAndName(mockOrganizationId, mockName)
    }

    verify { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(mockOrganizationId, mockName) }
    verify { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(DEFAULT_ORGANIZATION_ID, mockName) }
  }

  @Test
  fun `test get dataplane group by organization id and name falls back to default org when not found`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockDataplaneGroupId = UUID.randomUUID()
    val mockName = MOCK_NAME

    val dataplaneGroup =
      DataplaneGroup(
        id = mockDataplaneGroupId,
        organizationId = DEFAULT_ORGANIZATION_ID,
        name = mockName,
        enabled = true,
        tombstone = false,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    every { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(mockOrganizationId, mockName) } returns emptyList()
    every { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(DEFAULT_ORGANIZATION_ID, mockName) } returns listOf(dataplaneGroup)

    val retrievedDataplaneGroup = dataplaneGroupServiceDataImpl.getDataplaneGroupByOrganizationIdAndName(mockOrganizationId, mockName)
    assertEquals(dataplaneGroup.toConfigModel(), retrievedDataplaneGroup)

    verify { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(mockOrganizationId, mockName) }
    verify { dataplaneGroupRepository.findAllByOrganizationIdAndNameIgnoreCase(DEFAULT_ORGANIZATION_ID, mockName) }
  }

  @Test
  fun `test list dataplane groups by organization id with tombstones`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockDataplaneGroupId1 = UUID.randomUUID()
    val mockDataplaneGroupId2 = UUID.randomUUID()
    every { dataplaneGroupRepository.findAllByOrganizationIdInOrderByUpdatedAtDesc(listOf(mockOrganizationId)) } returns
      listOf(
        createDataplaneGroup(mockDataplaneGroupId1),
        createDataplaneGroup(mockDataplaneGroupId2),
      )

    val retrievedDataplaneGroups = dataplaneGroupServiceDataImpl.listDataplaneGroups(listOf(mockOrganizationId), true)

    assertEquals(retrievedDataplaneGroups.size, 2)
  }

  @Test
  fun `test list dataplane groups by organization id without tombstones`() {
    val mockOrganizationId = UUID.randomUUID()
    val mockDataplaneGroupId1 = UUID.randomUUID()
    val mockDataplaneGroupId2 = UUID.randomUUID()
    every { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(mockOrganizationId)) } returns
      listOf(
        createDataplaneGroup(mockDataplaneGroupId1),
        createDataplaneGroup(mockDataplaneGroupId2),
      )

    val retrievedDataplaneGroups = dataplaneGroupServiceDataImpl.listDataplaneGroups(listOf(mockOrganizationId), false)

    assertEquals(retrievedDataplaneGroups.size, 2)
  }

  @Test
  fun `validateDataplaneGroupName throws if name conflicts with default org`() {
    val conflictingName = MOCK_NAME
    val dataplaneGroup =
      DataplaneGroup(
        organizationId = UUID.randomUUID(), // not default
        name = conflictingName,
        enabled = true,
        tombstone = false,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    val defaultGroup =
      DataplaneGroup(
        id = UUID.randomUUID(),
        organizationId = DEFAULT_ORGANIZATION_ID,
        name = conflictingName,
        enabled = true,
        tombstone = false,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )

    every { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) } returns
      listOf(defaultGroup)

    assertThrows<RuntimeException> {
      dataplaneGroupServiceDataImpl.validateDataplaneGroupName(dataplaneGroup.toConfigModel())
    }
  }

  @Test
  fun `test list default dataplane groups`() {
    val defaultDataplaneGroupId1 = UUID.randomUUID()
    val defaultDataplaneGroupId2 = UUID.randomUUID()

    val defaultDataplaneGroups =
      listOf(
        DataplaneGroup(
          id = defaultDataplaneGroupId1,
          organizationId = DEFAULT_ORGANIZATION_ID,
          name = "Default Group 1",
          enabled = true,
          tombstone = false,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
        ),
        DataplaneGroup(
          id = defaultDataplaneGroupId2,
          organizationId = DEFAULT_ORGANIZATION_ID,
          name = "Default Group 2",
          enabled = true,
          tombstone = false,
          createdAt = OffsetDateTime.now(),
          updatedAt = OffsetDateTime.now(),
        ),
      )

    every { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) } returns
      defaultDataplaneGroups

    val retrievedDataplaneGroups = dataplaneGroupServiceDataImpl.listDefaultDataplaneGroups()

    assertEquals(2, retrievedDataplaneGroups.size)
    assertEquals(defaultDataplaneGroups.map { it.toConfigModel() }, retrievedDataplaneGroups)

    verify { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) }
  }

  @Test
  fun `test list default dataplane groups returns empty list when none exist`() {
    every { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) } returns
      emptyList()

    val retrievedDataplaneGroups = dataplaneGroupServiceDataImpl.listDefaultDataplaneGroups()

    assertEquals(0, retrievedDataplaneGroups.size)
    assertEquals(emptyList<ConfigDataplaneGroup>(), retrievedDataplaneGroups)

    verify { dataplaneGroupRepository.findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(listOf(DEFAULT_ORGANIZATION_ID)) }
  }

  private fun createDataplaneGroup(id: UUID): DataplaneGroup =
    DataplaneGroup(
      id = id,
      organizationId = MOCK_ORGANIZATION_ID,
      name = "Test Dataplane Group",
      enabled = true,
      tombstone = false,
      createdAt = OffsetDateTime.now(),
      updatedAt = OffsetDateTime.now(),
    )
}
