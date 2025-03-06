/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.repositories.entities.Dataplane
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

private val MOCK_DATAPLANE_GROUP_ID = UUID.randomUUID()

class DataplaneServiceDataImplTest {
  private val dataplaneRepository = mockk<DataplaneRepository>()
  private var dataplaneServiceDataImpl = DataplaneServiceDataImpl(dataplaneRepository)

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @Test
  fun `test get dataplane by id`() {
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)

    every { dataplaneRepository.findById(dataplaneId) } returns Optional.of(dataplane)

    val retrievedDataplane = dataplaneServiceDataImpl.getDataplane(dataplaneId)
    assertEquals(dataplane.toConfigModel(), retrievedDataplane)

    verify { dataplaneRepository.findById(dataplaneId) }
  }

  @Test
  fun `test get dataplane by non-existent id throws`() {
    every { dataplaneRepository.findById(any()) } returns Optional.empty()

    assertThrows<ConfigNotFoundException> { dataplaneServiceDataImpl.getDataplane(UUID.randomUUID()) }

    verify { dataplaneRepository.findById(any()) }
  }

  @Test
  fun `test write new dataplane`() {
    val dataplane = createDataplane(UUID.randomUUID())

    every { dataplaneRepository.existsById(dataplane.id) } returns false
    every { dataplaneRepository.save(any()) } returns dataplane

    val retrievedDataplane = dataplaneServiceDataImpl.writeDataplane(dataplane.toConfigModel())
    assertEquals(dataplane.toConfigModel(), retrievedDataplane)

    verify { dataplaneRepository.existsById(dataplane.id) }
    verify { dataplaneRepository.save(any()) }
  }

  @Test
  fun `test update existing dataplane`() {
    val dataplane = createDataplane(UUID.randomUUID())

    every { dataplaneRepository.existsById(dataplane.id) } returns true
    every { dataplaneRepository.update(any()) } returns dataplane

    val retrievedDataplane = dataplaneServiceDataImpl.writeDataplane(dataplane.toConfigModel())
    assertEquals(dataplane.toConfigModel(), retrievedDataplane)

    verify { dataplaneRepository.existsById(dataplane.id) }
    verify { dataplaneRepository.update(any()) }
  }

  @Test
  fun `test list dataplanes by dataplane group id with tombstones`() {
    val mockDataplaneGroupId = UUID.randomUUID()
    val mockDataplaneId1 = UUID.randomUUID()
    val mockDataplaneId2 = UUID.randomUUID()
    every { dataplaneRepository.findAllByDataplaneGroupIdOrderByUpdatedAtDesc(mockDataplaneGroupId) } returns
      listOf(
        createDataplane(mockDataplaneId1),
        createDataplane(mockDataplaneId2),
      )

    val retrievedDataplanes = dataplaneServiceDataImpl.listDataplanes(mockDataplaneGroupId, true)

    assertEquals(retrievedDataplanes.size, 2)
  }

  @Test
  fun `test list dataplanes by dataplane group id without tombstones`() {
    val mockDataplaneGroupId = UUID.randomUUID()
    val mockDataplaneId1 = UUID.randomUUID()
    val mockDataplaneId2 = UUID.randomUUID()
    every { dataplaneRepository.findAllByDataplaneGroupIdAndTombstoneFalseOrderByUpdatedAtDesc(mockDataplaneGroupId) } returns
      listOf(
        createDataplane(mockDataplaneId1),
        createDataplane(mockDataplaneId2),
      )

    val retrievedDataplanes = dataplaneServiceDataImpl.listDataplanes(mockDataplaneGroupId, false)

    assertEquals(retrievedDataplanes.size, 2)
  }

  private fun createDataplane(id: UUID): Dataplane =
    Dataplane(
      id = id,
      dataplaneGroupId = MOCK_DATAPLANE_GROUP_ID,
      name = "Test Dataplane ",
      enabled = true,
      updatedBy = UUID.randomUUID(),
      tombstone = false,
      createdAt = OffsetDateTime.now(),
      updatedAt = OffsetDateTime.now(),
    )
}
