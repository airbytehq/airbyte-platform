/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.problems.throwable.generated.DataplaneNameAlreadyExistsProblem
import io.airbyte.config.Dataplane
import io.airbyte.data.services.DataplaneTokenService
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.jooq.exception.DataAccessException
import org.junit.Assert.assertEquals
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class DataplaneServiceTest {
  private lateinit var dataplaneDataService: io.airbyte.data.services.DataplaneService
  private lateinit var dataplaneService: DataplaneService
  private lateinit var serviceAccountsService: ServiceAccountsService
  private lateinit var dataplaneTokenService: DataplaneTokenService

  private val dataplaneGroupId = UUID.randomUUID()
  private val dataplaneNameConstraintViolationMessage =
    "duplicate key value violates unique constraint: dataplane_dataplane_group_id_name_key"

  @BeforeEach
  fun setup() {
    // Setup all fallbacks/base cases
    dataplaneDataService = mockk()
    serviceAccountsService = mockk()
    dataplaneTokenService = mockk()
    dataplaneService =
      DataplaneService(
        dataplaneDataService,
        serviceAccountsService,
        dataplaneTokenService,
      )
  }

  @Test
  fun `writeDataplane with a duplicate name returns a problem`() {
    every { dataplaneDataService.updateDataplane(any()) } throws DataAccessException(dataplaneNameConstraintViolationMessage)

    assertThrows<DataplaneNameAlreadyExistsProblem> {
      dataplaneService.writeDataplane(createDataplane(UUID.randomUUID()))
    }
  }

  @Test
  fun `updateDataplane returns the dataplane`() {
    val mockDataplane = createDataplane()
    val newName = "new name"
    val newEnabled = true

    every { dataplaneDataService.getDataplane(any()) } returns
      mockDataplane.apply {
        name = newName
        enabled = newEnabled
      }
    every {
      dataplaneDataService.updateDataplane(
        mockDataplane.apply {
          name = newName
          enabled = newEnabled
        },
      )
    } returns
      mockDataplane.apply {
        name = newName
        enabled = newEnabled
      }

    val updatedDataplane =
      dataplaneService.updateDataplane(
        mockDataplane.id,
        newName,
        newEnabled,
      )

    assert(updatedDataplane.id == mockDataplane.id)
    assert(updatedDataplane.name == newName)
    assert(updatedDataplane.enabled == newEnabled)
  }

  @Test
  fun `updateDataplane with a duplicate name returns a problem`() {
    val mockDataplane = createDataplane()

    every { dataplaneDataService.getDataplane(any()) } returns mockDataplane
    every { dataplaneDataService.updateDataplane(any()) } throws DataAccessException(dataplaneNameConstraintViolationMessage)

    assertThrows<DataplaneNameAlreadyExistsProblem> {
      dataplaneService.updateDataplane(mockDataplane.id, "", true)
    }
  }

  @Test
  fun `updateDataplane with only name set should preserve enabled`() {
    val originalName = "original name"
    val originalEnabled = true
    val newName = "patched name"
    val mockDataplane =
      createDataplane().apply {
        name = originalName
        enabled = originalEnabled
      }

    every { dataplaneDataService.getDataplane(mockDataplane.id) } returns mockDataplane
    every { dataplaneDataService.updateDataplane(any()) } answers { firstArg() }

    val updated =
      dataplaneService.updateDataplane(
        dataplaneId = mockDataplane.id,
        updatedName = newName,
        updatedEnabled = null,
      )

    Assertions.assertEquals(newName, updated.name)
    Assertions.assertEquals(originalEnabled, updated.enabled)
  }

  @Test
  fun `updateDataplane with only enabled set should preserve name`() {
    val originalName = "original name"
    val originalEnabled = false
    val newEnabled = true
    val mockDataplane =
      createDataplane().apply {
        name = originalName
        enabled = originalEnabled
      }

    every { dataplaneDataService.getDataplane(mockDataplane.id) } returns mockDataplane
    every { dataplaneDataService.updateDataplane(any()) } answers { firstArg() }

    val updated =
      dataplaneService.updateDataplane(
        dataplaneId = mockDataplane.id,
        updatedName = null,
        updatedEnabled = newEnabled,
      )

    assertEquals(originalName, updated.name)
    assertEquals(newEnabled, updated.enabled)
  }

  @Test
  fun `deleteDataplane tombstones dataplane and deletes its credentials`() {
    val mockDataplane = createDataplane()

    every { dataplaneDataService.getDataplane(any()) } returns mockDataplane
    every { dataplaneDataService.listDataplanes(any(), false) } returns listOf(mockDataplane)
    every { serviceAccountsService.delete(mockDataplane.id) } just Runs
    every { dataplaneDataService.updateDataplane(mockDataplane.apply { tombstone = true }) } returns
      mockDataplane.apply { tombstone = true }

    dataplaneService.deleteDataplane(mockDataplane.id)

    verify {
      serviceAccountsService.delete(mockDataplane.id)
      dataplaneDataService.updateDataplane(mockDataplane.apply { tombstone = true })
    }
  }

  private fun createDataplane(id: UUID? = null): Dataplane =
    io.airbyte.data.repositories.entities
      .Dataplane(
        id = id ?: UUID.randomUUID(),
        dataplaneGroupId = dataplaneGroupId,
        name = "Test Dataplane",
        enabled = false,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        tombstone = false,
      ).toConfigModel()
}
