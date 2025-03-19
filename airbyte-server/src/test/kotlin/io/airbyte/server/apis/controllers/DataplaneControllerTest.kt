/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataplaneCreateRequestBody
import io.airbyte.api.model.generated.DataplaneDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneListRequestBody
import io.airbyte.api.model.generated.DataplaneUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.DataplaneNameAlreadyExistsProblem
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Dataplane
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import io.airbyte.data.services.DataplaneService as DataDataplaneService
import io.airbyte.server.services.DataplaneService as ServerDataplaneService

class DataplaneControllerTest {
  companion object {
    private val dataDataplaneService = mockk<DataDataplaneService>()
    private val serverDataplaneService = mockk<ServerDataplaneService>()
    private val currentUserService = mockk<CurrentUserService>()
    private val dataplaneController = DataplaneController(dataDataplaneService, serverDataplaneService, currentUserService)
    private val MOCK_DATAPLANE_GROUP_ID = UUID.randomUUID()
    private const val DATAPLANE_NAME_CONSTRAINT_VIOLATION_MESSAGE =
      "duplicate key value violates unique constraint: dataplane_dataplane_group_id_name_key"
  }

  @Test
  fun `writeDataplane with a duplicate name returns a problem`() {
    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataDataplaneService.writeDataplane(any()) } throws DataAccessException(DATAPLANE_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    val dataplaneCreateRequestBody = DataplaneCreateRequestBody().dataplaneGroupId(MOCK_DATAPLANE_GROUP_ID)

    assertThrows<DataplaneNameAlreadyExistsProblem> {
      dataplaneController.createDataplane(dataplaneCreateRequestBody)
    }
  }

  @Test
  fun `updateDataplane returns the dataplane`() {
    val mockDataplane = createDataplane()
    val newName = "new name"
    val newEnabled = true

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataDataplaneService.getDataplane(any()) } returns mockDataplane
    every { dataDataplaneService.writeDataplane(any()) } returns
      mockDataplane.apply {
        name = newName
        enabled = newEnabled
      }
    val updatedDataplane =
      dataplaneController.updateDataplane(
        DataplaneUpdateRequestBody()
          .dataplaneId(mockDataplane.id)
          .name(newName)
          .enabled(newEnabled),
      )

    assert(updatedDataplane.dataplaneId == mockDataplane.id)
    assert(updatedDataplane.name == newName)
    assert(updatedDataplane.enabled == newEnabled)
  }

  @Test
  fun `updateDataplane with a duplicate name returns a problem`() {
    val mockDataplane = createDataplane()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataDataplaneService.getDataplane(any()) } returns mockDataplane
    every { dataDataplaneService.writeDataplane(any()) } throws DataAccessException(DATAPLANE_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    assertThrows<DataplaneNameAlreadyExistsProblem> {
      dataplaneController.updateDataplane(DataplaneUpdateRequestBody().dataplaneId(mockDataplane.id))
    }
  }

  @Test
  fun `deleteDataplane tombstones dataplane`() {
    val mockDataplane = createDataplane()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataDataplaneService.getDataplane(any()) } returns mockDataplane
    every { dataDataplaneService.writeDataplane(mockDataplane.apply { tombstone = true }) } returns
      mockDataplane.apply { tombstone = true }

    val dataplaneDeleteRequestBody =
      DataplaneDeleteRequestBody().dataplaneId(mockDataplane.id)

    dataplaneController.deleteDataplane(dataplaneDeleteRequestBody)

    verify { dataDataplaneService.writeDataplane(mockDataplane.apply { tombstone = true }) }
  }

  @Test
  fun `listDataplanes returns dataplanes`() {
    val dataplaneId1 = UUID.randomUUID()
    val dataplaneId2 = UUID.randomUUID()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataDataplaneService.listDataplanes(MOCK_DATAPLANE_GROUP_ID, false) } returns
      listOf(
        createDataplane(dataplaneId1),
        createDataplane(dataplaneId2),
      )
    val dataplanes = dataplaneController.listDataplanes(DataplaneListRequestBody().dataplaneGroupId(MOCK_DATAPLANE_GROUP_ID))
    val responseDataplanes = dataplanes.dataplanes

    assert(responseDataplanes.size == 2)
    assert(responseDataplanes[0].dataplaneId == dataplaneId1)
    assert(responseDataplanes[1].dataplaneId == dataplaneId2)
  }

  private fun createDataplane(id: UUID? = null): Dataplane =
    io.airbyte.data.repositories.entities
      .Dataplane(
        id = id ?: UUID.randomUUID(),
        dataplaneGroupId = MOCK_DATAPLANE_GROUP_ID,
        name = "Test Dataplane",
        enabled = false,
        updatedBy = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        tombstone = false,
      ).toConfigModel()
}
