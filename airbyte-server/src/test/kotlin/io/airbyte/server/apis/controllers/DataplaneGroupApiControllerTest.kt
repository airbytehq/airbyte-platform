/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataplaneGroupCreateRequestBody
import io.airbyte.api.model.generated.DataplaneGroupDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.model.generated.DataplaneGroupUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.DataplaneGroupNameAlreadyExistsProblem
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
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

class DataplaneGroupApiControllerTest {
  companion object {
    private val dataplaneGroupService = mockk<DataplaneGroupService>()
    private val currentUserService = mockk<CurrentUserService>()
    private val dataplaneGroupApiController = DataplaneGroupApiController(dataplaneGroupService, currentUserService)
    private val MOCK_ORGANIZATION_ID = UUID.randomUUID()
    private const val DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE =
      "duplicate key value violates unique constraint: dataplane_group_organization_id_name_key"
  }

  @Test
  fun `writeDataplaneGroup returns the dataplane group`() {
    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneGroupService.writeDataplaneGroup(any()) } returns createDataplaneGroup()

    val dataplaneGroup = dataplaneGroupApiController.createDataplaneGroup(DataplaneGroupCreateRequestBody().organizationId(MOCK_ORGANIZATION_ID))
    val responseDataplaneGroups = dataplaneGroup!!

    assert(responseDataplaneGroups.organizationId == MOCK_ORGANIZATION_ID)
  }

  @Test
  fun `writeDataplaneGroup with a duplicate name returns a problem`() {
    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneGroupService.writeDataplaneGroup(any()) } throws DataAccessException(DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    val dataplaneGroupCreateRequestBody = DataplaneGroupCreateRequestBody().organizationId(MOCK_ORGANIZATION_ID)

    assertThrows<DataplaneGroupNameAlreadyExistsProblem> {
      dataplaneGroupApiController.createDataplaneGroup(dataplaneGroupCreateRequestBody)
    }
  }

  @Test
  fun `updateDataplaneGroup returns the dataplaneGroup`() {
    val mockDataplaneGroup = createDataplaneGroup()
    val newName = "new name"
    val newEnabled = true

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } returns
      mockDataplaneGroup.apply {
        name = newName
        enabled = newEnabled
      }

    val updatedDataplaneGroup =
      dataplaneGroupApiController.updateDataplaneGroup(
        DataplaneGroupUpdateRequestBody()
          .dataplaneGroupId(mockDataplaneGroup.id)
          .name(newName)
          .enabled(newEnabled),
      )
    val responseDataplaneGroups = updatedDataplaneGroup!!

    assert(responseDataplaneGroups.dataplaneGroupId == mockDataplaneGroup.id)
    assert(responseDataplaneGroups.name == newName)
    assert(responseDataplaneGroups.enabled == newEnabled)
  }

  @Test
  fun `updateDataplaneGroup with a duplicate name returns a problem`() {
    val mockDataplaneGroup = createDataplaneGroup()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } throws DataAccessException(DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    assertThrows<DataplaneGroupNameAlreadyExistsProblem> {
      dataplaneGroupApiController.updateDataplaneGroup(DataplaneGroupUpdateRequestBody().dataplaneGroupId(mockDataplaneGroup.id))
    }
  }

  @Test
  fun `deleteDataplaneGroup tombstones dataplane group `() {
    val mockDataplaneGroup = createDataplaneGroup()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(mockDataplaneGroup.apply { tombstone = true }) } returns
      mockDataplaneGroup.apply { tombstone = true }

    val dataplaneGroupDeleteRequestBody =
      DataplaneGroupDeleteRequestBody().dataplaneGroupId(mockDataplaneGroup.id)

    dataplaneGroupApiController.deleteDataplaneGroup(dataplaneGroupDeleteRequestBody)

    verify { dataplaneGroupService.writeDataplaneGroup(mockDataplaneGroup.apply { tombstone = true }) }
  }

  @Test
  fun `listDataplaneGroups returns dataplane groups`() {
    val dataplaneGroupId1 = UUID.randomUUID()
    val dataplaneGroupId2 = UUID.randomUUID()

    every { currentUserService.currentUserIdIfExists } returns Optional.of(UUID.randomUUID())
    every { dataplaneGroupService.listDataplaneGroups(MOCK_ORGANIZATION_ID, any()) } returns
      listOf(
        createDataplaneGroup(dataplaneGroupId1),
        createDataplaneGroup(dataplaneGroupId2),
      )
    val dataplaneGroups = dataplaneGroupApiController.listDataplaneGroups(DataplaneGroupListRequestBody().organizationId(MOCK_ORGANIZATION_ID))

    val responseDataplaneGroups = dataplaneGroups?.dataplaneGroups!!
    assert(responseDataplaneGroups.size == 2)
    assert(responseDataplaneGroups[0].dataplaneGroupId == dataplaneGroupId1)
    assert(responseDataplaneGroups[1].dataplaneGroupId == dataplaneGroupId2)
  }

  private fun createDataplaneGroup(id: UUID? = null): DataplaneGroup =
    io.airbyte.data.repositories.entities
      .DataplaneGroup(
        id = id ?: UUID.randomUUID(),
        organizationId = MOCK_ORGANIZATION_ID,
        name = "Test Dataplane Group",
        enabled = false,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        updatedBy = UUID.randomUUID(),
        tombstone = false,
      ).toConfigModel()
}
