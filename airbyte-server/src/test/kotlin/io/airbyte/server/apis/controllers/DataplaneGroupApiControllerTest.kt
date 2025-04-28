/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataplaneGroupCreateRequestBody
import io.airbyte.api.model.generated.DataplaneGroupDeleteRequestBody
import io.airbyte.api.model.generated.DataplaneGroupListRequestBody
import io.airbyte.api.model.generated.DataplaneGroupUpdateRequestBody
import io.airbyte.api.problems.throwable.generated.DataplaneGroupNameAlreadyExistsProblem
import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.server.services.DataplaneService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class DataplaneGroupApiControllerTest {
  companion object {
    private val dataplaneGroupService = mockk<DataplaneGroupService>()
    private val dataplaneService = mockk<DataplaneService>()
    private val dataplaneGroupApiController = DataplaneGroupApiController(dataplaneGroupService, dataplaneService)
    private val MOCK_ORGANIZATION_ID = UUID.randomUUID()
    private const val DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE =
      "duplicate key value violates unique constraint: dataplane_group_organization_id_name_key"
  }

  @Test
  fun `createDataplaneGroup returns the dataplane group`() {
    every { dataplaneGroupService.writeDataplaneGroup(any()) } returns createDataplaneGroup()
    every { dataplaneService.listDataplanes(any()) } returns emptyList()

    val dataplaneGroup = dataplaneGroupApiController.createDataplaneGroup(DataplaneGroupCreateRequestBody().organizationId(MOCK_ORGANIZATION_ID))
    val responseDataplaneGroups = dataplaneGroup!!

    assert(responseDataplaneGroups.organizationId == MOCK_ORGANIZATION_ID)
  }

  @Test
  fun `createDataplaneGroup with a duplicate name returns a problem`() {
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

    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } returns
      mockDataplaneGroup.apply {
        name = newName
        enabled = newEnabled
      }
    every { dataplaneService.listDataplanes(any()) } returns emptyList()

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

    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } throws DataAccessException(DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    assertThrows<DataplaneGroupNameAlreadyExistsProblem> {
      dataplaneGroupApiController.updateDataplaneGroup(DataplaneGroupUpdateRequestBody().dataplaneGroupId(mockDataplaneGroup.id))
    }
  }

  @Test
  fun `updateDataplaneGroup with only name set should preserve enabled`() {
    val originalEnabled = true
    val newName = "patched group name"
    val mockDataplaneGroup =
      createDataplaneGroup().apply {
        enabled = originalEnabled
      }

    every { dataplaneGroupService.getDataplaneGroup(mockDataplaneGroup.id) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } answers { firstArg() }
    every { dataplaneService.listDataplanes(any()) } returns emptyList()

    val updated =
      dataplaneGroupApiController.updateDataplaneGroup(
        DataplaneGroupUpdateRequestBody()
          .dataplaneGroupId(mockDataplaneGroup.id)
          .name(newName),
      )

    assert(updated!!.name == newName)
    assert(updated.enabled == originalEnabled)
  }

  @Test
  fun `updateDataplaneGroup with only enabled set should preserve name`() {
    val originalName = "Original Group"
    val newEnabled = true
    val mockDataplaneGroup =
      createDataplaneGroup().apply {
        name = originalName
      }

    every { dataplaneGroupService.getDataplaneGroup(mockDataplaneGroup.id) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } answers { firstArg() }
    every { dataplaneService.listDataplanes(any()) } returns emptyList()

    val updated =
      dataplaneGroupApiController.updateDataplaneGroup(
        DataplaneGroupUpdateRequestBody()
          .dataplaneGroupId(mockDataplaneGroup.id)
          .enabled(newEnabled),
      )

    assert(updated!!.enabled == newEnabled)
    assert(updated.name == originalName)
  }

  @Test
  fun `deleteDataplaneGroup tombstones dataplane group `() {
    val mockDataplaneGroup = createDataplaneGroup()
    val mockDataplane =
      Dataplane().apply {
        id = UUID.randomUUID()
        dataplaneGroupId = mockDataplaneGroup.id
        name = "name"
        enabled = true
        createdAt = OffsetDateTime.now().toEpochSecond()
        updatedAt = OffsetDateTime.now().toEpochSecond()
      }

    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(mockDataplaneGroup.apply { tombstone = true }) } returns
      mockDataplaneGroup.apply { tombstone = true }
    every { dataplaneService.listDataplanes(any()) } returns listOf(mockDataplane)
    every { dataplaneService.deleteDataplane(any()) } returns mockDataplane

    val dataplaneGroupDeleteRequestBody =
      DataplaneGroupDeleteRequestBody().dataplaneGroupId(mockDataplaneGroup.id)

    dataplaneGroupApiController.deleteDataplaneGroup(dataplaneGroupDeleteRequestBody)

    verify {
      dataplaneGroupService.writeDataplaneGroup(mockDataplaneGroup.apply { tombstone = true })
      dataplaneService.deleteDataplane(any())
    }
  }

  @Test
  fun `listDataplaneGroups returns dataplane groups`() {
    val dataplaneGroupId1 = UUID.randomUUID()
    val dataplaneGroupId2 = UUID.randomUUID()

    every { dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, MOCK_ORGANIZATION_ID), any()) } returns
      listOf(
        createDataplaneGroup(dataplaneGroupId1),
        createDataplaneGroup(dataplaneGroupId2),
      )
    every { dataplaneService.listDataplanes(any()) } returns emptyList()

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
        tombstone = false,
      ).toConfigModel()
}
