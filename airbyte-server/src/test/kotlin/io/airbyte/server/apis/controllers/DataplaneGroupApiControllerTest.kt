/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.DataplaneGroupNameAlreadyExistsProblem
import io.airbyte.api.server.generated.models.DataplaneGroupCreateRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupDeleteRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupListRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupUpdateRequestBody
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.domain.models.OrganizationId
import io.airbyte.server.services.DataplaneService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class DataplaneGroupApiControllerTest {
  companion object {
    private val dataplaneGroupService = mockk<DataplaneGroupService>()
    private val dataplaneService = mockk<DataplaneService>()
    private val entitlementService = mockk<EntitlementService>(relaxed = true)
    private val dataplaneGroupApiController = DataplaneGroupApiController(dataplaneGroupService, dataplaneService, entitlementService)
    private val MOCK_ORGANIZATION_ID = UUID.randomUUID()
    private const val DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE =
      "duplicate key value violates unique constraint: dataplane_group_organization_id_name_key"
  }

  @Test
  fun `createDataplaneGroup returns the dataplane group`() {
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.writeDataplaneGroup(any()) } returns createDataplaneGroup()
    every { dataplaneService.listDataplanes(any<UUID>()) } returns emptyList()

    val dataplaneGroup =
      dataplaneGroupApiController.createDataplaneGroup(
        DataplaneGroupCreateRequestBody(organizationId = MOCK_ORGANIZATION_ID, name = "test", enabled = true),
      )
    val responseDataplaneGroups = dataplaneGroup

    assertEquals(MOCK_ORGANIZATION_ID, responseDataplaneGroups.organizationId)
  }

  @Test
  fun `createDataplaneGroup with a duplicate name returns a problem`() {
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.writeDataplaneGroup(any()) } throws DataAccessException(DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    val dataplaneGroupCreateRequestBody = DataplaneGroupCreateRequestBody(organizationId = MOCK_ORGANIZATION_ID, name = "test", enabled = true)

    assertThrows<DataplaneGroupNameAlreadyExistsProblem> {
      dataplaneGroupApiController.createDataplaneGroup(dataplaneGroupCreateRequestBody)
    }
  }

  @Test
  fun `updateDataplaneGroup returns the dataplaneGroup`() {
    val mockDataplaneGroup = createDataplaneGroup()
    val newName = "new name"
    val newEnabled = true

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplaneGroup.id) } returns MOCK_ORGANIZATION_ID
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } returns
      mockDataplaneGroup.apply {
        name = newName
        enabled = newEnabled
      }
    every { dataplaneService.listDataplanes(any<UUID>()) } returns emptyList()

    val updatedDataplaneGroup =
      dataplaneGroupApiController.updateDataplaneGroup(
        DataplaneGroupUpdateRequestBody(
          dataplaneGroupId = mockDataplaneGroup.id,
          name = newName,
          enabled = newEnabled,
        ),
      )
    val responseDataplaneGroups = updatedDataplaneGroup

    assertEquals(mockDataplaneGroup.id, responseDataplaneGroups.dataplaneGroupId)
    assertEquals(newName, responseDataplaneGroups.name)
    assertEquals(newEnabled, responseDataplaneGroups.enabled)
  }

  @Test
  fun `updateDataplaneGroup with a duplicate name returns a problem`() {
    val mockDataplaneGroup = createDataplaneGroup()

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplaneGroup.id) } returns MOCK_ORGANIZATION_ID
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } throws DataAccessException(DATAPLANE_GROUP_NAME_CONSTRAINT_VIOLATION_MESSAGE)

    assertThrows<DataplaneGroupNameAlreadyExistsProblem> {
      dataplaneGroupApiController.updateDataplaneGroup(DataplaneGroupUpdateRequestBody(dataplaneGroupId = mockDataplaneGroup.id))
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

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplaneGroup.id) } returns MOCK_ORGANIZATION_ID
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.getDataplaneGroup(mockDataplaneGroup.id) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } answers { firstArg() }
    every { dataplaneService.listDataplanes(any<UUID>()) } returns emptyList()

    val updated =
      dataplaneGroupApiController.updateDataplaneGroup(
        DataplaneGroupUpdateRequestBody(dataplaneGroupId = mockDataplaneGroup.id, name = newName),
      )

    assertEquals(newName, updated.name)
    assertEquals(originalEnabled, updated.enabled)
  }

  @Test
  fun `updateDataplaneGroup with only enabled set should preserve name`() {
    val originalName = "Original Group"
    val newEnabled = true
    val mockDataplaneGroup =
      createDataplaneGroup().apply {
        name = originalName
      }

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplaneGroup.id) } returns MOCK_ORGANIZATION_ID
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.getDataplaneGroup(mockDataplaneGroup.id) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(any()) } answers { firstArg() }
    every { dataplaneService.listDataplanes(any<UUID>()) } returns emptyList()

    val updated =
      dataplaneGroupApiController.updateDataplaneGroup(
        DataplaneGroupUpdateRequestBody(dataplaneGroupId = mockDataplaneGroup.id, enabled = newEnabled),
      )

    assertEquals(newEnabled, updated.enabled)
    assertEquals(originalName, updated.name)
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

    every { dataplaneGroupService.getOrganizationIdFromDataplaneGroup(mockDataplaneGroup.id) } returns MOCK_ORGANIZATION_ID
    every { entitlementService.ensureEntitled(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement) } returns Unit
    every { dataplaneGroupService.getDataplaneGroup(any()) } returns mockDataplaneGroup
    every { dataplaneGroupService.writeDataplaneGroup(mockDataplaneGroup.apply { tombstone = true }) } returns
      mockDataplaneGroup.apply { tombstone = true }
    every { dataplaneService.listDataplanes(any<UUID>()) } returns listOf(mockDataplane)
    every { dataplaneService.deleteDataplane(any()) } returns mockDataplane

    val dataplaneGroupDeleteRequestBody =
      DataplaneGroupDeleteRequestBody(dataplaneGroupId = mockDataplaneGroup.id)

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

    // Mock entitlement checks - no PrivateLink, yes SelfManagedRegions
    every {
      entitlementService.checkEntitlement(OrganizationId(MOCK_ORGANIZATION_ID), PrivateLinkEntitlement)
    } returns EntitlementResult(PrivateLinkEntitlement.featureId, false, "Not entitled", PrivateLinkEntitlement.featureId)

    every {
      entitlementService.checkEntitlement(OrganizationId(MOCK_ORGANIZATION_ID), SelfManagedRegionsEntitlement)
    } returns EntitlementResult(SelfManagedRegionsEntitlement.featureId, true, "Entitled", SelfManagedRegionsEntitlement.featureId)

    every { dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, MOCK_ORGANIZATION_ID), any()) } returns
      listOf(
        createDataplaneGroup(dataplaneGroupId1),
        createDataplaneGroup(dataplaneGroupId2),
      )
    every { dataplaneService.listDataplanes(any<UUID>()) } returns emptyList()

    val dataplaneGroups = dataplaneGroupApiController.listDataplaneGroups(DataplaneGroupListRequestBody(organizationId = MOCK_ORGANIZATION_ID))

    val responseDataplaneGroups = dataplaneGroups.dataplaneGroups!!
    assertEquals(2, responseDataplaneGroups.size)
    assertEquals(dataplaneGroupId1, responseDataplaneGroups[0].dataplaneGroupId)
    assertEquals(dataplaneGroupId2, responseDataplaneGroups[1].dataplaneGroupId)
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
