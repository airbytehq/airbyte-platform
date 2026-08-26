/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataWorkerAllocationGetRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationListRequestBody
import io.airbyte.api.model.generated.DataWorkerReallocateRequestBody
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.dataworker.DataWorkerAllocation
import io.airbyte.domain.models.dataworker.OrganizationDataWorkerAllocations
import io.airbyte.domain.services.dataworker.DataWorkerAllocatedCapacityService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

internal class DataWorkerAllocationApiControllerTest {
  private lateinit var allocatedCapacityService: DataWorkerAllocatedCapacityService
  private lateinit var dataplaneGroupService: DataplaneGroupService
  private lateinit var entitlementService: EntitlementService
  private lateinit var controller: DataWorkerAllocationApiController

  private val organizationId = UUID.randomUUID()
  private val usRegion = UUID.randomUUID()
  private val euRegion = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    allocatedCapacityService = mockk(relaxed = true)
    dataplaneGroupService = mockk()
    entitlementService = mockk()
    controller =
      DataWorkerAllocationApiController(
        allocatedCapacityService,
        dataplaneGroupService,
        entitlementService,
      )

    givenEntitledTo(PrivateLinkEntitlement.featureId, false)
    givenEntitledTo(SelfManagedRegionsEntitlement.featureId, false)
    givenSharedRegions(usRegion, euRegion)
  }

  @Test
  fun `listDataWorkerAllocations returns every region and the total`() {
    every { allocatedCapacityService.getAllocations(OrganizationId(organizationId)) } returns
      OrganizationDataWorkerAllocations(
        organizationId = OrganizationId(organizationId),
        totalAllocatedCapacity = 12.5,
        allocations =
          listOf(
            DataWorkerAllocation(DataplaneGroupId(usRegion), 10.0),
            DataWorkerAllocation(DataplaneGroupId(euRegion), 2.5),
          ),
      )

    val response = controller.listDataWorkerAllocations(DataWorkerAllocationListRequestBody().organizationId(organizationId))

    assertEquals(organizationId, response.organizationId)
    assertEquals(12.5, response.totalAllocatedCapacity)
    assertEquals(listOf(usRegion, euRegion), response.allocations.map { it.dataplaneGroupId })
    assertEquals(listOf(10.0, 2.5), response.allocations.map { it.allocatedCapacity })
  }

  @Test
  fun `listDataWorkerAllocations reports an organization with no capacity as zero`() {
    every { allocatedCapacityService.getAllocations(OrganizationId(organizationId)) } returns
      OrganizationDataWorkerAllocations(OrganizationId(organizationId), 0.0, emptyList())

    val response = controller.listDataWorkerAllocations(DataWorkerAllocationListRequestBody().organizationId(organizationId))

    assertEquals(0.0, response.totalAllocatedCapacity)
    assertEquals(0, response.allocations.size)
  }

  @Test
  fun `getDataWorkerAllocation returns the region's capacity`() {
    every {
      allocatedCapacityService.getAllocation(OrganizationId(organizationId), DataplaneGroupId(usRegion))
    } returns DataWorkerAllocation(DataplaneGroupId(usRegion), 7.5)

    val response =
      controller.getDataWorkerAllocation(
        DataWorkerAllocationGetRequestBody().organizationId(organizationId).dataplaneGroupId(usRegion),
      )

    assertEquals(usRegion, response.dataplaneGroupId)
    assertEquals(7.5, response.allocatedCapacity)
  }

  @Test
  fun `getDataWorkerAllocation reports a region with no allocation as zero rather than failing`() {
    every { allocatedCapacityService.getAllocation(OrganizationId(organizationId), DataplaneGroupId(euRegion)) } returns null

    val response =
      controller.getDataWorkerAllocation(
        DataWorkerAllocationGetRequestBody().organizationId(organizationId).dataplaneGroupId(euRegion),
      )

    assertEquals(euRegion, response.dataplaneGroupId)
    assertEquals(0.0, response.allocatedCapacity)
  }

  @Test
  fun `reallocateDataWorkerCapacity moves the capacity and returns the updated list`() {
    every { allocatedCapacityService.getAllocations(OrganizationId(organizationId)) } returns
      OrganizationDataWorkerAllocations(
        organizationId = OrganizationId(organizationId),
        totalAllocatedCapacity = 15.0,
        allocations =
          listOf(
            DataWorkerAllocation(DataplaneGroupId(usRegion), 7.0),
            DataWorkerAllocation(DataplaneGroupId(euRegion), 8.0),
          ),
      )

    val response = controller.reallocateDataWorkerCapacity(reallocateRequest(from = usRegion, to = euRegion, amount = 3.0))

    verify {
      allocatedCapacityService.reallocateCapacity(
        OrganizationId(organizationId),
        DataplaneGroupId(usRegion),
        DataplaneGroupId(euRegion),
        3.0,
      )
    }
    assertEquals(15.0, response.totalAllocatedCapacity)
  }

  @Test
  fun `reallocateDataWorkerCapacity refuses a region the organization may not use`() {
    val someoneElsesRegion = UUID.randomUUID()

    assertThrows<BadRequestProblem> {
      controller.reallocateDataWorkerCapacity(reallocateRequest(from = usRegion, to = someoneElsesRegion, amount = 1.0))
    }

    verify(exactly = 0) { allocatedCapacityService.reallocateCapacity(any(), any(), any(), any()) }
  }

  @Test
  fun `reallocateDataWorkerCapacity refuses an organization's own region when it is not entitled to self-managed regions`() {
    val ownRegion = UUID.randomUUID()
    givenRegionsOwnedBy(organizationId, ownRegion)

    assertThrows<BadRequestProblem> {
      controller.reallocateDataWorkerCapacity(reallocateRequest(from = usRegion, to = ownRegion, amount = 1.0))
    }

    verify(exactly = 0) { allocatedCapacityService.reallocateCapacity(any(), any(), any(), any()) }
  }

  @Test
  fun `reallocateDataWorkerCapacity allows an organization's own region when it is entitled to self-managed regions`() {
    val ownRegion = UUID.randomUUID()
    givenEntitledTo(SelfManagedRegionsEntitlement.featureId, true)
    every {
      dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, organizationId), true)
    } returns listOf(regionRow(usRegion), regionRow(ownRegion))

    controller.reallocateDataWorkerCapacity(reallocateRequest(from = usRegion, to = ownRegion, amount = 1.0))

    verify {
      allocatedCapacityService.reallocateCapacity(
        OrganizationId(organizationId),
        DataplaneGroupId(usRegion),
        DataplaneGroupId(ownRegion),
        1.0,
      )
    }
  }

  @Test
  fun `reallocateDataWorkerCapacity allows a PrivateLink region only when the organization is entitled to it`() {
    val privateLinkRegion = UUID.randomUUID()
    givenEntitledTo(PrivateLinkEntitlement.featureId, true)
    every {
      dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID), true)
    } returns listOf(regionRow(usRegion), regionRow(privateLinkRegion))

    controller.reallocateDataWorkerCapacity(reallocateRequest(from = usRegion, to = privateLinkRegion, amount = 1.0))

    verify {
      allocatedCapacityService.reallocateCapacity(
        OrganizationId(organizationId),
        DataplaneGroupId(usRegion),
        DataplaneGroupId(privateLinkRegion),
        1.0,
      )
    }
  }

  @Test
  fun `reallocateDataWorkerCapacity moves capacity out of a region the organization may no longer use`() {
    // PrivateLink and self-managed regions are entitlements, and entitlements get revoked. Capacity
    // left behind in a region the organization can no longer use still has to be recoverable, so the
    // source region is not checked against the usable list.
    val revokedRegion = UUID.randomUUID()

    controller.reallocateDataWorkerCapacity(reallocateRequest(from = revokedRegion, to = usRegion, amount = 1.0))

    verify {
      allocatedCapacityService.reallocateCapacity(
        OrganizationId(organizationId),
        DataplaneGroupId(revokedRegion),
        DataplaneGroupId(usRegion),
        1.0,
      )
    }
  }

  private fun reallocateRequest(
    from: UUID,
    to: UUID,
    amount: Double,
  ) = DataWorkerReallocateRequestBody()
    .organizationId(organizationId)
    .fromDataplaneGroupId(from)
    .toDataplaneGroupId(to)
    .amount(amount)

  private fun givenEntitledTo(
    featureId: String,
    entitled: Boolean,
  ) {
    every {
      entitlementService.checkEntitlement(OrganizationId(organizationId), match { it.featureId == featureId })
    } returns EntitlementResult(featureId, entitled)
  }

  /** Regions owned by the default organization, which every organization may use. */
  private fun givenSharedRegions(vararg dataplaneGroupIds: UUID) {
    every { dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), true) } returns
      dataplaneGroupIds.map { regionRow(it) }
  }

  private fun givenRegionsOwnedBy(
    ownerOrganizationId: UUID,
    vararg dataplaneGroupIds: UUID,
  ) {
    every { dataplaneGroupService.listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID, ownerOrganizationId), true) } returns
      dataplaneGroupIds.map { regionRow(it) }
  }

  private fun regionRow(dataplaneGroupId: UUID) =
    DataplaneGroup().apply {
      id = dataplaneGroupId
    }
}
