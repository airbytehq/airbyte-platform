/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataWorkerAllocationApi
import io.airbyte.api.model.generated.DataWorkerAllocationGetRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationListRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationListResponse
import io.airbyte.api.model.generated.DataWorkerAllocationRead
import io.airbyte.api.model.generated.DataWorkerReallocateRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.PrivateLinkEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.dataworker.DataWorkerAllocation
import io.airbyte.domain.models.dataworker.OrganizationDataWorkerAllocations
import io.airbyte.domain.services.dataworker.DataWorkerAllocatedCapacityService
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

/**
 * Reads and edits the Data Worker capacity an organization holds in each region.
 *
 * `AuthorizationServerHandler` strips any client-supplied `X-Airbyte-Organization-Id` and
 * repopulates it from the request body's `organization_id`, so `@Secured` checks the role against
 * the same organization the route goes on to read or modify.
 */
@Controller("/api/v1/data_worker_allocation")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataWorkerAllocationApiController(
  private val dataWorkerAllocatedCapacityService: DataWorkerAllocatedCapacityService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val entitlementService: EntitlementService,
) : DataWorkerAllocationApi {
  @Post("/list")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDataWorkerAllocations(
    @Body dataWorkerAllocationListRequestBody: DataWorkerAllocationListRequestBody,
  ): DataWorkerAllocationListResponse {
    val organizationId = dataWorkerAllocationListRequestBody.organizationId

    return dataWorkerAllocatedCapacityService
      .getAllocations(OrganizationId(organizationId))
      .toListResponse()
  }

  @Post("/get")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDataWorkerAllocation(
    @Body dataWorkerAllocationGetRequestBody: DataWorkerAllocationGetRequestBody,
  ): DataWorkerAllocationRead {
    val organizationId = dataWorkerAllocationGetRequestBody.organizationId
    val dataplaneGroupId = DataplaneGroupId(dataWorkerAllocationGetRequestBody.dataplaneGroupId)

    // An organization holding nothing in a region has no row rather than a zero row. Reporting that
    // as zero keeps this consistent with /list, which reports an organization with no rows at all
    // as a total of zero rather than as an error.
    val allocation =
      dataWorkerAllocatedCapacityService.getAllocation(OrganizationId(organizationId), dataplaneGroupId)
        ?: DataWorkerAllocation(dataplaneGroupId, 0.0)

    return allocation.toRead()
  }

  @Post("/reallocate")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun reallocateDataWorkerCapacity(
    @Body dataWorkerReallocateRequestBody: DataWorkerReallocateRequestBody,
  ): DataWorkerAllocationListResponse {
    val organizationId = dataWorkerReallocateRequestBody.organizationId
    requireRegionIsUsableBy(organizationId, dataWorkerReallocateRequestBody.toDataplaneGroupId)

    dataWorkerAllocatedCapacityService.reallocateCapacity(
      organizationId = OrganizationId(organizationId),
      from = DataplaneGroupId(dataWorkerReallocateRequestBody.fromDataplaneGroupId),
      to = DataplaneGroupId(dataWorkerReallocateRequestBody.toDataplaneGroupId),
      amount = dataWorkerReallocateRequestBody.amount,
    )

    return dataWorkerAllocatedCapacityService
      .getAllocations(OrganizationId(organizationId))
      .toListResponse()
  }

  /**
   * Rejects a region the organization is not allowed to run in.
   *
   * `dataplane_group.organization_id` records who *owns* a region rather than who may use it —
   * every shared Cloud region is owned by [DEFAULT_ORGANIZATION_ID].
   *
   * The source region is deliberately not checked. PrivateLink and self-managed regions are
   * entitlements, and an entitlement can be revoked while the organization still holds capacity in
   * that region. Checking for destination region to allow source regions space reallocated
   *
   * Tombstoned regions are included so that a deleted destination reaches the service, which
   * reports it as deleted rather than as one the organization may not use.
   */
  private fun requireRegionIsUsableBy(
    organizationId: UUID,
    dataplaneGroupId: UUID,
  ) {
    val usableRegionIds =
      dataplaneGroupService
        .listDataplaneGroups(usableRegionOwners(organizationId), true)
        .map { it.id }
        .toSet()

    if (dataplaneGroupId !in usableRegionIds) {
      throw BadRequestProblem(
        ProblemMessageData().message("Organization $organizationId may not use region $dataplaneGroupId."),
      )
    }
  }

  /**
   * The organizations whose regions this one may draw from: the shared Cloud regions, plus
   * PrivateLink and its own self-managed regions when entitled to them.
   */
  private fun usableRegionOwners(organizationId: UUID): List<UUID> {
    val owners = mutableListOf(DEFAULT_ORGANIZATION_ID)

    if (entitlementService.checkEntitlement(OrganizationId(organizationId), PrivateLinkEntitlement).isEntitled) {
      owners.add(PRIVATELINK_DATAPLANE_GROUP_ORGANIZATION_ID)
    }
    if (entitlementService.checkEntitlement(OrganizationId(organizationId), SelfManagedRegionsEntitlement).isEntitled) {
      owners.add(organizationId)
    }

    return owners
  }

  private fun OrganizationDataWorkerAllocations.toListResponse(): DataWorkerAllocationListResponse =
    DataWorkerAllocationListResponse()
      .organizationId(organizationId.value)
      .totalAllocatedCapacity(totalAllocatedCapacity)
      .allocations(allocations.map { it.toRead() })

  private fun DataWorkerAllocation.toRead(): DataWorkerAllocationRead =
    DataWorkerAllocationRead()
      .dataplaneGroupId(dataplaneGroupId.value)
      .allocatedCapacity(allocatedCapacity)
}
