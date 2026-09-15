/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DataWorkerAllocationApi
import io.airbyte.api.model.generated.DataWorkerAddCapacityRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationGetRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationListRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationListResponse
import io.airbyte.api.model.generated.DataWorkerAllocationRead
import io.airbyte.api.model.generated.DataWorkerReallocateRequestBody
import io.airbyte.api.model.generated.DataWorkerRemoveCapacityRequestBody
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * Defines the Cloud-only Data Worker allocation API for OSS and Cloud deployments.
 *
 * The Cloud implementation lives in `DataWorkerAllocationWrappedController`.
 */
@Controller("/api/v1/data_worker_allocation")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataWorkerAllocationApiController : DataWorkerAllocationApi {
  @Post("/list")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDataWorkerAllocations(
    @Body dataWorkerAllocationListRequestBody: DataWorkerAllocationListRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()

  @Post("/get")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDataWorkerAllocation(
    @Body dataWorkerAllocationGetRequestBody: DataWorkerAllocationGetRequestBody,
  ): DataWorkerAllocationRead = throw ApiNotImplementedInOssProblem()

  @Post("/reallocate")
  @Secured(AuthRoleConstants.ORGANIZATION_ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun reallocateDataWorkerCapacity(
    @Body dataWorkerReallocateRequestBody: DataWorkerReallocateRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()

  @Post("/add_capacity")
  @Secured(AuthRoleConstants.ADMIN)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun addDataWorkerCapacity(
    @Body dataWorkerAddCapacityRequestBody: DataWorkerAddCapacityRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()

  @Post("/remove_capacity")
  @Secured(AuthRoleConstants.ADMIN)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun removeDataWorkerCapacity(
    @Body dataWorkerRemoveCapacityRequestBody: DataWorkerRemoveCapacityRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()
}
