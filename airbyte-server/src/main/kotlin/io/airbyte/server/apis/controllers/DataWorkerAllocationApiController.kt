/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.DataWorkerAllocationApi
import io.airbyte.api.server.generated.models.DataWorkerAddCapacityRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationGetRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationListRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationListResponse
import io.airbyte.api.server.generated.models.DataWorkerAllocationRead
import io.airbyte.api.server.generated.models.DataWorkerReallocateRequestBody
import io.airbyte.api.server.generated.models.DataWorkerRemoveCapacityRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * Defines the Cloud-only Data Worker allocation API for OSS and Cloud deployments.
 *
 * The Cloud implementation lives in `DataWorkerAllocationWrappedController`.
 */
@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class DataWorkerAllocationApiController : DataWorkerAllocationApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DataWorkerAllocationOrganizationUnsupportedRule::class,
  )
  override fun listDataWorkerAllocations(
    @Body dataWorkerAllocationListRequestBody: DataWorkerAllocationListRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DataWorkerAllocationOrganizationUnsupportedRule::class,
  )
  override fun getDataWorkerAllocation(
    @Body dataWorkerAllocationGetRequestBody: DataWorkerAllocationGetRequestBody,
  ): DataWorkerAllocationRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DataWorkerAllocationOrganizationUnsupportedRule::class,
  )
  override fun reallocateDataWorkerCapacity(
    @Body dataWorkerReallocateRequestBody: DataWorkerReallocateRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.OssInstanceAdminUnsupportedRule::class,
  )
  override fun addDataWorkerCapacity(
    @Body dataWorkerAddCapacityRequestBody: DataWorkerAddCapacityRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()

  @AuditLogging(provider = AuditLoggingProvider.BASIC)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.OssInstanceAdminUnsupportedRule::class,
  )
  override fun removeDataWorkerCapacity(
    @Body dataWorkerRemoveCapacityRequestBody: DataWorkerRemoveCapacityRequestBody,
  ): DataWorkerAllocationListResponse = throw ApiNotImplementedInOssProblem()
}
