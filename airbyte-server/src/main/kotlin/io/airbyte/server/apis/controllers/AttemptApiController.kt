/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.AttemptApi
import io.airbyte.api.server.generated.models.AttemptInfoRead
import io.airbyte.api.server.generated.models.AttemptStats
import io.airbyte.api.server.generated.models.CreateNewAttemptNumberRequest
import io.airbyte.api.server.generated.models.CreateNewAttemptNumberResponse
import io.airbyte.api.server.generated.models.FailAttemptRequest
import io.airbyte.api.server.generated.models.GetAttemptStatsRequestBody
import io.airbyte.api.server.generated.models.InternalOperationResult
import io.airbyte.api.server.generated.models.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.server.generated.models.SaveStatsRequestBody
import io.airbyte.api.server.generated.models.SaveStreamAttemptMetadataRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class AttemptApiController(
  private val attemptHandler: AttemptHandler,
  private val jobConverter: JobConverter,
) : AttemptApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getAttemptForJob(
    @Body requestBody: GetAttemptStatsRequestBody,
  ): AttemptInfoRead =
    execute {
      val attempt =
        attemptHandler.getAttemptForJob(
          requestBody.jobId,
          requestBody.attemptNumber,
        )
      jobConverter.getAttemptInfoRead(attempt)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createNewAttemptNumber(
    @Body requestBody: CreateNewAttemptNumberRequest,
  ): CreateNewAttemptNumberResponse = execute { attemptHandler.createNewAttemptNumber(requestBody.jobId) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getAttemptCombinedStats(
    @Body requestBody: GetAttemptStatsRequestBody,
  ): AttemptStats =
    execute {
      attemptHandler.getAttemptCombinedStats(
        requestBody.jobId,
        requestBody.attemptNumber,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun saveStats(
    @Body requestBody: SaveStatsRequestBody,
  ): InternalOperationResult = execute { attemptHandler.saveStats(requestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun saveStreamMetadata(
    @Body requestBody: SaveStreamAttemptMetadataRequestBody?,
  ): InternalOperationResult = execute { attemptHandler.saveStreamMetadata(requestBody.requireBody()) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun saveSyncConfig(
    @Body requestBody: SaveAttemptSyncConfigRequestBody,
  ): InternalOperationResult = execute { attemptHandler.saveSyncConfig(requestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun failAttempt(
    @Body requestBody: FailAttemptRequest,
  ) {
    execute<Any?> {
      attemptHandler.failAttempt(
        requestBody.attemptNumber,
        requestBody.jobId,
        requestBody.failureSummary,
        requestBody.standardSyncOutput,
      )
      null
    }
  }
}
