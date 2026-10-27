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
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.JobAdminRule
import io.airbyte.server.auth.JobReadRule
import io.airbyte.server.auth.SaveAttemptStatsRule
import io.airbyte.server.auth.SaveAttemptStreamMetadataRule
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun createNewAttemptNumber(
    @Body requestBody: CreateNewAttemptNumberRequest,
  ): CreateNewAttemptNumberResponse = execute { attemptHandler.createNewAttemptNumber(requestBody.jobId) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SaveAttemptStatsRule::class,
  )
  override fun saveStats(
    @Body requestBody: SaveStatsRequestBody,
  ): InternalOperationResult = execute { attemptHandler.saveStats(requestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = SaveAttemptStreamMetadataRule::class,
  )
  override fun saveStreamMetadata(
    @Body requestBody: SaveStreamAttemptMetadataRequestBody?,
  ): InternalOperationResult = execute { attemptHandler.saveStreamMetadata(requestBody.requireBody()) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun saveSyncConfig(
    @Body requestBody: SaveAttemptSyncConfigRequestBody,
  ): InternalOperationResult = execute { attemptHandler.saveSyncConfig(requestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
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
