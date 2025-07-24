/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.AttemptApi
import io.airbyte.api.model.generated.AttemptInfoRead
import io.airbyte.api.model.generated.AttemptStats
import io.airbyte.api.model.generated.CreateNewAttemptNumberRequest
import io.airbyte.api.model.generated.CreateNewAttemptNumberResponse
import io.airbyte.api.model.generated.FailAttemptRequest
import io.airbyte.api.model.generated.GetAttemptStatsRequestBody
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody
import io.airbyte.api.model.generated.SaveStatsRequestBody
import io.airbyte.api.model.generated.SaveStreamAttemptMetadataRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/attempt")
@Secured(SecurityRule.IS_AUTHENTICATED)
class AttemptApiController(
  private val attemptHandler: AttemptHandler,
  private val jobConverter: JobConverter,
) : AttemptApi {
  @Post(uri = "/get_for_job", processes = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun getAttemptForJob(
    @Body requestBody: GetAttemptStatsRequestBody,
  ): AttemptInfoRead? =
    execute {
      val attempt =
        attemptHandler.getAttemptForJob(
          requestBody.jobId,
          requestBody.attemptNumber,
        )
      jobConverter.getAttemptInfoRead(attempt)
    }

  @Post(uri = "/create_new_attempt_number", processes = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
  override fun createNewAttemptNumber(
    @Body requestBody: CreateNewAttemptNumberRequest,
  ): CreateNewAttemptNumberResponse? = execute { attemptHandler.createNewAttemptNumber(requestBody.jobId) }

  @Post(uri = "/get_combined_stats", processes = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun getAttemptCombinedStats(
    @Body requestBody: GetAttemptStatsRequestBody,
  ): AttemptStats? =
    execute {
      attemptHandler.getAttemptCombinedStats(
        requestBody.jobId,
        requestBody.attemptNumber,
      )
    }

  @Post(uri = "/save_stats", processes = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun saveStats(
    @Body requestBody: SaveStatsRequestBody,
  ): InternalOperationResult? = execute { attemptHandler.saveStats(requestBody) }

  @Post(uri = "/save_stream_metadata", processes = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun saveStreamMetadata(
    @Body requestBody: SaveStreamAttemptMetadataRequestBody,
  ): InternalOperationResult? = execute { attemptHandler.saveStreamMetadata(requestBody) }

  @Post(uri = "/save_sync_config", processes = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun saveSyncConfig(
    @Body requestBody: SaveAttemptSyncConfigRequestBody,
  ): InternalOperationResult? = execute { attemptHandler.saveSyncConfig(requestBody) }

  @Post(uri = "/fail", processes = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
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
