/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.JobsApi
import io.airbyte.api.model.generated.BooleanRead
import io.airbyte.api.model.generated.CheckInput
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionJobRequestBody
import io.airbyte.api.model.generated.DeleteStreamResetRecordsForJobRequest
import io.airbyte.api.model.generated.GetWebhookConfigRequest
import io.airbyte.api.model.generated.GetWebhookConfigResponse
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.JobCreate
import io.airbyte.api.model.generated.JobDebugInfoRead
import io.airbyte.api.model.generated.JobExplainRead
import io.airbyte.api.model.generated.JobFailureRequest
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobInfoLightRead
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.api.model.generated.JobListRequestBody
import io.airbyte.api.model.generated.JobOptionalRead
import io.airbyte.api.model.generated.JobReadList
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest
import io.airbyte.api.model.generated.PersistCancelJobRequestBody
import io.airbyte.api.model.generated.ReportJobStartRequest
import io.airbyte.api.model.generated.SyncInput
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.JobInputHandler
import io.airbyte.commons.server.handlers.JobsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.temporal.StreamResetRecordsHelper
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path

@Controller("/api/v1/jobs")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class JobsApiController(
  private val jobHistoryHandler: JobHistoryHandler,
  private val schedulerHandler: SchedulerHandler,
  private val jobInputHandler: JobInputHandler,
  private val jobsHandler: JobsHandler,
  private val streamResetRecordsHelper: StreamResetRecordsHelper,
) : JobsApi {
  @Post("/cancel")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  override fun cancelJob(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead? = execute { schedulerHandler.cancelJob(jobIdRequestBody) }

  @Post("/create")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  override fun createJob(
    @Body jobCreate: JobCreate,
  ): JobInfoRead? = execute { schedulerHandler.createJob(jobCreate) }

  @Post("/fail_non_terminal")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun failNonTerminalJobs(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ) {
    execute<Any?> {
      jobsHandler.failNonTerminalJobs(connectionIdRequestBody.connectionId)
      null // to satisfy the lambda interface bounds
    }
  }

  @Post("/get_check_input")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getCheckInput(
    @Body checkInput: CheckInput?,
  ): Any? = execute { jobInputHandler.getCheckJobInput(checkInput) }

  @Post("/get_debug_info")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun getJobDebugInfo(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobDebugInfoRead? = execute { jobHistoryHandler.getJobDebugInfo(jobIdRequestBody.id) }

  @Post("/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getJobInfo(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead? = execute { jobHistoryHandler.getJobInfo(jobIdRequestBody.id) }

  @Post("/get_without_logs")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getJobInfoWithoutLogs(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead? = execute { jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody.id) }

  @Post("/get_input")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getJobInput(
    @Body syncInput: SyncInput?,
  ): Any? = execute { jobInputHandler.getJobInput(syncInput) }

  @Post("/get_light")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getJobInfoLight(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoLightRead? = execute { jobHistoryHandler.getJobInfoLight(jobIdRequestBody) }

  @Post("/get_last_replication_job")
  @Secured(AuthRoleConstants.READER, AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getLastReplicationJob(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobOptionalRead? = execute { jobHistoryHandler.getLastReplicationJob(connectionIdRequestBody) }

  @Post("/get_last_replication_job_with_cancel")
  @Secured(AuthRoleConstants.READER, AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getLastReplicationJobWithCancel(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobOptionalRead? = execute { jobHistoryHandler.getLastReplicationJobWithCancel(connectionIdRequestBody) }

  @Post("/getWebhookConfig")
  @Secured(AuthRoleConstants.READER, AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWebhookConfig(
    @Body getWebhookConfigRequest: GetWebhookConfigRequest,
  ): GetWebhookConfigResponse? =
    execute {
      GetWebhookConfigResponse().value(Jsons.serialize(jobInputHandler.getJobWebhookConfig(getWebhookConfigRequest.jobId)))
    }

  @Post("/job_failure")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun jobFailure(
    @Body jobFailureRequest: JobFailureRequest,
  ): InternalOperationResult? = execute { jobsHandler.jobFailure(jobFailureRequest) }

  @POST
  @Path("/job_success_with_attempt_number")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun jobSuccessWithAttemptNumber(
    @Body jobSuccessWithAttemptNumberRequest: JobSuccessWithAttemptNumberRequest,
  ): InternalOperationResult? =
    execute {
      jobsHandler.jobSuccessWithAttemptNumber(
        jobSuccessWithAttemptNumberRequest,
      )
    }

  @Post("/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listJobsFor(
    @Body jobListRequestBody: JobListRequestBody,
  ): JobReadList? = execute { jobHistoryHandler.listJobsFor(jobListRequestBody) }

  @Post("/list_for_workspaces")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listJobsForWorkspaces(
    @Body requestBody: JobListForWorkspacesRequestBody,
  ): JobReadList? = execute { jobHistoryHandler.listJobsForWorkspaces(requestBody) }

  @Post("/reportJobStart")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun reportJobStart(
    @Body reportJobStartRequest: ReportJobStartRequest,
  ): InternalOperationResult? = execute { jobsHandler.reportJobStart(reportJobStartRequest.jobId) }

  @Post(uri = "/did_previous_job_succeed")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
  override fun didPreviousJobSucceed(
    @Body requestBody: ConnectionJobRequestBody,
  ): BooleanRead? =
    execute {
      jobsHandler.didPreviousJobSucceed(
        requestBody.connectionId,
        requestBody.jobId,
      )
    }

  @Post("/explain")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @RequiresIntent(Intent.ViewConnection)
  override fun explainJob(jobIdRequestBody: JobIdRequestBody): JobExplainRead = throw ApiNotImplementedInOssProblem()

  @Post("/persist_cancellation")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun persistJobCancellation(
    @Body requestBody: PersistCancelJobRequestBody,
  ) {
    execute<Any?> {
      jobsHandler.persistJobCancellation(
        requestBody.connectionId,
        requestBody.jobId,
        requestBody.attemptNumber,
        requestBody.attemptFailureSummary,
      )
      null
    }
  }

  @Post("/delete_stream_reset_records")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteStreamResetRecordsForJob(
    @Body requestBody: DeleteStreamResetRecordsForJobRequest,
  ) {
    execute<Any?> {
      streamResetRecordsHelper.deleteStreamResetRecordsForJob(requestBody.jobId, requestBody.connectionId)
      null
    }
  }
}
