/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.JobsApi
import io.airbyte.api.server.generated.models.BooleanRead
import io.airbyte.api.server.generated.models.CancelQueuedJobRequest
import io.airbyte.api.server.generated.models.CheckDataWorkerCapacityRead
import io.airbyte.api.server.generated.models.CheckDataWorkerCapacityRequest
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionJobRequestBody
import io.airbyte.api.server.generated.models.DeleteStreamResetRecordsForJobRequest
import io.airbyte.api.server.generated.models.GetWebhookConfigRequest
import io.airbyte.api.server.generated.models.GetWebhookConfigResponse
import io.airbyte.api.server.generated.models.InternalOperationResult
import io.airbyte.api.server.generated.models.JobCreate
import io.airbyte.api.server.generated.models.JobDebugInfoRead
import io.airbyte.api.server.generated.models.JobExplainRead
import io.airbyte.api.server.generated.models.JobFailureRequest
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobInfoLightRead
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobListForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.JobListRequestBody
import io.airbyte.api.server.generated.models.JobOptionalRead
import io.airbyte.api.server.generated.models.JobReadList
import io.airbyte.api.server.generated.models.JobSuccessWithAttemptNumberRequest
import io.airbyte.api.server.generated.models.PersistCancelJobRequestBody
import io.airbyte.api.server.generated.models.ReportJobStartRequest
import io.airbyte.api.server.generated.models.SetJobQueuedRequest
import io.airbyte.api.server.generated.models.SyncInput
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.JobInputHandler
import io.airbyte.commons.server.handlers.JobsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.temporal.StreamResetRecordsHelper
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.services.JobObservabilityService
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class JobsApiController(
  private val jobHistoryHandler: JobHistoryHandler,
  private val schedulerHandler: SchedulerHandler,
  private val jobInputHandler: JobInputHandler,
  private val jobsHandler: JobsHandler,
  private val streamResetRecordsHelper: StreamResetRecordsHelper,
  private val jobObservabilityService: JobObservabilityService,
) : JobsApi {
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun cancelJob(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead = execute { schedulerHandler.cancelJob(jobIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createJob(
    @Body jobCreate: JobCreate,
  ): JobInfoRead = execute { schedulerHandler.createJob(jobCreate) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun failNonTerminalJobs(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ) {
    execute<Any?> {
      jobsHandler.failNonTerminalJobs(connectionIdRequestBody.connectionId)
      null // to satisfy the lambda interface bounds
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun evaluateOutlier(jobIdRequestBody: JobIdRequestBody): InternalOperationResult =
    execute {
      try {
        jobObservabilityService.evaluateOutlier(jobId = jobIdRequestBody.id)
        InternalOperationResult(succeeded = true)
      } catch (t: Throwable) {
        ApmTraceUtils.addTagsToTrace(mapOf(MetricTags.JOB_ID to jobIdRequestBody.id))
        throw t
      }
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun finalizeJob(jobIdRequestBody: JobIdRequestBody): InternalOperationResult =
    execute {
      try {
        jobObservabilityService.finalizeStats(jobId = jobIdRequestBody.id)
        InternalOperationResult(succeeded = true)
      } catch (t: Throwable) {
        ApmTraceUtils.addTagsToTrace(mapOf(MetricTags.JOB_ID to jobIdRequestBody.id))
        throw t
      }
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getJobDebugInfo(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobDebugInfoRead = execute { jobHistoryHandler.getJobDebugInfo(jobIdRequestBody.id) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getJobInfo(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead = execute { jobHistoryHandler.getJobInfo(jobIdRequestBody.id) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getJobInfoWithoutLogs(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead = execute { jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody.id) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getJobInput(
    @Body syncInput: SyncInput,
  ): Any = execute { jobInputHandler.getJobInput(syncInput) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getJobInfoLight(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoLightRead = execute { jobHistoryHandler.getJobInfoLight(jobIdRequestBody) }

  @Secured(AuthRoleConstants.READER, AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getLastReplicationJob(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobOptionalRead = execute { jobHistoryHandler.getLastReplicationJob(connectionIdRequestBody) }

  @Secured(AuthRoleConstants.READER, AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getLastReplicationJobWithCancel(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobOptionalRead = execute { jobHistoryHandler.getLastReplicationJobWithCancel(connectionIdRequestBody) }

  @Secured(AuthRoleConstants.READER, AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getWebhookConfig(
    @Body getWebhookConfigRequest: GetWebhookConfigRequest,
  ): GetWebhookConfigResponse =
    execute {
      GetWebhookConfigResponse(
        value = Jsons.serialize(jobInputHandler.getJobWebhookConfig(getWebhookConfigRequest.jobId)),
      )
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun jobFailure(
    @Body jobFailureRequest: JobFailureRequest,
  ): InternalOperationResult = execute { jobsHandler.jobFailure(jobFailureRequest) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun jobSuccessWithAttemptNumber(
    @Body jobSuccessWithAttemptNumberRequest: JobSuccessWithAttemptNumberRequest,
  ): InternalOperationResult =
    execute {
      jobsHandler.jobSuccessWithAttemptNumber(
        jobSuccessWithAttemptNumberRequest,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listJobsFor(
    @Body jobListRequestBody: JobListRequestBody,
  ): JobReadList = execute { jobHistoryHandler.listJobsFor(jobListRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listJobsForWorkspaces(
    @Body requestBody: JobListForWorkspacesRequestBody,
  ): JobReadList = execute { jobHistoryHandler.listJobsForWorkspaces(requestBody) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun reportJobStart(
    @Body reportJobStartRequest: ReportJobStartRequest,
  ): InternalOperationResult = execute { jobsHandler.reportJobStart(reportJobStartRequest.jobId) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun setJobQueued(
    @Body setJobQueuedRequest: SetJobQueuedRequest,
  ): InternalOperationResult = execute { jobsHandler.setJobQueued(setJobQueuedRequest.jobId) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun cancelQueuedJob(
    @Body cancelQueuedJobRequest: CancelQueuedJobRequest,
  ): InternalOperationResult = execute { jobsHandler.cancelQueuedJob(cancelQueuedJobRequest.jobId) }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun checkDataWorkerCapacity(
    @Body checkDataWorkerCapacityRequest: CheckDataWorkerCapacityRequest,
  ): CheckDataWorkerCapacityRead =
    execute {
      jobsHandler.checkDataWorkerCapacity(
        checkDataWorkerCapacityRequest.jobId,
        checkDataWorkerCapacityRequest.connectionId,
        checkDataWorkerCapacityRequest.organizationId,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.ADMIN)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun didPreviousJobSucceed(
    @Body requestBody: ConnectionJobRequestBody,
  ): BooleanRead =
    execute {
      jobsHandler.didPreviousJobSucceed(
        requestBody.connectionId,
        requestBody.jobId,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @RequiresIntent(Intent.ViewConnection)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun explainJob(jobIdRequestBody: JobIdRequestBody): JobExplainRead = throw ApiNotImplementedInOssProblem()

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun persistJobCancellation(
    @Body requestBody: PersistCancelJobRequestBody?,
  ) {
    execute<Any?> {
      val requestBody = requestBody.requireBody()
      jobsHandler.persistJobCancellation(
        requestBody.connectionId,
        requestBody.jobId,
        requestBody.attemptNumber,
        requestBody.attemptFailureSummary,
      )
      null
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteStreamResetRecordsForJob(
    @Body requestBody: DeleteStreamResetRecordsForJobRequest,
  ) {
    execute<Any?> {
      streamResetRecordsHelper.deleteStreamResetRecordsForJob(requestBody.jobId, requestBody.connectionId)
      null
    }
  }
}
