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
import io.airbyte.server.auth.JobAdminReadRule
import io.airbyte.server.auth.JobAdminRule
import io.airbyte.server.auth.JobCreateRule
import io.airbyte.server.auth.JobExplainUnsupportedRule
import io.airbyte.server.auth.JobInputRule
import io.airbyte.server.auth.JobReadOrWorkerRule
import io.airbyte.server.auth.JobReadRule
import io.airbyte.server.auth.JobRunRule
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobRunRule::class,
  )
  override fun cancelJob(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead = execute { schedulerHandler.cancelJob(jobIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobCreateRule::class,
  )
  override fun createJob(
    @Body jobCreate: JobCreate,
  ): JobInfoRead = execute { schedulerHandler.createJob(jobCreate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun failNonTerminalJobs(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ) {
    execute<Any?> {
      jobsHandler.failNonTerminalJobs(connectionIdRequestBody.connectionId)
      null // to satisfy the lambda interface bounds
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
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

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun getJobDebugInfo(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobDebugInfoRead = execute { jobHistoryHandler.getJobDebugInfo(jobIdRequestBody.id) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun getJobInfo(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead = execute { jobHistoryHandler.getJobInfo(jobIdRequestBody.id) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun getJobInfoWithoutLogs(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoRead = execute { jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody.id) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobInputRule::class,
  )
  override fun getJobInput(
    @Body syncInput: SyncInput,
  ): Any = execute { jobInputHandler.getJobInput(syncInput) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun getJobInfoLight(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): JobInfoLightRead = execute { jobHistoryHandler.getJobInfoLight(jobIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadOrWorkerRule::class,
  )
  override fun getLastReplicationJob(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobOptionalRead = execute { jobHistoryHandler.getLastReplicationJob(connectionIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun getLastReplicationJobWithCancel(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobOptionalRead = execute { jobHistoryHandler.getLastReplicationJobWithCancel(connectionIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun getWebhookConfig(
    @Body getWebhookConfigRequest: GetWebhookConfigRequest,
  ): GetWebhookConfigResponse =
    execute {
      GetWebhookConfigResponse(
        value = Jsons.serialize(jobInputHandler.getJobWebhookConfig(getWebhookConfigRequest.jobId)),
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun jobFailure(
    @Body jobFailureRequest: JobFailureRequest,
  ): InternalOperationResult = execute { jobsHandler.jobFailure(jobFailureRequest) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun jobSuccessWithAttemptNumber(
    @Body jobSuccessWithAttemptNumberRequest: JobSuccessWithAttemptNumberRequest,
  ): InternalOperationResult =
    execute {
      jobsHandler.jobSuccessWithAttemptNumber(
        jobSuccessWithAttemptNumberRequest,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun listJobsFor(
    @Body jobListRequestBody: JobListRequestBody,
  ): JobReadList = execute { jobHistoryHandler.listJobsFor(jobListRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobReadRule::class,
  )
  override fun listJobsForWorkspaces(
    @Body requestBody: JobListForWorkspacesRequestBody,
  ): JobReadList = execute { jobHistoryHandler.listJobsForWorkspaces(requestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun reportJobStart(
    @Body reportJobStartRequest: ReportJobStartRequest,
  ): InternalOperationResult = execute { jobsHandler.reportJobStart(reportJobStartRequest.jobId) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun setJobQueued(
    @Body setJobQueuedRequest: SetJobQueuedRequest,
  ): InternalOperationResult = execute { jobsHandler.setJobQueued(setJobQueuedRequest.jobId) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun cancelQueuedJob(
    @Body cancelQueuedJobRequest: CancelQueuedJobRequest,
  ): InternalOperationResult = execute { jobsHandler.cancelQueuedJob(cancelQueuedJobRequest.jobId) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminReadRule::class,
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
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobExplainUnsupportedRule::class,
  )
  override fun explainJob(jobIdRequestBody: JobIdRequestBody): JobExplainRead = throw ApiNotImplementedInOssProblem()

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
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
