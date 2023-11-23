/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.JobsApi;
import io.airbyte.api.model.generated.AttemptNormalizationStatusReadList;
import io.airbyte.api.model.generated.BooleanRead;
import io.airbyte.api.model.generated.CheckInput;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionJobRequestBody;
import io.airbyte.api.model.generated.DeleteStreamResetRecordsForJobRequest;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobCreate;
import io.airbyte.api.model.generated.JobDebugInfoRead;
import io.airbyte.api.model.generated.JobFailureRequest;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoLightRead;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody;
import io.airbyte.api.model.generated.JobListRequestBody;
import io.airbyte.api.model.generated.JobOptionalRead;
import io.airbyte.api.model.generated.JobReadList;
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.api.model.generated.PersistCancelJobRequestBody;
import io.airbyte.api.model.generated.ReportJobStartRequest;
import io.airbyte.api.model.generated.SyncInput;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.JobInputHandler;
import io.airbyte.commons.server.handlers.JobsHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.temporal.StreamResetRecordsHelper;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Controller("/api/v1/jobs")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class JobsApiController implements JobsApi {

  private final JobHistoryHandler jobHistoryHandler;
  private final SchedulerHandler schedulerHandler;
  private final JobsHandler jobsHandler;
  private final JobInputHandler jobInputHandler;
  private final StreamResetRecordsHelper streamResetRecordsHelper;

  public JobsApiController(final JobHistoryHandler jobHistoryHandler,
                           final SchedulerHandler schedulerHandler,
                           final JobInputHandler jobInputHandler,
                           final JobsHandler jobsHandler,
                           final StreamResetRecordsHelper streamResetRecordsHelper) {
    this.jobHistoryHandler = jobHistoryHandler;
    this.schedulerHandler = schedulerHandler;
    this.jobInputHandler = jobInputHandler;
    this.jobsHandler = jobsHandler;
    this.streamResetRecordsHelper = streamResetRecordsHelper;
  }

  @Post("/cancel")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobInfoRead cancelJob(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.cancelJob(jobIdRequestBody));
  }

  @Post("/create")
  @SecuredWorkspace
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobInfoRead createJob(final JobCreate jobCreate) {
    return ApiHelper.execute(() -> schedulerHandler.createJob(jobCreate));
  }

  @Post("/fail_non_terminal")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public void failNonTerminalJobs(final ConnectionIdRequestBody connectionIdRequestBody) {
    ApiHelper.execute(() -> {
      jobsHandler.failNonTerminalJobs(connectionIdRequestBody.getConnectionId());
      return null; // to satisfy the lambda interface bounds
    });
  }

  @Post("/get_normalization_status")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public AttemptNormalizationStatusReadList getAttemptNormalizationStatusesForJob(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getAttemptNormalizationStatuses(jobIdRequestBody));
  }

  @Post("/get_check_input")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public Object getCheckInput(final CheckInput checkInput) {
    return ApiHelper.execute(() -> jobInputHandler.getCheckJobInput(checkInput));
  }

  @Post("/get_debug_info")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobDebugInfoRead getJobDebugInfo(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobDebugInfo(jobIdRequestBody));
  }

  @Post("/get")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoRead getJobInfo(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfo(jobIdRequestBody));
  }

  @Post("/get_without_logs")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoRead getJobInfoWithoutLogs(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody));
  }

  @Post("/get_input")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public Object getJobInput(final SyncInput syncInput) {
    return ApiHelper.execute(() -> jobInputHandler.getJobInput(syncInput));
  }

  @Post("/get_light")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoLightRead getJobInfoLight(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfoLight(jobIdRequestBody));
  }

  @Post("/get_last_replication_job")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobOptionalRead getLastReplicationJob(final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getLastReplicationJob(connectionIdRequestBody));
  }

  @Post("/job_failure")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public InternalOperationResult jobFailure(final JobFailureRequest jobFailureRequest) {
    return ApiHelper.execute(() -> jobsHandler.jobFailure(jobFailureRequest));
  }

  @POST
  @Path("/job_success_with_attempt_number")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public InternalOperationResult jobSuccessWithAttemptNumber(final JobSuccessWithAttemptNumberRequest jobSuccessWithAttemptNumberRequest) {
    return ApiHelper.execute(() -> jobsHandler.jobSuccessWithAttemptNumber(jobSuccessWithAttemptNumberRequest));
  }

  @Post("/list")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobReadList listJobsFor(final JobListRequestBody jobListRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.listJobsFor(jobListRequestBody));
  }

  @Post("/list_for_workspaces")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobReadList listJobsForWorkspaces(final JobListForWorkspacesRequestBody requestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.listJobsForWorkspaces(requestBody));
  }

  @Post("/reportJobStart")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public InternalOperationResult reportJobStart(final ReportJobStartRequest reportJobStartRequest) {
    return ApiHelper.execute(() -> jobsHandler.reportJobStart(reportJobStartRequest.getJobId()));
  }

  @Override
  @Post(uri = "/did_previous_job_succeed")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({ADMIN})
  public BooleanRead didPreviousJobSucceed(final ConnectionJobRequestBody requestBody) {
    return ApiHelper.execute(() -> jobsHandler.didPreviousJobSucceed(
        requestBody.getConnectionId(),
        requestBody.getJobId()));
  }

  @Override
  @Post("/persist_cancellation")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void persistJobCancellation(final PersistCancelJobRequestBody requestBody) {
    ApiHelper.execute(() -> {
      jobsHandler.persistJobCancellation(requestBody.getConnectionId(), requestBody.getJobId(), requestBody.getAttemptNumber(),
          requestBody.getAttemptFailureSummary());
      return null;
    });
  }

  @Override
  @Post("/delete_stream_reset_records")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void deleteStreamResetRecordsForJob(final DeleteStreamResetRecordsForJobRequest requestBody) {
    ApiHelper.execute(() -> {
      streamResetRecordsHelper.deleteStreamResetRecordsForJob(requestBody.getJobId(), requestBody.getConnectionId());
      return null;
    });
  }

}
