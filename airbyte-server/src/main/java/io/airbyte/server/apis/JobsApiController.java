/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.JobsApi;
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
import io.airbyte.commons.auth.generated.Intent;
import io.airbyte.commons.auth.permissions.RequiresIntent;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.JobInputHandler;
import io.airbyte.commons.server.handlers.JobsHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.temporal.StreamResetRecordsHelper;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

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
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @Override
  public JobInfoRead cancelJob(@Body final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.cancelJob(jobIdRequestBody));
  }

  @Post("/create")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @Override
  public JobInfoRead createJob(@Body final JobCreate jobCreate) {
    return ApiHelper.execute(() -> schedulerHandler.createJob(jobCreate));
  }

  @Post("/fail_non_terminal")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public void failNonTerminalJobs(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    ApiHelper.execute(() -> {
      jobsHandler.failNonTerminalJobs(connectionIdRequestBody.getConnectionId());
      return null; // to satisfy the lambda interface bounds
    });
  }

  @Post("/get_check_input")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public Object getCheckInput(@Body final CheckInput checkInput) {
    return ApiHelper.execute(() -> jobInputHandler.getCheckJobInput(checkInput));
  }

  @Post("/get_debug_info")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobDebugInfoRead getJobDebugInfo(@Body final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobDebugInfo(jobIdRequestBody));
  }

  @Post("/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoRead getJobInfo(@Body final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfo(jobIdRequestBody));
  }

  @Post("/get_without_logs")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoRead getJobInfoWithoutLogs(@Body final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody));
  }

  @Post("/get_input")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public Object getJobInput(@Body final SyncInput syncInput) {
    return ApiHelper.execute(() -> jobInputHandler.getJobInput(syncInput));
  }

  @Post("/get_light")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoLightRead getJobInfoLight(@Body final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfoLight(jobIdRequestBody));
  }

  @Post("/get_last_replication_job")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobOptionalRead getLastReplicationJob(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getLastReplicationJob(connectionIdRequestBody));
  }

  @Post("/get_last_replication_job_with_cancel")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobOptionalRead getLastReplicationJobWithCancel(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getLastReplicationJobWithCancel(connectionIdRequestBody));
  }

  @Post("/job_failure")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public InternalOperationResult jobFailure(@Body final JobFailureRequest jobFailureRequest) {
    return ApiHelper.execute(() -> jobsHandler.jobFailure(jobFailureRequest));
  }

  @POST
  @Path("/job_success_with_attempt_number")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public InternalOperationResult jobSuccessWithAttemptNumber(@Body final JobSuccessWithAttemptNumberRequest jobSuccessWithAttemptNumberRequest) {
    return ApiHelper.execute(() -> jobsHandler.jobSuccessWithAttemptNumber(jobSuccessWithAttemptNumberRequest));
  }

  @Post("/list")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobReadList listJobsFor(@Body final JobListRequestBody jobListRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.listJobsFor(jobListRequestBody));
  }

  @Post("/list_for_workspaces")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobReadList listJobsForWorkspaces(@Body final JobListForWorkspacesRequestBody requestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.listJobsForWorkspaces(requestBody));
  }

  @Post("/reportJobStart")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public InternalOperationResult reportJobStart(@Body final ReportJobStartRequest reportJobStartRequest) {
    return ApiHelper.execute(() -> jobsHandler.reportJobStart(reportJobStartRequest.getJobId()));
  }

  @Override
  @Post(uri = "/did_previous_job_succeed")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({ADMIN})
  public BooleanRead didPreviousJobSucceed(@Body final ConnectionJobRequestBody requestBody) {
    return ApiHelper.execute(() -> jobsHandler.didPreviousJobSucceed(
        requestBody.getConnectionId(),
        requestBody.getJobId()));
  }

  @Override
  @Post("/persist_cancellation")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void persistJobCancellation(@Body final PersistCancelJobRequestBody requestBody) {
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
  public void deleteStreamResetRecordsForJob(@Body final DeleteStreamResetRecordsForJobRequest requestBody) {
    ApiHelper.execute(() -> {
      streamResetRecordsHelper.deleteStreamResetRecordsForJob(requestBody.getJobId(), requestBody.getConnectionId());
      return null;
    });
  }

}
