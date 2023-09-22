/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.JobsApi;
import io.airbyte.api.model.generated.AttemptNormalizationStatusReadList;
import io.airbyte.api.model.generated.CheckInput;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobCreate;
import io.airbyte.api.model.generated.JobDebugInfoRead;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoLightRead;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody;
import io.airbyte.api.model.generated.JobListRequestBody;
import io.airbyte.api.model.generated.JobOptionalRead;
import io.airbyte.api.model.generated.JobReadList;
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.api.model.generated.SyncInput;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.JobInputHandler;
import io.airbyte.commons.server.handlers.JobsHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@SuppressWarnings("MissingJavadocType")
@Controller("/api/v1/jobs")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class JobsApiController implements JobsApi {

  private final JobHistoryHandler jobHistoryHandler;
  private final SchedulerHandler schedulerHandler;
  private final JobsHandler jobsHandler;
  private final JobInputHandler jobInputHandler;

  public JobsApiController(final JobHistoryHandler jobHistoryHandler,
                           final SchedulerHandler schedulerHandler,
                           final JobInputHandler jobInputHandler,
                           final JobsHandler jobsHandler) {
    this.jobHistoryHandler = jobHistoryHandler;
    this.schedulerHandler = schedulerHandler;
    this.jobInputHandler = jobInputHandler;
    this.jobsHandler = jobsHandler;
  }

  @Post("/cancel")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobInfoRead cancelJob(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.cancelJob(jobIdRequestBody));
  }

  @Post("/create")
  @SecuredWorkspace
  @Secured({EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobInfoRead createJob(final JobCreate jobCreate) {
    return ApiHelper.execute(() -> schedulerHandler.createJob(jobCreate));
  }

  @Post("/fail_non_terminal")
  @SecuredWorkspace
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
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public Object getCheckInput(final CheckInput checkInput) {
    return ApiHelper.execute(() -> jobInputHandler.getCheckJobInput(checkInput));
  }

  @Post("/get_debug_info")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public JobDebugInfoRead getJobDebugInfo(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobDebugInfo(jobIdRequestBody));
  }

  @Post("/get")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoRead getJobInfo(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfo(jobIdRequestBody));
  }

  @Post("/get_without_logs")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoRead getJobInfoWithoutLogs(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody));
  }

  @Post("/get_input")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public Object getJobInput(final SyncInput syncInput) {
    return ApiHelper.execute(() -> jobInputHandler.getJobInput(syncInput));
  }

  @Post("/get_light")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobInfoLightRead getJobInfoLight(final JobIdRequestBody jobIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getJobInfoLight(jobIdRequestBody));
  }

  @Post("/get_last_replication_job")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobOptionalRead getLastReplicationJob(final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getLastReplicationJob(connectionIdRequestBody));
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
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobReadList listJobsFor(final JobListRequestBody jobListRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.listJobsFor(jobListRequestBody));
  }

  @Post("/list_for_workspaces")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public JobReadList listJobsForWorkspaces(final JobListForWorkspacesRequestBody requestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.listJobsForWorkspaces(requestBody));
  }

}
