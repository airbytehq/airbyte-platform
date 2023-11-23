/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.AttemptApi;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.CreateNewAttemptNumberRequest;
import io.airbyte.api.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.model.generated.FailAttemptRequest;
import io.airbyte.api.model.generated.GetAttemptStatsRequestBody;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SaveStatsRequestBody;
import io.airbyte.api.model.generated.SetWorkflowInAttemptRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.AttemptHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/attempt")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class AttemptApiController implements AttemptApi {

  private final AttemptHandler attemptHandler;

  public AttemptApiController(final AttemptHandler attemptHandler) {
    this.attemptHandler = attemptHandler;
  }

  @Override
  @Post(uri = "/get_for_job",
        processes = MediaType.APPLICATION_JSON)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  public AttemptInfoRead getAttemptForJob(final GetAttemptStatsRequestBody requestBody) {
    return ApiHelper
        .execute(() -> attemptHandler.getAttemptForJob(requestBody.getJobId(), requestBody.getAttemptNumber()));
  }

  @Override
  @Post(uri = "/create_new_attempt_number",
        processes = MediaType.APPLICATION_JSON)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({ADMIN})
  public CreateNewAttemptNumberResponse createNewAttemptNumber(final CreateNewAttemptNumberRequest requestBody) {
    return ApiHelper
        .execute(() -> attemptHandler.createNewAttemptNumber(requestBody.getJobId()));
  }

  @Override
  @Post(uri = "/get_combined_stats",
        processes = MediaType.APPLICATION_JSON)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  public AttemptStats getAttemptCombinedStats(final GetAttemptStatsRequestBody requestBody) {
    return ApiHelper
        .execute(() -> attemptHandler.getAttemptCombinedStats(requestBody.getJobId(), requestBody.getAttemptNumber()));
  }

  @Override
  @Post(uri = "/save_stats",
        processes = MediaType.APPLICATION_JSON)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public InternalOperationResult saveStats(final SaveStatsRequestBody requestBody) {
    return ApiHelper.execute(() -> attemptHandler.saveStats(requestBody));
  }

  @Override
  @Post(uri = "/set_workflow_in_attempt",
        processes = MediaType.APPLICATION_JSON)
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public InternalOperationResult setWorkflowInAttempt(@Body final SetWorkflowInAttemptRequestBody requestBody) {
    return ApiHelper.execute(() -> attemptHandler.setWorkflowInAttempt(requestBody));
  }

  @Override
  @Post(uri = "/save_sync_config",
        processes = MediaType.APPLICATION_JSON)
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public InternalOperationResult saveSyncConfig(@Body final SaveAttemptSyncConfigRequestBody requestBody) {
    return ApiHelper.execute(() -> attemptHandler.saveSyncConfig(requestBody));
  }

  @Override
  @Post(uri = "/fail",
        processes = MediaType.APPLICATION_JSON)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({ADMIN})
  public void failAttempt(final FailAttemptRequest requestBody) {
    ApiHelper.execute(() -> {
      attemptHandler.failAttempt(
          requestBody.getAttemptNumber(),
          requestBody.getJobId(),
          requestBody.getFailureSummary(),
          requestBody.getStandardSyncOutput());
      return null;
    });

  }

}
