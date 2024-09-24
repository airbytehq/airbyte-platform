/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.DiagnosticToolApi;
import io.airbyte.api.model.generated.DiagnosticReportRequestBody;
import io.airbyte.commons.server.handlers.DiagnosticToolHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.io.File;

@Controller("/api/v1/diagnostic_tool")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class DiagnosticToolApiController implements DiagnosticToolApi {

  private final DiagnosticToolHandler diagnosticToolHandler;

  public DiagnosticToolApiController(final DiagnosticToolHandler diagnosticToolHandler) {
    this.diagnosticToolHandler = diagnosticToolHandler;
  }

  @Override
  @Post(uri = "/generate_report",
        produces = MediaType.APPLICATION_ZIP)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  public File generateDiagnosticReport(@Body final DiagnosticReportRequestBody diagnosticReportRequestBody) {
    return ApiHelper.execute(diagnosticToolHandler::generateDiagnosticReport);
  }

}
