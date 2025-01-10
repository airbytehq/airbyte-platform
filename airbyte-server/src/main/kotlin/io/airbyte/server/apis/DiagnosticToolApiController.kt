/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis

import io.airbyte.api.generated.DiagnosticToolApi
import io.airbyte.api.model.generated.DiagnosticReportRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.DiagnosticToolHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.validation.Valid
import java.io.File

@Controller("/api/v1/diagnostic_tool")
@Secured(SecurityRule.IS_AUTHENTICATED)
class DiagnosticToolApiController(
  private val diagnosticToolHandler: DiagnosticToolHandler,
) : DiagnosticToolApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun generateDiagnosticReport(
    @Body @Valid diagnosticReportRequestBody: DiagnosticReportRequestBody,
  ): File? = execute { diagnosticToolHandler.generateDiagnosticReport() }
}
