/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis

import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.api.server.generated.apis.DiagnosticToolApi
import io.airbyte.api.server.generated.models.DiagnosticReportRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.DiagnosticToolHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.validation.Valid
import java.io.File

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class DiagnosticToolApiController(
  private val diagnosticToolHandler: DiagnosticToolHandler,
) : DiagnosticToolApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DiagnosticReportRule::class,
  )
  override fun generateDiagnosticReport(
    @Body @Valid diagnosticReportRequestBody: DiagnosticReportRequestBody?,
  ): File {
    diagnosticReportRequestBody.requireBody()
    return execute { diagnosticToolHandler.generateDiagnosticReport() }
      ?: throw UnexpectedProblem("The diagnostic report could not be generated. Retry; if it keeps failing, contact support.", null)
  }
}
