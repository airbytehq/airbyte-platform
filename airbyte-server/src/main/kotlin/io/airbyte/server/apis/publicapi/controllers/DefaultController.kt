/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.micronaut.runtime.AirbyteInternalDocumentationConfig
import io.airbyte.publicApi.server.generated.apis.PublicRootApi
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.net.URI

@Controller(API_PATH)
@Secured(SecurityRule.IS_ANONYMOUS)
open class DefaultController(
  private val airbyteInternalDocumentationConfig: AirbyteInternalDocumentationConfig,
) : PublicRootApi {
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun getDocumentation(): Response =
    Response
      .status(302)
      .location(if (airbyteInternalDocumentationConfig.host.isNotBlank()) URI.create(airbyteInternalDocumentationConfig.host) else null)
      .build()
}
