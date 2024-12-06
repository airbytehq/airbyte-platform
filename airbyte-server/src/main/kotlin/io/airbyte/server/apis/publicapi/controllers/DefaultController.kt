package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.publicApi.server.generated.apis.PublicRootApi
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.micronaut.context.annotation.Value
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.net.URI

@Controller(API_PATH)
@Secured(SecurityRule.IS_ANONYMOUS)
open class DefaultController : PublicRootApi {
  @Value("\${airbyte.internal.documentation.host}")
  var documentationHost: String? = null

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun getDocumentation(): Response =
    Response
      .status(302)
      .location(documentationHost?.let { URI.create(it) })
      .build()
}
