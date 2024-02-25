package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.public_api.generated.PublicRootApi
import io.airbyte.server.apis.publicapi.constants.ROOT_PATH
import io.micronaut.context.annotation.Value
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import java.net.URI
import javax.ws.rs.core.Response

@Controller(ROOT_PATH)
open class DefaultController() : PublicRootApi {
  @Value("\${airbyte.internal.documentation.host}")
  var documentationHost: String? = null

  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getDocumentation(): Response {
    return Response
      .status(302)
      .location(URI.create(documentationHost))
      .build()
  }
}
