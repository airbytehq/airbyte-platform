package io.airbyte.api.server.controllers

import io.airbyte.api.server.constants.ROOT_PATH
import io.airbyte.api.server.controllers.interfaces.DefaultApi
import io.micronaut.context.annotation.Value
import io.micronaut.http.annotation.Controller
import jakarta.ws.rs.core.Response
import java.net.URI

@Controller(ROOT_PATH)
open class DefaultController() : DefaultApi {
  @Value("\${airbyte.internal.documentation.host}")
  var documentationHost: String? = null

  override fun getDocumentation(
    authorization: String?,
    userInfo: String?,
  ): Response {
    return Response
      .status(302)
      .location(URI.create(documentationHost))
      .build()
  }
}
