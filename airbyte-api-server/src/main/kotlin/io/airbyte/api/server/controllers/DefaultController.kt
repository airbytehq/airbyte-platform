package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.generated.DefaultApi
import io.airbyte.api.server.constants.ROOT_PATH
import io.micronaut.context.annotation.Value
import io.micronaut.http.annotation.Controller
import java.net.URI
import javax.ws.rs.core.Response

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
