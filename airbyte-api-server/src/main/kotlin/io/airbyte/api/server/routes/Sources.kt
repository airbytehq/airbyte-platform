package io.airbyte.api.server.routes

import io.airbyte.airbyte_api.generated.SourcesApi
import io.airbyte.airbyte_api.model.generated.InitiateOauthRequest
import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.api.server.services.SourceService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.core.Response

@Controller("/v1/sources")
class Sources(sourceService: SourceService) : SourcesApi {
  override fun createSource(sourceCreateRequest: SourceCreateRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun deleteSource(sourceId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun getSource(sourceId: UUID?): Response {
    TODO("Not yet implemented")
  }

  override fun initiateOAuth(initiateOauthRequest: InitiateOauthRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun listSources(workspaceIds: MutableList<UUID>?, includeDeleted: Boolean?, limit: Int?, offset: Int?): Response {
    TODO("Not yet implemented")
  }

  override fun patchSource(sourceId: UUID?, sourcePatchRequest: SourcePatchRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun putSource(sourceId: UUID?, sourcePutRequest: SourcePutRequest?): Response {
    TODO("Not yet implemented")
  }
}
