package io.airbyte.api.server.services.impls

import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.airbyte_api.model.generated.SourceResponse
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import io.airbyte.api.server.services.SourceService
import java.util.UUID

class SourceServiceImpl : SourceService {
  override fun createSource(sourceCreateRequest: SourceCreateRequest, sourceDefinitionId: UUID, userInfo: String): SourceResponse {
    TODO("Not yet implemented")
  }

  override fun updateSource(sourceId: UUID, sourcePutRequest: SourcePutRequest, userInfo: String): SourceResponse {
    TODO("Not yet implemented")
  }

  override fun partialUpdateSource(sourceId: UUID, sourcePatchRequest: SourcePatchRequest, userInfo: String): SourceResponse {
    TODO("Not yet implemented")
  }

  override fun deleteSource(sourceId: UUID, userInfo: String) {
    TODO("Not yet implemented")
  }

  override fun getSource(sourceId: UUID, userInfo: String): SourceResponse {
    TODO("Not yet implemented")
  }

  override fun getSourceSchema(
    sourceId: UUID,
    disableCache: Boolean,
    userInfo: String,
  ): SourceDiscoverSchemaRead {
    TODO("Not yet implemented")
  }
}
