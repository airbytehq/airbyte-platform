package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.airbyte_api.model.generated.SourceResponse
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaRead
import java.util.UUID
import javax.validation.constraints.NotBlank

interface SourceService {
  fun createSource(
    sourceCreateRequest: @NotBlank SourceCreateRequest,
    sourceDefinitionId: @NotBlank UUID,
    userInfo: String,
  ): SourceResponse

  fun updateSource(sourceId: UUID, sourcePutRequest: SourcePutRequest, userInfo: String): SourceResponse

  fun partialUpdateSource(sourceId: UUID, sourcePatchRequest: SourcePatchRequest, userInfo: String): SourceResponse

  fun deleteSource(sourceId: @NotBlank UUID, userInfo: String)

  fun getSource(sourceId: @NotBlank UUID, userInfo: String): SourceResponse

  fun getSourceSchema(sourceId: UUID, disableCache: Boolean, userInfo: String): SourceDiscoverSchemaRead
}
