/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.DataplaneCreateResponse
import io.airbyte.publicApi.server.generated.models.DataplaneCreateResponseBody

object DataplaneCreateResponseMapper {
  fun from(dataplaneCreateResponse: DataplaneCreateResponse?): DataplaneCreateResponseBody? {
    if (dataplaneCreateResponse == null) return null

    return DataplaneCreateResponseBody(
      regionId = dataplaneCreateResponse.regionId,
      dataplaneId = dataplaneCreateResponse.dataplaneId,
      clientId = dataplaneCreateResponse.clientId,
      clientSecret = dataplaneCreateResponse.clientSecret,
    )
  }
}
