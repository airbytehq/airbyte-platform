/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.domain

import io.swagger.v3.oas.annotations.media.Schema

data class DataplaneLogUploadAuthorizationRequest(
  @Schema(required = true)
  var dataplaneId: String = "",
)
