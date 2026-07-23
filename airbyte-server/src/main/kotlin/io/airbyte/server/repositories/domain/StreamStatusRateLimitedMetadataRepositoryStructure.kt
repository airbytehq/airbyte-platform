/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

data class StreamStatusRateLimitedMetadataRepositoryStructure
  @JsonCreator
  constructor(
    @JsonProperty("quotaReset") val quotaReset: Long,
  )
