/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.scim

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.node.ObjectNode

data class ScimPatchRequest
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  constructor(
    @get:JsonValue
    val body: ObjectNode,
  )
