/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.transformmodels

import com.fasterxml.jackson.databind.JsonNode

data class AddFieldTransform(
  val schema: JsonNode,
)
