/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification.slack

import com.fasterxml.jackson.databind.JsonNode

interface Block {
  fun toJsonNode(): JsonNode
}
