/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.BreakingChangeScope

data class StreamBreakingChangeScope(
  val impactedScopes: List<String> = emptyList(),
) {
  fun getScopeType(): BreakingChangeScope.ScopeType = scopeType

  companion object {
    private val scopeType = BreakingChangeScope.ScopeType.STREAM
  }
}
