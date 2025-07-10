/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.google.common.base.Preconditions
import io.airbyte.config.BreakingChangeScope

object BreakingChangeScopeFactory {
  @JvmStatic
  fun createStreamBreakingChangeScope(breakingChangeScope: BreakingChangeScope): StreamBreakingChangeScope {
    validateBreakingChangeScope(breakingChangeScope)
    val impactedScopes =
      breakingChangeScope.impactedScopes
        .mapNotNull { it as? String }
        .toList()
    return StreamBreakingChangeScope(impactedScopes)
  }

  @JvmStatic
  fun validateBreakingChangeScope(breakingChangeScope: BreakingChangeScope) {
    when (breakingChangeScope.scopeType) {
      BreakingChangeScope.ScopeType.STREAM ->
        Preconditions.checkArgument(
          breakingChangeScope.impactedScopes.all { it is String },
          "All elements in the impactedScopes array must be strings.",
        )

      else -> throw IllegalArgumentException(
        "Invalid scopeType: ${breakingChangeScope.scopeType} is not supported. Expected types: ${BreakingChangeScope.ScopeType.entries.joinToString()}",
      )
    }
  }
}
