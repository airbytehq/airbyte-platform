/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import com.google.common.base.Preconditions;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class BreakingChangeScopeFactory {

  public static StreamBreakingChangeScope createStreamBreakingChangeScope(final BreakingChangeScope breakingChangeScope) {
    validateBreakingChangeScope(breakingChangeScope);
    final List<String> impactedScopes = breakingChangeScope.getImpactedScopes().stream().map(impactedScope -> (String) impactedScope).toList();
    return new StreamBreakingChangeScope().withImpactedScopes(impactedScopes);
  }

  public static void validateBreakingChangeScope(final BreakingChangeScope breakingChangeScope) {
    Objects.requireNonNull(breakingChangeScope, "breakingChangeScope cannot be null");

    switch (breakingChangeScope.getScopeType()) {
      case STREAM:
        Preconditions.checkArgument(
            breakingChangeScope.getImpactedScopes().stream().allMatch(element -> element instanceof String),
            "All elements in the impactedScopes array must be strings.");
        break;
      // Add cases for other scope types as necessary
      default:
        throw new IllegalArgumentException(
            String.format("Invalid scopeType: %s is not supported. Expected types: %s",
                breakingChangeScope.getScopeType(), Arrays.toString(ScopeType.values())));
    }
  }

}
