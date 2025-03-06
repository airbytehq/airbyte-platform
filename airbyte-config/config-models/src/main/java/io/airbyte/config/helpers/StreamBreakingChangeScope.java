/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.config.BreakingChangeScope.ScopeType;
import java.util.ArrayList;
import java.util.List;

public class StreamBreakingChangeScope {

  private static final ScopeType scopeType = ScopeType.STREAM;
  private List<String> impactedScopes = new ArrayList<>();

  public ScopeType getScopeType() {
    return scopeType;
  }

  public StreamBreakingChangeScope withImpactedScopes(final List<String> impactedScopes) {
    this.impactedScopes = impactedScopes;
    return this;
  }

  public List<String> getImpactedScopes() {
    return impactedScopes;
  }

}
