/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import io.airbyte.config.BreakingChangeScope.ScopeType;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class StreamBreakingChangeScope {

  private static final ScopeType scopeType = ScopeType.STREAM;
  private List<String> impactedScopes = new ArrayList<String>();

  public ScopeType getScopeType() {
    return scopeType;
  }

  public StreamBreakingChangeScope withImpactedScopes(final List<String> impactedScopes) {
    this.impactedScopes = impactedScopes;
    return this;
  }

}
