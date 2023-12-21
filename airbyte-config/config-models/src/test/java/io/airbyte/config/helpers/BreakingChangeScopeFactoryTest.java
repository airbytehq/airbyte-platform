/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.*;

import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BreakingChangeScopeFactoryTest {

  @Test
  void testCreateStreamBreakingChangeScope() {
    final List<Object> impactedScopes = List.of("scope1", "scope2");
    final BreakingChangeScope breakingChangeScope = new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(impactedScopes);

    final StreamBreakingChangeScope streamBreakingChangeScope = BreakingChangeScopeFactory.createStreamBreakingChangeScope(breakingChangeScope);

    assertNotNull(streamBreakingChangeScope);
    assertEquals(impactedScopes, streamBreakingChangeScope.getImpactedScopes());
  }

  @Test
  void testValidateBreakingChangeScopeWithValidScope() {
    final List<Object> impactedScopes = List.of("scope1", "scope2");
    final BreakingChangeScope breakingChangeScope = new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(impactedScopes);
    assertDoesNotThrow(() -> BreakingChangeScopeFactory.validateBreakingChangeScope(breakingChangeScope));
  }

  @Test
  void testValidateBreakingChangeScopeWithInvalidImpactedScopes() {
    final List<Object> impactedScopes = List.of("scope1", 123);
    final BreakingChangeScope breakingChangeScope = new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(impactedScopes);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> BreakingChangeScopeFactory.validateBreakingChangeScope(breakingChangeScope));
    assertEquals("All elements in the impactedScopes array must be strings.", exception.getMessage());
  }

  @Test
  void testValidateBreakingChangeScopeWithNullScope() {
    assertThrows(NullPointerException.class, () -> BreakingChangeScopeFactory.validateBreakingChangeScope(null));
  }

  @Test
  void testValidateBreakingChangeScopeWithEmptyScope() {
    assertThrows(NullPointerException.class, () -> BreakingChangeScopeFactory.validateBreakingChangeScope(new BreakingChangeScope()));
  }

}
