/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.helpers.BreakingChangeScopeFactory.createStreamBreakingChangeScope
import io.airbyte.config.helpers.BreakingChangeScopeFactory.validateBreakingChangeScope
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

internal class BreakingChangeScopeFactoryTest {
  @Test
  fun testCreateStreamBreakingChangeScope() {
    val impactedScopes = mutableListOf<Any?>("scope1", "scope2")
    val breakingChangeScope = BreakingChangeScope().withScopeType(BreakingChangeScope.ScopeType.STREAM).withImpactedScopes(impactedScopes)

    val streamBreakingChangeScope = createStreamBreakingChangeScope(breakingChangeScope)

    Assertions.assertNotNull(streamBreakingChangeScope)
    Assertions.assertEquals(impactedScopes, streamBreakingChangeScope.impactedScopes)
  }

  @Test
  fun testValidateBreakingChangeScopeWithValidScope() {
    val impactedScopes = mutableListOf<Any?>("scope1", "scope2")
    val breakingChangeScope = BreakingChangeScope().withScopeType(BreakingChangeScope.ScopeType.STREAM).withImpactedScopes(impactedScopes)
    Assertions.assertDoesNotThrow(Executable { validateBreakingChangeScope(breakingChangeScope) })
  }

  @Test
  fun testValidateBreakingChangeScopeWithInvalidImpactedScopes() {
    val impactedScopes = mutableListOf<Any?>("scope1", 123)
    val breakingChangeScope = BreakingChangeScope().withScopeType(BreakingChangeScope.ScopeType.STREAM).withImpactedScopes(impactedScopes)

    val exception =
      Assertions.assertThrows<IllegalArgumentException>(
        IllegalArgumentException::class.java,
        Executable { validateBreakingChangeScope(breakingChangeScope) },
      )
    Assertions.assertEquals("All elements in the impactedScopes array must be strings.", exception.message)
  }

  @Test
  fun testValidateBreakingChangeScopeWithNullScopeType() {
    Assertions.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      Executable { BreakingChangeScopeFactory.validateBreakingChangeScope(BreakingChangeScope()) },
    )
  }

  @Test
  fun testValidateBreakingChangeScopeWithEmptyScope() {
    Assertions.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      Executable { validateBreakingChangeScope(BreakingChangeScope()) },
    )
  }
}
