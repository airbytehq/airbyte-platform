/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class ContextAppenderTest {
  @Test
  fun `verify it enriches Multi with the given contexts`() {
    val contextsToAppend =
      listOf(
        DataplaneGroup(key = "group-1"),
        Dataplane(key = "dataplane-1"),
      )
    val interceptor = ContextAppender(contexts = contextsToAppend)

    val multi = Multi(setOf(Connection(UUID.randomUUID()), Workspace(UUID.randomUUID())))
    val actualContext = interceptor.intercept(multi)

    assertTrue(actualContext is Multi)
    val contexts = actualContext.contexts
    assertTrue(contexts.containsAll(multi.contexts))
    assertTrue(contexts.containsAll(contextsToAppend))

    // This should verify that we didn't mutate the multi passed on the input
    assertNotEquals(multi.toLDContext(), actualContext.toLDContext())
  }

  @Test
  fun `verify it creates a multi from as single context`() {
    val contextsToAppend =
      listOf(
        DataplaneGroup(key = "group-1"),
        Dataplane(key = "dataplane-1"),
      )
    val interceptor = ContextAppender(contexts = contextsToAppend)

    val context = Connection(UUID.randomUUID())
    val actualContext = interceptor.intercept(context)

    assertTrue(actualContext is Multi)
    val contexts = actualContext.contexts
    assertTrue(contexts.contains(context))
    assertTrue(contexts.containsAll(contextsToAppend))
  }
}
