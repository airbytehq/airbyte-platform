/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID

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

    val actualMulti = assertInstanceOf(Multi::class.java, actualContext)
    val contexts = actualMulti.contexts
    assertTrue(contexts.containsAll(multi.contexts))
    assertTrue(contexts.containsAll(contextsToAppend))

    // This should verify that we didn't mutate the multi passed on the input
    assertNotEquals(multi.toLDContext(), actualMulti.toLDContext())
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

    val actualMulti = assertInstanceOf(Multi::class.java, actualContext)
    val contexts = actualMulti.contexts
    assertTrue(contexts.contains(context))
    assertTrue(contexts.containsAll(contextsToAppend))
  }
}
