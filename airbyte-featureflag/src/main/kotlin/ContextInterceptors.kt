/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

/**
 * Defines a hook supported by some client implementation in order to enrich the context being evaluated.
 */
interface ContextInterceptor {
  fun intercept(context: Context): Context
}

/**
 * ContextInterceptor that will append [contexts] to all context intercepted.
 */
data class ContextAppender(
  private val contexts: List<Context>,
) : ContextInterceptor {
  override fun intercept(context: Context): Context =
    when (context) {
      is Multi -> Multi(contexts = context.contexts.plus(contexts))
      else -> Multi(contexts = setOf(context).plus(contexts))
    }
}
