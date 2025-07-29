/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

/**
 * Defines a hook supported by some client implementations to enrich or modify the context
 * before it's used for feature flag evaluation.
 *
 * This allows for dynamic context enrichment, such as:
 * - Adding organization context when only workspace context is provided
 * - Appending user attributes from external systems
 * - Adding infrastructure-specific context (datacenter, region, etc.)
 *
 * Currently supported by [LaunchDarklyClient] via [LaunchDarklyClient.registerContextInterceptor].
 */
interface ContextInterceptor {
  /**
   * Modifies or enriches the provided context before feature flag evaluation.
   *
   * @param context The original context provided for flag evaluation
   * @return The modified context that will be used for actual flag evaluation
   */
  fun intercept(context: Context): Context
}

/**
 * A concrete [ContextInterceptor] implementation that appends additional contexts
 * to the provided context before feature flag evaluation.
 *
 * This is useful for automatically adding common context that should be present
 * for all flag evaluations, such as:
 * - Current datacenter or region information
 * - Default organization context
 * - System-level attributes
 *
 * @param contexts The list of contexts to append to every intercepted context
 */
data class ContextAppender(
  private val contexts: List<Context>,
) : ContextInterceptor {
  /**
   * Appends the configured contexts to the provided context.
   *
   * If the input context is already a [Multi], the additional contexts are added to it.
   * If the input is a single context, it's converted to a [Multi] with the additional contexts.
   *
   * @param context The original context to enrich
   * @return A [Multi] context containing both the original and additional contexts
   */
  override fun intercept(context: Context): Context =
    when (context) {
      is Multi -> Multi(contexts = context.contexts.plus(contexts))
      else -> Multi(contexts = setOf(context).plus(contexts))
    }
}
