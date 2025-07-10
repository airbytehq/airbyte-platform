/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.temporal

import io.temporal.activity.ActivityOptions

/**
 * Functional interface that defines the function used to generate a Temporal activity stub.
 *
 * @param <C> The Temporal activity stub class.
 * @param <A> The [ActivityOptions] for the Temporal activity stub.
 * @param <O> The Temporal activity stub object.
 */
fun interface TemporalActivityStubGeneratorFunction<C : Class<*>, A : ActivityOptions, O> {
  fun apply(
    c: C,
    a: A,
  ): O
}
