/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.annotations

/**
 * Denotes a field in a Temporal workflow that represents a Temporal activity stub. Fields marked
 * with this annotation will automatically have a Temporal activity stub created, if not already
 * initialized when execution of the Temporal workflow starts.
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class TemporalActivityStub(
  /**
   * The name of the singleton bean that holds the Temporal activity options for the Temporal activity
   * stub annotated by this annotation. This bean must exist in the application context.
   *
   * @return The name of the singleton bean that holds the Temporal activity options for that Temporal
   * activity stub.
   */
  val activityOptionsBeanName: String,
)
