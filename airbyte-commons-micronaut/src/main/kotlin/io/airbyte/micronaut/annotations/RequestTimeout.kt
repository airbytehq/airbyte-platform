/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.annotations

import io.micronaut.aop.Around

/**
 * Used to denote that a method should complete in the specified amount of time.
 *
 * @property timeout The timeout value as an ISO-8601 duration format.  Defaults to "PT30S" (30 seconds)
 */
@MustBeDocumented
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
@Around
annotation class RequestTimeout(
  val timeout: String = "PT30S",
)
