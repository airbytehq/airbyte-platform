/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.auth.permissions

import io.airbyte.commons.auth.generated.Intent

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
annotation class RequiresIntent(
  val value: Intent,
)
