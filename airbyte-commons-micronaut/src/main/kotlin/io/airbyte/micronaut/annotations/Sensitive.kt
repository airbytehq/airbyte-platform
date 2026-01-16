/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.annotations

import io.micronaut.aop.Around

/**
 * Used to denote that a property whose is sensitive should be read out of a secret.
 *
 * @property secretNameProperty The property name where the secret name should be expected
 * @property secretKeyProperty The property name where the secret key should be expected
 */
@MustBeDocumented
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.FIELD)
@Around
annotation class Sensitive(
  val secretNameProperty: String,
  val secretKeyProperty: String,
)
