/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.route

import io.micronaut.aop.Around
import kotlin.reflect.KClass

/**
 * Migration inventory metadata, not an authorization grant. Keep existing checks on LEGACY routes.
 * Declare each implementation method, including overrides; controller defaults would hide new routes.
 * SHARED names its operation rule directly; the default represents an absent rule for LEGACY.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@Around
annotation class AuthorizeOperation(
  val access: RouteAccessPolicy,
  val migration: RouteMigration,
  val rule: KClass<out OperationRule> = OperationRule::class,
)

enum class RouteAccessPolicy { PUBLIC, LOGIN, PROTECTED }

enum class RouteMigration { NOT_APPLICABLE, LEGACY, SHARED }
