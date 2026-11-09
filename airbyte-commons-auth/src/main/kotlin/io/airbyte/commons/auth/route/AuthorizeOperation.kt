/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.route

import io.micronaut.aop.Around
import kotlin.reflect.KClass

/**
 * Protected operations name the rule that runs before controller work.
 * Declare each implementation method, including overrides; controller defaults would hide new routes.
 * Public/login operations have no rule.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@Around
annotation class AuthorizeOperation(
  val access: RouteAccessPolicy,
  val rule: KClass<out OperationRule> = OperationRule::class,
)

enum class RouteAccessPolicy { PUBLIC, LOGIN, PROTECTED }
