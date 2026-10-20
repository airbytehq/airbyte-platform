/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.route

/**
 * Migration inventory metadata, not an authorization grant. Keep existing checks on LEGACY routes.
 * Declare each implementation method, including overrides; controller defaults would hide new routes.
 * SHARED is reserved and rejected until the shared checker and business guard are implemented.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class AuthorizeOperation(
  val access: RouteAccessPolicy,
  val migration: RouteMigration,
)

enum class RouteAccessPolicy { PUBLIC, LOGIN, PROTECTED }

enum class RouteMigration { NOT_APPLICABLE, LEGACY, SHARED }
