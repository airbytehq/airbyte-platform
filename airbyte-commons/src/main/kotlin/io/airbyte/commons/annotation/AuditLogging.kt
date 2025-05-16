/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.annotation

import io.micronaut.aop.Around

/**
 * Annotation to be used to enable audit logging for a method.
 *
 * @param provider the name of the audit provider to use.
 *
 * Example usage:
 * When you want to enable audit-logging for a function:
 * 1. Choose a provider name from `AuditLoggingProvider`. If you need a new one, add it to the `AuditLoggingProvider` class.
 * 2. Apply "AuditLogging" annotation to a function with a proper provider name.
 * ```
 * @AuditLogging(provider = AuditLoggingProvider.CREATE_PERMISSION)
 * public PermissionRead createPermission(@Body final PermissionCreate permissionCreate) {
 *   ...
 *   // return API response
 * }
 * ```
 * 2. Create an audit provider bean to implement the provider interface. Make sure the bean name is matched with provider name.
 * ```
 * @Singleton
 * @Named(AuditLoggingProvider.CREATE_PERMISSION)
 * class CreatePermissionAuditProvider() : AuditProvider {
 *   ...
 *   // Put your customized logic to generate audit info summary from the API request and response.
 * }
 * ```
 */
@Around
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
annotation class AuditLogging(
  val provider: String,
)

class AuditLoggingProvider {
  companion object {
    const val BASIC = "basicAudit"
    const val ONLY_ACTOR = "onlyActorAudit"
    const val CREATE_PERMISSION = "createPermission"
    const val UPDATE_PERMISSION = "updatePermission"
    const val DELETE_PERMISSION = "deletePermission"
  }
}
