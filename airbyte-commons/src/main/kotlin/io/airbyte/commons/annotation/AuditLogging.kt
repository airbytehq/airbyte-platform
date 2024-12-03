package io.airbyte.commons.annotation

import io.micronaut.aop.Around

/**
 * Annotation to be used to enable audit logging for a method.
 *
 * @param provider the name of the audit provider to use.
 *
 * Example usage:
 * When you want to enable audit-logging for a function:
 * 1. Apply this annotation to a function, provider name can be the same as function name.
 * ```
 * @AuditLogging(provider = "createPermission")
 * public PermissionRead createPermission(@Body final PermissionCreate permissionCreate) {
 *   ...
 *   // return API response
 * }
 * ```
 * 2. Create a bean to implement the provider interface. Make sure the bean is named with the same name as the provider name.
 * ```
 * @Singleton
 * @Named("createPermission")
 * class CreatePermissionAuditProvider() : AuditProvider {
 *   ...
 *   // Generate audit info summary
 * }
 * ```
 */
@Around
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
annotation class AuditLogging(val provider: String)
