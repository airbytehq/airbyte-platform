package io.airbyte.commons.auth

import io.airbyte.commons.auth.config.AuthConfigs
import io.airbyte.commons.auth.config.AuthMode
import io.micronaut.context.annotation.Requires
import io.micronaut.context.condition.Condition
import io.micronaut.context.condition.ConditionContext

/**
 * Annotation used to mark a bean that requires a specific [AuthMode] to be active in order to be loaded.
 *
 * Example usage:
 * ```
 * @RequiresAuthMode(AuthMode.OIDC)
 * @Singleton
 * class AuthServiceOidcImpl : AuthService {
 *  // ...
 *  }
 *  ```
 */
@Requires(condition = AuthModeCondition::class)
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class RequiresAuthMode(val value: AuthMode)

/**
 * Condition that powers the [RequiresAuthMode] annotation.
 */
class AuthModeCondition : Condition {
  override fun matches(context: ConditionContext<*>): Boolean {
    val annotationMetadata =
      context.component.annotationMetadata
        ?: throw IllegalStateException("AuthModeCondition can only be used with annotated beans.")

    val authModeFromAnnotation =
      annotationMetadata.enumValue(RequiresAuthMode::class.java, AuthMode::class.java)
        .orElseThrow { IllegalStateException("RequiresAuthMode annotation must have a value in order to be used with AuthModeCondition.") }

    val currentAuthMode = context.getBean(AuthConfigs::class.java).authMode

    return authModeFromAnnotation == currentAuthMode
  }
}
