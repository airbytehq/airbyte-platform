/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license.condition

import io.airbyte.commons.license.ActiveAirbyteLicense
import io.micronaut.context.condition.Condition
import io.micronaut.context.condition.ConditionContext
import io.micronaut.kotlin.context.findBean

/**
 * Condition that checks if the Airbyte License is a PRO license. Used to conditionally activate
 * beans that should only be activated for verified installations of Airbyte Pro.
 */
class VerifiedProLicenseCondition : Condition {
  override fun matches(context: ConditionContext<*>): Boolean = context.findBean<ActiveAirbyteLicense>()?.isPro ?: false
}
