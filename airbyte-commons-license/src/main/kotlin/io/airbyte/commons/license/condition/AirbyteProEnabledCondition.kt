/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license.condition

import io.airbyte.config.Configs.AirbyteEdition
import io.micronaut.context.condition.Condition
import io.micronaut.context.condition.ConditionContext
import io.micronaut.kotlin.context.getBean

/**
 * Condition that checks if the Airbyte instance is a PRO installation. For now, this condition
 * passes even without a verified license. This is because we don't currently want to block Pro
 * functionality behind a license. This will likely change in the future.
 */
class AirbyteProEnabledCondition : Condition {
  override fun matches(context: ConditionContext<*>): Boolean = context.getBean<AirbyteEdition>() == AirbyteEdition.PRO
}
