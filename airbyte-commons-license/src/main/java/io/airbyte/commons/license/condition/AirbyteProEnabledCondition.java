/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license.condition;

import io.airbyte.config.Configs.AirbyteEdition;
import io.micronaut.context.condition.Condition;
import io.micronaut.context.condition.ConditionContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Condition that checks if the Airbyte instance is a PRO installation. For now, this condition
 * passes even without a verified license. This is because we don't currently want to block Pro
 * functionality behind a license. This will likely change in the future.
 */
@Slf4j
public class AirbyteProEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context) {
    final AirbyteEdition edition = context.getBean(AirbyteEdition.class);
    return edition.equals(AirbyteEdition.PRO);
  }

}
