/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license.condition;

import io.airbyte.commons.license.ActiveAirbyteLicense;
import io.micronaut.context.condition.Condition;
import io.micronaut.context.condition.ConditionContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Condition that checks if the Airbyte License is a PRO license. Used to conditionally activate
 * beans that should only be activated for verified installations of Airbyte Pro.
 */
@Slf4j
public class VerifiedProLicenseCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context) {
    final Optional<ActiveAirbyteLicense> activeAirbyteLicense = context.findBean(ActiveAirbyteLicense.class);

    return activeAirbyteLicense.map(ActiveAirbyteLicense::isPro).orElse(false);
  }

}
