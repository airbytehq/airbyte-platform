/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import org.jooq.Condition;

public class ConditionsHelper {

  public static Condition[] addAll(final Condition[] conditions, final Condition... conditionsToAdd) {
    final Condition[] joinedArray = new Condition[conditions.length + conditionsToAdd.length];

    System.arraycopy(conditions, 0, joinedArray, 0, conditions.length);
    System.arraycopy(conditionsToAdd, 0, joinedArray, conditions.length, conditionsToAdd.length);

    return joinedArray;
  }

}
