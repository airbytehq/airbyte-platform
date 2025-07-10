/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import org.jooq.Condition

object ConditionsHelper {
  fun addAll(vararg conditionsToAdd: Condition): Array<Condition> = conditionsToAdd.toList().toTypedArray()
}
