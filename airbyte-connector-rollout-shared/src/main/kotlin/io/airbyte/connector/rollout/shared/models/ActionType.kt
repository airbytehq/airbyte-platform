/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared.models

enum class ActionType {
  PROMOTE,
  ROLLBACK,
}
