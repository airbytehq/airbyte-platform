/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.config.Dataplane
import io.airbyte.domain.models.ServiceAccount

data class DataplaneWithServiceAccount(
  val dataplane: Dataplane,
  val serviceAccount: ServiceAccount,
)
