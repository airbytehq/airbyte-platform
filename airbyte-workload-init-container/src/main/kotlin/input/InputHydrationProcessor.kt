/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.input

import io.airbyte.workload.api.domain.Workload

interface InputHydrationProcessor {
  fun process(workload: Workload)
}
