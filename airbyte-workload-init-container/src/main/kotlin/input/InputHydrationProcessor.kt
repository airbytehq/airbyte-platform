package io.airbyte.initContainer.input

import io.airbyte.workload.api.client.model.generated.Workload

interface InputHydrationProcessor {
  fun process(workload: Workload)
}
