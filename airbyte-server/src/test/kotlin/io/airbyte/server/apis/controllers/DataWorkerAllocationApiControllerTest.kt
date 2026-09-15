/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DataWorkerAddCapacityRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationGetRequestBody
import io.airbyte.api.model.generated.DataWorkerAllocationListRequestBody
import io.airbyte.api.model.generated.DataWorkerReallocateRequestBody
import io.airbyte.api.model.generated.DataWorkerRemoveCapacityRequestBody
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class DataWorkerAllocationApiControllerTest {
  private val controller = DataWorkerAllocationApiController()

  @Test
  fun `listDataWorkerAllocations is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.listDataWorkerAllocations(DataWorkerAllocationListRequestBody())
    }
  }

  @Test
  fun `getDataWorkerAllocation is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.getDataWorkerAllocation(DataWorkerAllocationGetRequestBody())
    }
  }

  @Test
  fun `reallocateDataWorkerCapacity is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.reallocateDataWorkerCapacity(DataWorkerReallocateRequestBody())
    }
  }

  @Test
  fun `addDataWorkerCapacity is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.addDataWorkerCapacity(DataWorkerAddCapacityRequestBody())
    }
  }

  @Test
  fun `removeDataWorkerCapacity is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.removeDataWorkerCapacity(DataWorkerRemoveCapacityRequestBody())
    }
  }
}
