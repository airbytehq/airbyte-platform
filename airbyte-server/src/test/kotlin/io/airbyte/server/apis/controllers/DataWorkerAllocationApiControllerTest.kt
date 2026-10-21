/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.models.DataWorkerAddCapacityRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationGetRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationListRequestBody
import io.airbyte.api.server.generated.models.DataWorkerReallocateRequestBody
import io.airbyte.api.server.generated.models.DataWorkerRemoveCapacityRequestBody
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

internal class DataWorkerAllocationApiControllerTest {
  private val controller = DataWorkerAllocationApiController()

  @Test
  fun `listDataWorkerAllocations is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.listDataWorkerAllocations(DataWorkerAllocationListRequestBody(organizationId = UUID.randomUUID()))
    }
  }

  @Test
  fun `getDataWorkerAllocation is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.getDataWorkerAllocation(DataWorkerAllocationGetRequestBody(organizationId = UUID.randomUUID(), dataplaneGroupId = UUID.randomUUID()))
    }
  }

  @Test
  fun `reallocateDataWorkerCapacity is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.reallocateDataWorkerCapacity(
        DataWorkerReallocateRequestBody(
          organizationId = UUID.randomUUID(),
          fromDataplaneGroupId = UUID.randomUUID(),
          toDataplaneGroupId = UUID.randomUUID(),
          amount = 1.0,
        ),
      )
    }
  }

  @Test
  fun `addDataWorkerCapacity is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.addDataWorkerCapacity(DataWorkerAddCapacityRequestBody(organizationId = UUID.randomUUID(), amount = 1.0))
    }
  }

  @Test
  fun `removeDataWorkerCapacity is not implemented in OSS`() {
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.removeDataWorkerCapacity(
        DataWorkerRemoveCapacityRequestBody(organizationId = UUID.randomUUID(), dataplaneGroupId = UUID.randomUUID(), amount = 1.0),
      )
    }
  }
}
