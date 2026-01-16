/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class JobsHelperTest {
  @Test
  fun `it should return the correct field and method pair`() {
    val result = orderByToFieldAndMethod("createdAt|ASC")
    assertEquals(result.first.ordinal, JobListForWorkspacesRequestBody.OrderByFieldEnum.CREATED_AT.ordinal)
    assertEquals(result.first::class, JobListForWorkspacesRequestBody.OrderByFieldEnum.CREATED_AT::class)
    assertEquals(result.second.ordinal, JobListForWorkspacesRequestBody.OrderByMethodEnum.ASC.ordinal)
    assertEquals(result.second::class, JobListForWorkspacesRequestBody.OrderByMethodEnum.ASC::class)
  }
}
