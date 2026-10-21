/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.server.generated.models.JobListForWorkspacesRequestBody
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class JobsHelperTest {
  @Test
  fun `it should return the correct field and method pair`() {
    val result = orderByToFieldAndMethod("createdAt|ASC")
    assertEquals(result.first.ordinal, JobListForWorkspacesRequestBody.OrderByField.CREATED_AT.ordinal)
    assertEquals(result.first::class, JobListForWorkspacesRequestBody.OrderByField.CREATED_AT::class)
    assertEquals(result.second.ordinal, JobListForWorkspacesRequestBody.OrderByMethod.ASC.ordinal)
    assertEquals(result.second::class, JobListForWorkspacesRequestBody.OrderByMethod.ASC::class)
  }
}
