/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationSemanticSearchEnablementUpdateRequestBody
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class OrganizationSemanticSearchApiControllerTest {
  private val controller = OrganizationSemanticSearchApiController()

  @Test
  fun `semantic search enablement is unavailable in OSS`() {
    val organizationId = UUID.randomUUID()
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.getOrganizationSemanticSearchEnablement(OrganizationIdRequestBody(organizationId = organizationId))
    }
    assertThrows<ApiNotImplementedInOssProblem> {
      controller.updateOrganizationSemanticSearchEnablement(
        OrganizationSemanticSearchEnablementUpdateRequestBody(
          organizationId = organizationId,
          semanticSearchEnabled = true,
        ),
      )
    }
  }
}
