/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.OrchestrationApi
import io.airbyte.api.server.generated.models.OrchestrationCreateRequestBody
import io.airbyte.api.server.generated.models.OrchestrationDeleteRequestBody
import io.airbyte.api.server.generated.models.OrchestrationGetRequestBody
import io.airbyte.api.server.generated.models.OrchestrationListRequestBody
import io.airbyte.api.server.generated.models.OrchestrationRead
import io.airbyte.api.server.generated.models.OrchestrationReadList
import io.airbyte.api.server.generated.models.OrchestrationRunRequestBody
import io.airbyte.api.server.generated.models.OrchestrationRunResponseBody
import io.airbyte.api.server.generated.models.OrchestrationUpdateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.OrchestrationEntitlement
import io.airbyte.persistence.job.WorkspaceHelper
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class OrchestrationApiController(
  private val workspaceHelper: WorkspaceHelper,
  private val entitlementService: EntitlementService,
) : OrchestrationApi {
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun getOrchestration(
    @Body orchestrationGetRequestBody: OrchestrationGetRequestBody,
  ): OrchestrationRead {
    checkEntitlement(orchestrationGetRequestBody.workspaceId)
    TODO("Not yet implemented")
  }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  override fun listOrchestrationsForWorkspace(
    @Body orchestrationListRequestBody: OrchestrationListRequestBody,
  ): OrchestrationReadList {
    checkEntitlement(orchestrationListRequestBody.workspaceId)
    TODO("Not yet implemented")
  }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  override fun createOrchestration(
    @Body orchestrationCreateRequestBody: OrchestrationCreateRequestBody,
  ): OrchestrationRead {
    checkEntitlement(orchestrationCreateRequestBody.workspaceId)
    TODO("Not yet implemented")
  }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  override fun deleteOrchestration(
    @Body orchestrationDeleteRequestBody: OrchestrationDeleteRequestBody,
  ) {
    checkEntitlement(orchestrationDeleteRequestBody.workspaceId)
    TODO("Not yet implemented")
  }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  override fun updateOrchestration(
    @Body orchestrationUpdateRequestBody: OrchestrationUpdateRequestBody,
  ): OrchestrationRead {
    checkEntitlement(orchestrationUpdateRequestBody.workspaceId)
    TODO("Not yet implemented")
  }

  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  override fun runOrchestration(
    @Body orchestrationRunRequestBody: OrchestrationRunRequestBody,
  ): OrchestrationRunResponseBody {
    checkEntitlement(orchestrationRunRequestBody.workspaceId)
    TODO("Not yet implemented")
  }

  internal fun checkEntitlement(workspaceId: UUID) {
    val orgId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
    entitlementService.ensureEntitled(orgId, OrchestrationEntitlement)
  }
}
