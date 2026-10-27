/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.data.repositories.CommandAuthorizationRepository
import io.airbyte.data.repositories.WorkloadAuthorizationRepository
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import jakarta.inject.Singleton

/** ID-only execution routes resolve saved ownership without reading their payloads. */
@Singleton
class SavedExecutionAuthorizationDataService(
  private val commands: CommandAuthorizationRepository,
  private val workloads: WorkloadAuthorizationRepository,
  private val data: AuthorizationDataService,
) {
  fun command(id: String): WorkspaceAndOrganization {
    val saved = commands.findOwnerById(id).orElseThrow { AuthorizationResourceNotFoundException() }
    if (saved.id != id) throw AuthorizationResourceNotFoundException()
    val owner = data.savedWorkspaceAndOrganization(saved.workspaceId, true)
    if (owner.organizationId != saved.organizationId) throw AuthorizationResourceNotFoundException()
    val scoped = commands.findByIdAndOrganizationId(id, owner.organizationId).orElseThrow { AuthorizationResourceNotFoundException() }
    if (scoped != saved) throw AuthorizationResourceNotFoundException()
    val workload =
      workloads
        .findByIdAndOrganizationId(
          saved.workloadId,
          owner.organizationId,
        ).orElseThrow { AuthorizationResourceNotFoundException() }
    if (workload.id != saved.workloadId || workload.workspaceId != owner.workspaceId) throw AuthorizationResourceNotFoundException()
    if (workload.organizationId != owner.organizationId && !(workload.organizationId == null && workload.type == "spec")) {
      throw AuthorizationResourceNotFoundException()
    }
    return owner
  }

  fun specCommand(id: String): WorkspaceAndOrganization {
    val owner = command(id)
    val saved = commands.findByIdAndOrganizationId(id, owner.organizationId).orElseThrow { AuthorizationResourceNotFoundException() }
    if (saved.commandType != "SPEC") throw AuthorizationResourceNotFoundException()
    return owner
  }

  fun workload(id: String): AuthorizationTarget {
    val saved = workloads.findOwnerById(id).orElseThrow { AuthorizationResourceNotFoundException() }
    if (saved.id != id) throw AuthorizationResourceNotFoundException()
    val workspaceId = saved.workspaceId
    val target =
      if (workspaceId != null) {
        val owner = data.savedWorkspaceAndOrganization(workspaceId, true)
        if (saved.organizationId != null && saved.organizationId != owner.organizationId) throw AuthorizationResourceNotFoundException()
        if (saved.organizationId == null && saved.type != "spec") throw AuthorizationResourceNotFoundException()
        AuthorizationTarget.Workspace(owner)
      } else {
        AuthorizationTarget.Organization(data.organization(saved.organizationId ?: throw AuthorizationResourceNotFoundException()))
      }
    val organization =
      when (target) {
        is AuthorizationTarget.Workspace -> target.workspaceAndOrganization.organizationId
        is AuthorizationTarget.Organization -> target.organizationId
        AuthorizationTarget.Global -> throw AuthorizationResourceNotFoundException()
      }
    val scoped = workloads.findByIdAndOrganizationId(id, organization).orElseThrow { AuthorizationResourceNotFoundException() }
    if (scoped != saved) throw AuthorizationResourceNotFoundException()
    return target
  }
}
