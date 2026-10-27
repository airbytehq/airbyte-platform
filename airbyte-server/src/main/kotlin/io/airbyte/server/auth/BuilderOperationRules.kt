/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.BuilderProjectForDefinitionRequestBody
import io.airbyte.api.server.generated.models.BuilderProjectOauthConsentRequest
import io.airbyte.api.server.generated.models.CompleteConnectorBuilderProjectOauthRequest
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectForkRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectFullResolveRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectResolveRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectTestingValuesUpdate
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderPublishRequestBody
import io.airbyte.api.server.generated.models.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.BuilderProjectAuthorizationMetadata
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.BuilderAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class BuilderWorkspaceInput(
  val workspace: UUID,
) : OperationInput

data class BuilderCreateInput(
  val workspace: UUID,
  val base: Optional<UUID>,
) : OperationInput

data class BuilderForkInput(
  val workspace: UUID,
  val definition: UUID,
) : OperationInput

data class BuilderSavedInput(
  val workspace: UUID,
  val project: UUID,
  val version: Optional<Long> = Optional.empty(),
) : OperationInput

data class BuilderUpdateInput(
  val workspace: UUID,
  val project: UUID,
  val base: Optional<UUID>,
) : OperationInput

data class BuilderDefinitionInput(
  val workspace: UUID,
  val definition: UUID,
) : OperationInput

data class BuilderResolveInput(
  val workspace: UUID,
  val tracingProject: Optional<UUID>,
) : OperationInput

abstract class BuilderOperationRule(
  private val data: AuthorizationDataService,
  protected val projects: BuilderAuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  protected fun owner(
    caller: Authentication,
    workspace: UUID,
    write: Boolean = false,
  ): WorkspaceAndOrganization {
    val owner = data.workspaceAndOrganization(workspace)
    permissions.requireWorkspaces(caller, listOf(owner), if (write) sourceWriteRoles else workspaceReadRoles, organizationCallers)
    return owner
  }

  protected fun base(
    version: Optional<UUID>,
    owner: WorkspaceAndOrganization,
  ) {
    version.ifPresent { if (!projects.baseUsable(it, owner)) throw ForbiddenProblem() }
  }

  protected fun definition(
    definition: Optional<UUID>,
    owner: WorkspaceAndOrganization,
  ) {
    definition.ifPresent { if (!projects.definitionUsable(it, owner)) throw ForbiddenProblem() }
  }

  protected fun manifest(
    project: BuilderProjectAuthorizationMetadata,
    owner: WorkspaceAndOrganization,
  ) {
    if (!project.hasDraft) definition(project.definition, owner)
  }
}

@Singleton
class BuilderCreateRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ConnectorBuilderProjectWithWorkspaceId ?: throw BadRequestProblem()
    return BuilderCreateInput(body.workspaceId, Optional.ofNullable(body.builderProject.baseActorDefinitionVersionId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderCreateInput
    base(request.base, owner(caller, request.workspace, true))
  }
}

@Singleton
class BuilderForkRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ConnectorBuilderProjectForkRequestBody ?: throw BadRequestProblem()
    return BuilderForkInput(body.workspaceId, body.baseActorDefinitionId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderForkInput
    if (!projects.forkUsable(request.definition, owner(caller, request.workspace, true))) throw ForbiddenProblem()
  }
}

@Singleton
class BuilderDeleteRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body =
      args.singleOrNull() as? ConnectorBuilderProjectIdWithWorkspaceId ?: throw BadRequestProblem()
    return BuilderSavedInput(body.workspaceId, body.builderProjectId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderSavedInput
    owner(caller, request.workspace, true)
    projects.saved(request.project, request.workspace)
  }
}

@Singleton
class BuilderSavedReadRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body =
      args.singleOrNull() as? ConnectorBuilderProjectIdWithWorkspaceId ?: throw BadRequestProblem()
    return BuilderSavedInput(body.workspaceId, body.builderProjectId, Optional.ofNullable(body.version))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderSavedInput
    val owner = owner(caller, request.workspace)
    val project = projects.saved(request.project, request.workspace)
    if (request.version.isPresent) {
      if (project.definition.isEmpty) throw AuthorizationResourceNotFoundException()
      definition(project.definition, owner)
    } else {
      base(project.baseVersion, owner)
      manifest(project, owner)
    }
  }
}

@Singleton
class BuilderProjectListRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    BuilderWorkspaceInput((args.singleOrNull() as? WorkspaceIdRequestBody)?.workspaceId ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderWorkspaceInput
    owner(caller, request.workspace)
  }
}

@Singleton
class BuilderPublishRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body =
      args.singleOrNull() as? ConnectorBuilderPublishRequestBody ?: throw BadRequestProblem()
    return BuilderSavedInput(body.workspaceId, body.builderProjectId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderSavedInput
    owner(caller, request.workspace, true)
    projects.saved(request.project, request.workspace)
  }
}

@Singleton
class BuilderTestingReadRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is ConnectorBuilderProjectStreamReadRequestBody -> BuilderSavedInput(body.workspaceId, body.builderProjectId)
      is ConnectorBuilderProjectFullResolveRequestBody -> BuilderSavedInput(requireNotNull(body.workspaceId), requireNotNull(body.builderProjectId))
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderSavedInput
    owner(caller, request.workspace)
    projects.saved(request.project, request.workspace)
  }
}

@Singleton
class BuilderResolveRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ConnectorBuilderProjectResolveRequestBody ?: throw BadRequestProblem()
    return BuilderResolveInput(requireNotNull(body.workspaceId), Optional.ofNullable(body.builderProjectId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    owner(caller, (input as BuilderResolveInput).workspace)
  }
}

@Singleton
class BuilderCapabilitiesRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    BuilderWorkspaceInput((args.singleOrNull() as? WorkspaceIdRequestBody)?.workspaceId ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    owner(caller, (input as BuilderWorkspaceInput).workspace)
  }
}

@Singleton
class BuilderUpdateRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body =
      args.singleOrNull() as? ExistingConnectorBuilderProjectWithWorkspaceId ?: throw BadRequestProblem()
    return BuilderUpdateInput(body.workspaceId, body.builderProjectId, Optional.ofNullable(body.builderProject.baseActorDefinitionVersionId))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderUpdateInput
    val owner = owner(caller, request.workspace, true)
    val project = projects.saved(request.project, request.workspace)
    base(request.base.or { project.baseVersion }, owner)
    definition(project.definition, owner)
  }
}

@Singleton
class BuilderTestingWriteRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body =
      args.singleOrNull() as? ConnectorBuilderProjectTestingValuesUpdate ?: throw BadRequestProblem()
    return BuilderSavedInput(body.workspaceId, body.builderProjectId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderSavedInput
    owner(caller, request.workspace, true)
    projects.saved(request.project, request.workspace)
  }
}

@Singleton
class BuilderDefinitionProjectRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body =
      args.singleOrNull() as? BuilderProjectForDefinitionRequestBody ?: throw BadRequestProblem()
    return BuilderDefinitionInput(body.workspaceId, body.actorDefinitionId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderDefinitionInput
    val owner = owner(caller, request.workspace)
    projects.forDefinition(request.definition, owner).ifPresent { definition(it.definition, owner) }
  }
}

@Singleton
class BuilderOAuthRule(
  data: AuthorizationDataService,
  projects: BuilderAuthorizationDataService,
  permissions: OperationPermissions,
) : BuilderOperationRule(data, projects, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is BuilderProjectOauthConsentRequest -> BuilderSavedInput(body.workspaceId, body.builderProjectId)
      is CompleteConnectorBuilderProjectOauthRequest -> BuilderSavedInput(body.workspaceId, body.builderProjectId)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as BuilderSavedInput
    val owner = owner(caller, request.workspace)
    manifest(projects.saved(request.project, request.workspace), owner)
  }
}
