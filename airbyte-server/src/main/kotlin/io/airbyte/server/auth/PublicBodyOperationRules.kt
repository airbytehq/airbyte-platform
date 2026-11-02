/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.model.generated.ProblemValueData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.UnknownValueProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.publicApi.server.generated.models.DestinationCreateRequest
import io.airbyte.publicApi.server.generated.models.EmbeddedScopedTokenRequest
import io.airbyte.publicApi.server.generated.models.EmbeddedWidgetRequest
import io.airbyte.publicApi.server.generated.models.GroupCreateRequest
import io.airbyte.publicApi.server.generated.models.InitiateOauthRequest
import io.airbyte.publicApi.server.generated.models.RegionCreateRequest
import io.airbyte.publicApi.server.generated.models.SourceCreateRequest
import io.airbyte.publicApi.server.generated.models.TagCreateRequest
import io.airbyte.publicApi.server.generated.models.WorkspaceCreateRequest
import io.airbyte.server.apis.publicapi.mappers.DESTINATION_NAME_TO_DEFINITION_ID
import io.airbyte.server.apis.publicapi.mappers.SOURCE_NAME_TO_DEFINITION_ID
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class PublicActorCreateInput(
  val workspace: UUID,
  val definition: UUID,
) : OperationInput

private fun actorDefinition(
  configuration: JsonNode,
  source: Boolean,
): UUID {
  val name = configuration.findValue(if (source) "sourceType" else "destinationType")?.asText() ?: throw UnprocessableEntityProblem()
  return (if (source) SOURCE_NAME_TO_DEFINITION_ID else DESTINATION_NAME_TO_DEFINITION_ID)[name]
    ?: throw UnknownValueProblem(ProblemValueData().value(name))
}

abstract class PublicActorCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.firstOrNull()) {
      is SourceCreateRequest -> PublicActorCreateInput(body.workspaceId, body.definitionId ?: actorDefinition(body.configuration, true))
      is DestinationCreateRequest -> PublicActorCreateInput(body.workspaceId, body.definitionId ?: actorDefinition(body.configuration, false))
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER, OperationCaller.EMBEDDED)
    permissions.requireCaller(caller, callers)
    val request = input as PublicActorCreateInput
    val owner = data.workspaceAndOrganization(request.workspace)
    val roles = if (source) sourceWriteRoles + AuthRoleConstants.EMBEDDED_END_USER else destinationWriteRoles + AuthRoleConstants.EMBEDDED_END_USER
    permissions.requireWorkspaces(caller, listOf(owner), roles, callers)
    val type = if (source) ConfigNotFoundType.STANDARD_SOURCE_DEFINITION else ConfigNotFoundType.STANDARD_DESTINATION_DEFINITION
    publicConfigResource(type, request.definition) { data.requireActorDefinitionType(request.definition, source) }
    if (!data.canUseActorDefinition(request.definition, owner)) throw ForbiddenProblem()
  }
}

@Singleton
class PublicSourceCreateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicActorCreateRule(data, permissions, true)

@Singleton
class PublicDestinationCreateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicActorCreateRule(data, permissions, false)

@Singleton
class PublicTagCreateRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.WORKSPACE, publicWriteRoles) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicResourceInput((args.firstOrNull() as? TagCreateRequest ?: throw BadRequestProblem()).workspaceId)
}

@Singleton
class PublicOrganizationBodyAdminRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.ORGANIZATION, setOf(AuthRoleConstants.ORGANIZATION_ADMIN)) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicResourceInput(
      when (val body = args.firstOrNull()) {
        is GroupCreateRequest -> body.organizationId
        is RegionCreateRequest -> body.organizationId
        is EmbeddedWidgetRequest -> body.organizationId
        else -> throw BadRequestProblem()
      },
    )
}

@Singleton
class PublicEmbeddedTokenRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(data, permissions, PublicResourceKind.WORKSPACE, setOf(AuthRoleConstants.ORGANIZATION_ADMIN)) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicResourceInput((args.firstOrNull() as? EmbeddedScopedTokenRequest ?: throw BadRequestProblem()).workspaceId)
}

data class PublicWorkspaceCreateInput(
  val region: Optional<UUID>,
) : OperationInput

@Singleton
class PublicWorkspaceCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicWorkspaceCreateInput(Optional.ofNullable((args.firstOrNull() as? WorkspaceCreateRequest ?: throw BadRequestProblem()).regionId))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val organization = data.organization(DEFAULT_ORGANIZATION_ID)
    permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_EDITOR), callers)
    (input as PublicWorkspaceCreateInput).region.ifPresent { if (!data.canUseDataplaneGroup(it, organization)) throw ForbiddenProblem() }
  }
}

/** The OSS OAuth endpoint is a static 501 after the original workspace action. */
@Singleton
open class PublicSourceOAuthRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : PublicResourceRule(
    data,
    permissions,
    PublicResourceKind.WORKSPACE,
    sourceWriteRoles + AuthRoleConstants.EMBEDDED_END_USER,
    setOf(OperationCaller.USER, OperationCaller.EMBEDDED),
  ) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicResourceInput((args.firstOrNull() as? InitiateOauthRequest ?: throw BadRequestProblem()).workspaceId)
}
