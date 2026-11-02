/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.PublicDefinitionAuthorizationService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class PublicDefinitionInput(
  val workspace: UUID,
  val definition: Optional<UUID>,
) : OperationInput

abstract class PublicDefinitionRule(
  private val data: AuthorizationDataService,
  private val definitions: PublicDefinitionAuthorizationService,
  private val permissions: OperationPermissions,
  private val source: Boolean,
  private val write: Boolean,
  private val declarative: Boolean = false,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    PublicDefinitionInput(publicResourceId(args.firstOrNull()), Optional.ofNullable(args.getOrNull(1) as? UUID))

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val callers = setOf(OperationCaller.USER)
    permissions.requireCaller(caller, callers)
    val request = input as PublicDefinitionInput
    val owner = data.workspaceAndOrganization(request.workspace)
    val roles =
      if (!write) {
        setOf(AuthRoleConstants.WORKSPACE_READER)
      } else if (source) {
        setOf(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR)
      } else {
        setOf(AuthRoleConstants.WORKSPACE_EDITOR)
      }
    permissions.requireWorkspaces(caller, listOf(owner), roles, callers)
    request.definition.ifPresent { id ->
      val type = if (source) ConfigNotFoundType.STANDARD_SOURCE_DEFINITION else ConfigNotFoundType.STANDARD_DESTINATION_DEFINITION
      publicConfigResource(type, id) {
        data.requireActorDefinitionType(id, source)
        if (!data.canUseActorDefinition(id, owner)) throw AuthorizationResourceNotFoundException()
        val saved = definitions.definition(id, source)
        if (saved.declarative != declarative) throw AuthorizationResourceNotFoundException()
        if (write && !saved.custom) throw BadRequestProblem(ProblemMessageData().message("Public definitions cannot be modified."))
        if (declarative && write && definitions.builderWorkspace(id) != owner.workspaceId) throw AuthorizationResourceNotFoundException()
      }
    }
  }
}

@Singleton
class PublicSourceDefinitionReadRule(
  data: AuthorizationDataService,
  definitions: PublicDefinitionAuthorizationService,
  permissions: OperationPermissions,
) : PublicDefinitionRule(data, definitions, permissions, true, false)

@Singleton
class PublicSourceDefinitionWriteRule(
  data: AuthorizationDataService,
  definitions: PublicDefinitionAuthorizationService,
  permissions: OperationPermissions,
) : PublicDefinitionRule(data, definitions, permissions, true, true)

@Singleton
class PublicDestinationDefinitionReadRule(
  data: AuthorizationDataService,
  definitions: PublicDefinitionAuthorizationService,
  permissions: OperationPermissions,
) : PublicDefinitionRule(data, definitions, permissions, false, false)

@Singleton
class PublicDestinationDefinitionWriteRule(
  data: AuthorizationDataService,
  definitions: PublicDefinitionAuthorizationService,
  permissions: OperationPermissions,
) : PublicDefinitionRule(data, definitions, permissions, false, true)

@Singleton
class PublicDeclarativeDefinitionReadRule(
  data: AuthorizationDataService,
  definitions: PublicDefinitionAuthorizationService,
  permissions: OperationPermissions,
) : PublicDefinitionRule(data, definitions, permissions, true, false, true)

@Singleton
class PublicDeclarativeDefinitionWriteRule(
  data: AuthorizationDataService,
  definitions: PublicDefinitionAuthorizationService,
  permissions: OperationPermissions,
) : PublicDefinitionRule(data, definitions, permissions, true, true, true)
