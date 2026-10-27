/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.ScopedConfigurationContextRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationCreateRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationDeleteRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationListRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationReadRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationUpdateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.ScopedConfigurationAuthorizationService
import io.airbyte.domain.services.auth.ScopedConfigurationReferences
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

data class ScopedConfigurationCreateInput(
  val references: ScopedConfigurationReferences,
) : OperationInput

data class ScopedConfigurationUpdateInput(
  val id: UUID,
  val references: ScopedConfigurationReferences,
) : OperationInput

data class ScopedConfigurationSavedInput(
  val id: UUID,
) : OperationInput

data class ScopedConfigurationListInput(
  val key: Optional<String>,
  val origin: Optional<String>,
) : OperationInput

data class ScopedConfigurationContextInput(
  val key: String,
  val type: ConfigScopeType,
  val scope: UUID,
  val resourceType: ConfigResourceType,
  val resource: Optional<UUID>,
) : OperationInput

private fun scopedReferences(
  body: ScopedConfigurationCreateRequestBody,
  data: ScopedConfigurationAuthorizationService,
): ScopedConfigurationReferences {
  if ((body.resourceType == null) !=
    (body.resourceId == null)
  ) {
    throw BadRequestException("resourceType and resourceId must either both be set or both be null")
  }
  if (body.resourceType != null) ConfigResourceType.fromValue(body.resourceType)
  val origin = ConfigOriginType.fromValue(body.originType)
  return ScopedConfigurationReferences(
    ConfigScopeType.fromValue(body.scopeType),
    UUID.fromString(body.scopeId),
    if (body.resourceType != null &&
      ConfigResourceType.fromValue(body.resourceType) == ConfigResourceType.ACTOR_DEFINITION
    ) {
      Optional.of(UUID.fromString(body.resourceId))
    } else {
      Optional.empty()
    },
    if (origin == ConfigOriginType.USER) Optional.of(UUID.fromString(body.origin)) else Optional.empty(),
    if (data.isConnectorVersionKey(body.configKey)) Optional.of(UUID.fromString(body.value)) else Optional.empty(),
  )
}

abstract class ScopedConfigurationOperationRule(
  protected val data: ScopedConfigurationAuthorizationService,
  private val permissions: OperationPermissions,
) : OperationRule {
  protected fun requireAdmin(caller: Authentication) = permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), organizationCallers)

  protected fun validateNew(references: ScopedConfigurationReferences) {
    try {
      data.validate(references)
    } catch (failure: AuthorizationResourceNotFoundException) {
      throw BadRequestException(failure.message)
    }
  }
}

@Singleton
class ScopedConfigurationCreateRule(
  data: ScopedConfigurationAuthorizationService,
  permissions: OperationPermissions,
) : ScopedConfigurationOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    ScopedConfigurationCreateInput(
      scopedReferences(args.singleOrNull() as? ScopedConfigurationCreateRequestBody ?: throw BadRequestProblem(), data),
    )

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    validateNew((input as ScopedConfigurationCreateInput).references)
  }
}

@Singleton
class ScopedConfigurationUpdateRule(
  data: ScopedConfigurationAuthorizationService,
  permissions: OperationPermissions,
) : ScopedConfigurationOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ScopedConfigurationUpdateRequestBody ?: throw BadRequestProblem()
    return ScopedConfigurationUpdateInput(
      body.scopedConfigurationId,
      scopedReferences(body.data, data),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    val request = input as ScopedConfigurationUpdateInput
    validateNew(request.references)
    data.existing(request.id).ifPresent { data.validate(it) }
  }
}

@Singleton
class ScopedConfigurationReadRule(
  data: ScopedConfigurationAuthorizationService,
  permissions: OperationPermissions,
) : ScopedConfigurationOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    ScopedConfigurationSavedInput((args.singleOrNull() as? ScopedConfigurationReadRequestBody)?.scopedConfigurationId ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    data.validate(data.read((input as ScopedConfigurationSavedInput).id))
  }
}

@Singleton
class ScopedConfigurationDeleteRule(
  data: ScopedConfigurationAuthorizationService,
  permissions: OperationPermissions,
) : ScopedConfigurationOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput =
    ScopedConfigurationSavedInput((args.singleOrNull() as? ScopedConfigurationDeleteRequestBody)?.scopedConfigurationId ?: throw BadRequestProblem())

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    data.validate(data.deletion((input as ScopedConfigurationSavedInput).id))
  }
}

@Singleton
class ScopedConfigurationListRule(
  data: ScopedConfigurationAuthorizationService,
  permissions: OperationPermissions,
) : ScopedConfigurationOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ScopedConfigurationListRequestBody ?: throw BadRequestProblem()
    return ScopedConfigurationListInput(Optional.ofNullable(body.configKey), Optional.ofNullable(body.originType))
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) = requireAdmin(caller)
}

@Singleton
class ScopedConfigurationContextRule(
  data: ScopedConfigurationAuthorizationService,
  permissions: OperationPermissions,
) : ScopedConfigurationOperationRule(data, permissions) {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? ScopedConfigurationContextRequestBody ?: throw BadRequestProblem()
    return ScopedConfigurationContextInput(
      body.configKey,
      ConfigScopeType.fromValue(body.scopeType),
      body.scopeId,
      ConfigResourceType.fromValue(body.resourceType),
      Optional.of(body.resourceId),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAdmin(caller)
    val request = input as ScopedConfigurationContextInput
    if (!data.isSupportedKey(request.key)) throw BadRequestException("Config key ${request.key} is not supported")
    if (request.type == ConfigScopeType.ACTOR) data.requireActorContext(request.scope)
  }
}
