/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.CreateOrUpdateSecretsPersistenceConfigRequestBody
import io.airbyte.api.server.generated.models.MigrateSecretStorageRequestBody
import io.airbyte.api.server.generated.models.SecretPersistenceConfigGetRequestBody
import io.airbyte.api.server.generated.models.SecretStorageCreateRequestBody
import io.airbyte.api.server.generated.models.SecretStorageIdRequestBody
import io.airbyte.api.server.generated.models.SecretStorageListRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.config.ScopeType
import io.airbyte.domain.models.SecretStorage
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

data class SecretScopeInput(
  val type: ScopeType,
  val id: UUID,
) : OperationInput

data class SavedSecretStorageInput(
  val id: UUID,
) : OperationInput

data class SecretMigrationInput(
  val from: UUID,
  val to: UUID,
  val scope: SecretScopeInput,
) : OperationInput

private fun secretScopeTarget(
  data: AuthorizationDataService,
  scope: SecretScopeInput,
): AuthorizationTarget =
  when (scope.type) {
    ScopeType.WORKSPACE -> AuthorizationTarget.Workspace(data.workspaceAndOrganization(scope.id))
    ScopeType.ORGANIZATION -> AuthorizationTarget.Organization(data.organization(scope.id))
  }

private fun secretStorageId(args: Array<out Any?>): SavedSecretStorageInput =
  SavedSecretStorageInput((args.singleOrNull() as? SecretStorageIdRequestBody)?.secretStorageId ?: throw BadRequestProblem())

@Singleton
class SecretStorageManageRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput =
    when (val body = args.singleOrNull()) {
      is SecretStorageCreateRequestBody -> SecretScopeInput(ScopeType.fromValue(body.scopeType.value), body.scopeId)
      is SecretStorageListRequestBody -> SecretScopeInput(ScopeType.fromValue(body.scopeType.value), body.scopeId)
      else -> throw BadRequestProblem()
    }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAuthorizationTarget(permissions, caller, secretScopeTarget(data, input as SecretScopeInput), Intent.ManageSecretStorages.roles)
  }
}

@Singleton
class SecretStorageDeleteRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = secretStorageId(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val id = (input as SavedSecretStorageInput).id
    if (id == SecretStorage.DEFAULT_SECRET_STORAGE_ID.value) {
      permissions.requireGlobal(caller, Intent.ManageSecretStorages.roles, organizationCallers)
      throw IllegalArgumentException("Cannot disable the default secret storage")
    }
    requireAuthorizationTarget(permissions, caller, data.secretStorageTarget(id), Intent.ManageSecretStorages.roles)
  }
}

@Singleton
class SecretStorageReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = secretStorageId(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAuthorizationTarget(
      permissions,
      caller,
      data.secretStorageTarget((input as SavedSecretStorageInput).id),
      setOf(AuthRoleConstants.DATAPLANE),
    )
  }
}

@Singleton
class SecretStorageMigrationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? MigrateSecretStorageRequestBody ?: throw BadRequestProblem()
    return SecretMigrationInput(
      body.fromSecretStorageId,
      body.toSecretStorageId,
      SecretScopeInput(ScopeType.fromValue(body.scopeType.value), body.scopeId),
    )
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SecretMigrationInput
    val target = secretScopeTarget(data, request.scope)
    requireAuthorizationTarget(permissions, caller, target, Intent.ManageSecretStorages.roles)
    if (request.to ==
      SecretStorage.DEFAULT_SECRET_STORAGE_ID.value
    ) {
      throw IllegalArgumentException("Cannot migrate secrets to the default secret storage")
    }
    val organization = organization(target)
    if (request.from != SecretStorage.DEFAULT_SECRET_STORAGE_ID.value) requireOwnedStore(caller, request.from, organization, "from")
    requireOwnedStore(caller, request.to, organization, "to")
  }

  private fun requireOwnedStore(
    caller: Authentication,
    id: UUID,
    organization: UUID,
    direction: String,
  ) {
    val target = data.secretStorageTarget(id)
    require(organization(target) == organization) {
      "Cannot migrate secrets $direction secret storage ${io.airbyte.domain.models.SecretStorageId(
        id,
      )} since it is not owned by the requested scope (organizationId=${io.airbyte.domain.models.OrganizationId(organization)})"
    }
    requireAuthorizationTarget(permissions, caller, target, Intent.ManageSecretStorages.roles)
  }

  private fun organization(target: AuthorizationTarget): UUID =
    when (target) {
      is AuthorizationTarget.Organization -> target.organizationId
      is AuthorizationTarget.Workspace -> target.workspaceAndOrganization.organizationId
      AuthorizationTarget.Global -> throw IllegalArgumentException("Secret storage has no organization")
    }
}

@Singleton
class SecretsPersistenceWriteRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? CreateOrUpdateSecretsPersistenceConfigRequestBody ?: throw BadRequestProblem()
    return SecretScopeInput(ScopeType.fromValue(body.scope.toString()), body.scopeId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    requireAuthorizationTarget(permissions, caller, secretScopeTarget(data, input as SecretScopeInput), setOf(AuthRoleConstants.ADMIN))
  }
}

@Singleton
class SecretsPersistenceReadRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val body = args.singleOrNull() as? SecretPersistenceConfigGetRequestBody ?: throw BadRequestProblem()
    return SecretScopeInput(ScopeType.fromValue(body.scopeType.toString()), body.scopeId)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as SecretScopeInput
    requireAuthorizationTarget(permissions, caller, secretScopeTarget(data, request), setOf(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE))
    if (request.type ==
      ScopeType.WORKSPACE
    ) {
      throw BadObjectSchemaKnownException("Runtime secret persistence is not supported for workspace ${request.id}")
    }
  }
}
