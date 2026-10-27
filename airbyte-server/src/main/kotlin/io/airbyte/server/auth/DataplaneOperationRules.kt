/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.DataplaneCreateRequestBody
import io.airbyte.api.server.generated.models.DataplaneDeleteRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupCreateRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupDeleteRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupListRequestBody
import io.airbyte.api.server.generated.models.DataplaneGroupUpdateRequestBody
import io.airbyte.api.server.generated.models.DataplaneHeartbeatRequestBody
import io.airbyte.api.server.generated.models.DataplaneInitRequestBody
import io.airbyte.api.server.generated.models.DataplaneListRequestBody
import io.airbyte.api.server.generated.models.DataplaneUpdateRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.DataplaneAuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.UUID

sealed interface DataplaneOperationInput : OperationInput {
  data class Organization(
    val organizationId: UUID,
  ) : DataplaneOperationInput

  data class Group(
    val groupId: UUID,
  ) : DataplaneOperationInput

  data class Saved(
    val dataplaneId: UUID,
  ) : DataplaneOperationInput

  data class Client(
    val clientId: String,
  ) : DataplaneOperationInput

  data class Create(
    val groupId: UUID,
    val instanceScope: Boolean,
  ) : DataplaneOperationInput
}

private fun dataplaneInput(args: Array<out Any?>): DataplaneOperationInput =
  when (val body = args.firstOrNull()) {
    is DataplaneCreateRequestBody -> DataplaneOperationInput.Create(body.dataplaneGroupId, body.instanceScope ?: false)
    is DataplaneUpdateRequestBody -> DataplaneOperationInput.Saved(body.dataplaneId)
    is DataplaneDeleteRequestBody -> DataplaneOperationInput.Saved(body.dataplaneId)
    is DataplaneListRequestBody -> DataplaneOperationInput.Group(body.dataplaneGroupId)
    is OrganizationIdRequestBody -> DataplaneOperationInput.Organization(body.organizationId)
    is DataplaneGroupCreateRequestBody -> DataplaneOperationInput.Organization(body.organizationId)
    is DataplaneGroupUpdateRequestBody -> DataplaneOperationInput.Group(body.dataplaneGroupId)
    is DataplaneGroupDeleteRequestBody -> DataplaneOperationInput.Group(body.dataplaneGroupId)
    is DataplaneGroupListRequestBody -> DataplaneOperationInput.Organization(body.organizationId)
    is DataplaneInitRequestBody -> DataplaneOperationInput.Client(body.clientId)
    is DataplaneHeartbeatRequestBody -> DataplaneOperationInput.Client(body.clientId)
    else -> throw BadRequestProblem()
  }

private val dataplaneManagers =
  setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)

abstract class DataplaneManagementOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val roles: Set<String>,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = dataplaneInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val organization =
      when (val request = input as DataplaneOperationInput) {
        is DataplaneOperationInput.Organization -> data.organization(request.organizationId)
        is DataplaneOperationInput.Group -> data.dataplaneGroupOrganization(request.groupId)
        is DataplaneOperationInput.Saved -> data.dataplaneOrganization(request.dataplaneId)
        is DataplaneOperationInput.Client -> throw BadRequestProblem()
        is DataplaneOperationInput.Create -> throw BadRequestProblem()
      }
    permissions.requireOrganizations(caller, listOf(organization), roles, dataplaneManagers)
  }
}

@Singleton
class DataplaneManageRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DataplaneManagementOperationRule(data, permissions, Intent.ManageDataplanes.roles)

@Singleton
class DataplaneCreateRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = dataplaneInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val request = input as DataplaneOperationInput.Create
    val organization = data.dataplaneGroupOrganization(request.groupId)
    permissions.requireOrganizations(caller, listOf(organization), Intent.ManageDataplanes.roles, dataplaneManagers)
    if (request.instanceScope) permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), dataplaneManagers)
  }
}

@Singleton
class DataplaneGroupManageRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DataplaneManagementOperationRule(data, permissions, Intent.ManageDataplaneGroups.roles)

/** This is a filtered list of usable shared regions, not organization administration. */
@Singleton
class DataplaneGroupListRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = dataplaneInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    permissions.requireAuthenticated(caller, dataplaneManagers)
    data.organization((input as DataplaneOperationInput.Organization).organizationId)
  }
}

@Singleton
class DataplaneMachineRule(
  private val data: DataplaneAuthorizationDataService,
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = dataplaneInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val clientId = (input as DataplaneOperationInput.Client).clientId
    permissions.requireDataplaneIdentity(caller, UUID.fromString(clientId))
    permissions.requireOrganizations(
      caller,
      listOf(data.clientOrganization(clientId)),
      setOf(AuthRoleConstants.DATAPLANE),
      setOf(OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT),
    )
  }
}
