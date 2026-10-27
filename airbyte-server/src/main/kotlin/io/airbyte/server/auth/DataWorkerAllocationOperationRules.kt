/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.DataWorkerAddCapacityRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationGetRequestBody
import io.airbyte.api.server.generated.models.DataWorkerAllocationListRequestBody
import io.airbyte.api.server.generated.models.DataWorkerReallocateRequestBody
import io.airbyte.api.server.generated.models.DataWorkerRemoveCapacityRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationInput
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.security.authentication.Authentication
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

sealed interface DataWorkerAllocationOperationInput : OperationInput {
  val organizationId: UUID

  data class Organization(
    override val organizationId: UUID,
  ) : DataWorkerAllocationOperationInput

  data class Filter(
    override val organizationId: UUID,
    val regionId: UUID,
  ) : DataWorkerAllocationOperationInput

  data class Reallocate(
    override val organizationId: UUID,
    val source: UUID,
    val destination: UUID,
  ) : DataWorkerAllocationOperationInput

  data class Add(
    override val organizationId: UUID,
    val destination: Optional<UUID>,
  ) : DataWorkerAllocationOperationInput

  data class Remove(
    override val organizationId: UUID,
    val source: UUID,
  ) : DataWorkerAllocationOperationInput
}

private fun allocationInput(args: Array<out Any?>): DataWorkerAllocationOperationInput =
  when (val body = args.singleOrNull()) {
    is DataWorkerAllocationListRequestBody -> DataWorkerAllocationOperationInput.Organization(body.organizationId)
    is DataWorkerAllocationGetRequestBody -> DataWorkerAllocationOperationInput.Filter(body.organizationId, body.dataplaneGroupId)
    is DataWorkerReallocateRequestBody ->
      DataWorkerAllocationOperationInput.Reallocate(
        body.organizationId,
        body.fromDataplaneGroupId,
        body.toDataplaneGroupId,
      )
    is DataWorkerAddCapacityRequestBody -> DataWorkerAllocationOperationInput.Add(body.organizationId, Optional.ofNullable(body.dataplaneGroupId))
    is DataWorkerRemoveCapacityRequestBody -> DataWorkerAllocationOperationInput.Remove(body.organizationId, body.dataplaneGroupId)
    else -> throw BadRequestProblem()
  }

abstract class DataWorkerAllocationOperationRule(
  private val data: AuthorizationDataService,
  private val permissions: OperationPermissions,
  private val instanceAdmin: Boolean,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput = allocationInput(args)

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    val organization = data.organization((input as DataWorkerAllocationOperationInput).organizationId)
    val callers = setOf(OperationCaller.USER, OperationCaller.INTERNAL_CLIENT, OperationCaller.DATAPLANE, OperationCaller.SERVICE_ACCOUNT)
    if (instanceAdmin) {
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), callers)
    } else {
      permissions.requireOrganizations(caller, listOf(organization), setOf(AuthRoleConstants.ORGANIZATION_ADMIN), callers)
    }
  }
}

/** GET's region is an exact scoped filter; an unknown region still reports zero. */
@Singleton
class DataWorkerAllocationOrganizationRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DataWorkerAllocationOperationRule(data, permissions, false)

@Singleton
class DataWorkerAllocationInstanceRule(
  data: AuthorizationDataService,
  permissions: OperationPermissions,
) : DataWorkerAllocationOperationRule(data, permissions, true)

/** OSS allocation stubs use the organization ID for authorization, without loading allocations or regions. */
@Singleton
class DataWorkerAllocationOrganizationUnsupportedRule(
  private val permissions: OperationPermissions,
) : OperationRule {
  override fun readInput(args: Array<out Any?>): OperationInput {
    val organization: UUID? =
      when (val body = args.singleOrNull()) {
        is DataWorkerAllocationListRequestBody -> body.organizationId
        is DataWorkerAllocationGetRequestBody -> body.organizationId
        is DataWorkerReallocateRequestBody -> body.organizationId
        else -> throw BadRequestProblem()
      }
    return if (organization == null) MetadataOperationInput else OrganizationOperationInput(organization)
  }

  override fun authorize(
    caller: Authentication,
    input: OperationInput,
  ) {
    if (input == MetadataOperationInput) {
      permissions.requireGlobal(caller, setOf(AuthRoleConstants.ORGANIZATION_ADMIN), organizationCallers)
      return
    }
    permissions.requireOrganizations(
      caller,
      listOf((input as OrganizationOperationInput).organizationId),
      setOf(AuthRoleConstants.ORGANIZATION_ADMIN),
      organizationCallers,
    )
  }
}
