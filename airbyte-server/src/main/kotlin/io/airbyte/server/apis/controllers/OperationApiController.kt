/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.OperationApi
import io.airbyte.api.server.generated.models.CheckOperationRead
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.OperationCreate
import io.airbyte.api.server.generated.models.OperationIdRequestBody
import io.airbyte.api.server.generated.models.OperationRead
import io.airbyte.api.server.generated.models.OperationReadList
import io.airbyte.api.server.generated.models.OperationUpdate
import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OperationApiController(
  private val operationsHandler: OperationsHandler,
) : OperationApi {
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun checkOperation(
    @Body operatorConfiguration: OperatorConfiguration,
  ): CheckOperationRead = execute { operationsHandler.checkOperation(operatorConfiguration) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createOperation(
    @Body operationCreate: OperationCreate,
  ): OperationRead = execute { operationsHandler.createOperation(operationCreate) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteOperation(
    @Body operationIdRequestBody: OperationIdRequestBody,
  ) {
    execute<Any?> {
      operationsHandler.deleteOperation(operationIdRequestBody)
      null
    }
  }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getOperation(
    @Body operationIdRequestBody: OperationIdRequestBody,
  ): OperationRead = execute { operationsHandler.getOperation(operationIdRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listOperationsForConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): OperationReadList = execute { operationsHandler.listOperationsForConnection(connectionIdRequestBody) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateOperation(
    @Body operationUpdate: OperationUpdate,
  ): OperationRead = execute { operationsHandler.updateOperation(operationUpdate) }
}
